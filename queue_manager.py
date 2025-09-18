"""
Queue Manager for handling API requests with rate limiting and backoff
"""
import asyncio
import time
import random
from typing import Callable, Any, Optional, Dict
from dataclasses import dataclass
from enum import Enum
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiohttp
from config import settings


class RequestStatus(Enum):
    """Status of a queued request"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"


@dataclass
class QueuedRequest:
    """Represents a queued API request"""
    id: str
    func: Callable
    args: tuple = ()
    kwargs: Dict[str, Any] = None
    status: RequestStatus = RequestStatus.PENDING
    created_at: float = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    retry_count: int = 0
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()
        if self.kwargs is None:
            self.kwargs = {}


class RateLimitError(Exception):
    """Exception raised when rate limit is exceeded"""
    def __init__(self, message: str, retry_after: Optional[int] = None):
        super().__init__(message)
        self.retry_after = retry_after


class QueueManager:
    """Manages API request queue with rate limiting and backoff"""
    
    def __init__(self, max_workers: int = None):
        self.max_workers = max_workers or settings.worker_threads
        self.queue = asyncio.Queue(maxsize=settings.max_queue_size)
        self.workers = []
        self.request_counter = 0
        self.active_requests: Dict[str, QueuedRequest] = {}
        self.completed_requests: Dict[str, QueuedRequest] = {}
        self._shutdown = False
        
    async def start(self):
        """Start the queue manager and worker tasks"""
        logger.info(f"Starting QueueManager with {self.max_workers} workers")
        
        # Start worker tasks
        for i in range(self.max_workers):
            worker = asyncio.create_task(self._worker(f"worker-{i}"))
            self.workers.append(worker)
            
    async def stop(self):
        """Stop the queue manager and all workers"""
        logger.info("Stopping QueueManager")
        self._shutdown = True
        
        # Cancel all workers
        for worker in self.workers:
            worker.cancel()
            
        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        
    async def submit_request(self, func: Callable, *args, **kwargs) -> str:
        """Submit a request to the queue"""
        if self._shutdown:
            raise RuntimeError("QueueManager is shutting down")
            
        self.request_counter += 1
        request_id = f"req-{self.request_counter}"
        
        request = QueuedRequest(
            id=request_id,
            func=func,
            args=args,
            kwargs=kwargs
        )
        
        try:
            await self.queue.put(request)
            self.active_requests[request_id] = request
            logger.debug(f"Submitted request {request_id} to queue")
            return request_id
        except asyncio.QueueFull:
            raise RuntimeError(f"Queue is full (max size: {settings.max_queue_size})")
    
    async def get_request_status(self, request_id: str) -> Optional[QueuedRequest]:
        """Get the status of a request"""
        return self.active_requests.get(request_id) or self.completed_requests.get(request_id)
    
    async def wait_for_request(self, request_id: str, timeout: Optional[float] = None) -> QueuedRequest:
        """Wait for a request to complete"""
        start_time = time.time()
        
        while True:
            request = await self.get_request_status(request_id)
            if not request:
                raise ValueError(f"Request {request_id} not found")
                
            if request.status in [RequestStatus.COMPLETED, RequestStatus.FAILED]:
                return request
                
            if timeout and (time.time() - start_time) > timeout:
                raise asyncio.TimeoutError(f"Request {request_id} timed out after {timeout}s")
                
            await asyncio.sleep(0.1)
    
    async def _worker(self, worker_name: str):
        """Worker task that processes requests from the queue"""
        logger.debug(f"Worker {worker_name} started")
        
        while not self._shutdown:
            try:
                # Get request from queue with timeout
                request = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                
                if request is None:  # Shutdown signal
                    break
                    
                await self._process_request(request, worker_name)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_name} error: {e}")
                
        logger.debug(f"Worker {worker_name} stopped")
    
    async def _process_request(self, request: QueuedRequest, worker_name: str):
        """Process a single request with retry logic"""
        request.status = RequestStatus.PROCESSING
        request.started_at = time.time()
        
        logger.debug(f"Worker {worker_name} processing request {request.id}")
        
        try:
            # Execute the request with retry logic
            result = await self._execute_with_retry(request)
            
            request.status = RequestStatus.COMPLETED
            request.completed_at = time.time()
            request.kwargs['result'] = result
            
            logger.debug(f"Request {request.id} completed successfully")
            
        except RateLimitError as e:
            request.status = RequestStatus.RATE_LIMITED
            request.error = str(e)
            request.retry_count += 1
            
            # Calculate backoff time
            backoff_time = self._calculate_backoff(request.retry_count, e.retry_after)
            
            logger.warning(f"Request {request.id} rate limited, retrying in {backoff_time}s")
            
            # Re-queue the request after backoff
            await asyncio.sleep(backoff_time)
            await self.queue.put(request)
            return
            
        except Exception as e:
            request.status = RequestStatus.FAILED
            request.error = str(e)
            request.completed_at = time.time()
            
            logger.error(f"Request {request.id} failed: {e}")
        
        finally:
            # Move to completed requests
            if request.id in self.active_requests:
                del self.active_requests[request.id]
            self.completed_requests[request.id] = request
    
    async def _execute_with_retry(self, request: QueuedRequest):
        """Execute a request with retry logic"""
        @retry(
            stop=stop_after_attempt(settings.max_retries),
            wait=wait_exponential(
                multiplier=settings.initial_backoff,
                max=settings.max_backoff
            ),
            retry=retry_if_exception_type(RateLimitError)
        )
        async def _execute():
            try:
                # Execute the function
                if asyncio.iscoroutinefunction(request.func):
                    return await request.func(*request.args, **request.kwargs)
                else:
                    return request.func(*request.args, **request.kwargs)
                    
            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    retry_after = None
                    if 'Retry-After' in e.headers:
                        retry_after = int(e.headers['Retry-After'])
                    raise RateLimitError(f"Rate limit exceeded: {e}", retry_after)
                else:
                    raise
            except Exception as e:
                # Check if it's a 429 response in other formats
                if hasattr(e, 'status') and e.status == 429:
                    raise RateLimitError(f"Rate limit exceeded: {e}")
                raise
        
        return await _execute()
    
    def _calculate_backoff(self, retry_count: int, retry_after: Optional[int] = None) -> float:
        """Calculate backoff time for retry"""
        if retry_after:
            # Use server-specified retry-after time with jitter
            base_time = retry_after
        else:
            # Use exponential backoff
            base_time = min(
                settings.initial_backoff * (settings.backoff_multiplier ** retry_count),
                settings.max_backoff
            )
        
        # Add jitter to prevent thundering herd
        jitter = random.uniform(0.1, 0.3) * base_time
        return base_time + jitter
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            "queue_size": self.queue.qsize(),
            "active_requests": len(self.active_requests),
            "completed_requests": len(self.completed_requests),
            "workers": len(self.workers),
            "total_requests": self.request_counter
        }


