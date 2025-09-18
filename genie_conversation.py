"""
Genie Conversation Interface with Queue Management and Rate Limiting
"""
import asyncio
import uuid
from typing import List, Optional, Dict, Any, AsyncGenerator
from datetime import datetime
from loguru import logger

from databricks_client import DatabricksGenieClient, ConversationMessage, ConversationResponse
from queue_manager import QueueManager, QueuedRequest, RequestStatus


class GenieConversationManager:
    """Manages Genie conversations with queue-based rate limiting"""
    
    def __init__(self, queue_manager: Optional[QueueManager] = None):
        self.queue_manager = queue_manager or QueueManager()
        self.conversations: Dict[str, List[ConversationMessage]] = {}
        self.client: Optional[DatabricksGenieClient] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize the conversation manager"""
        if self._initialized:
            return
            
        # Start queue manager
        await self.queue_manager.start()
        
        # Initialize Databricks client
        self.client = DatabricksGenieClient()
        await self.client.__aenter__()
        
        self._initialized = True
        logger.info("GenieConversationManager initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        if not self._initialized:
            return
            
        # Close Databricks client
        if self.client:
            await self.client.__aexit__(None, None, None)
        
        # Stop queue manager
        await self.queue_manager.stop()
        
        self._initialized = False
        logger.info("GenieConversationManager cleaned up")
    
    async def start_conversation(self, system_message: Optional[str] = None) -> str:
        """Start a new conversation"""
        conversation_id = str(uuid.uuid4())
        self.conversations[conversation_id] = []
        
        if system_message:
            self.conversations[conversation_id].append(
                ConversationMessage(role="system", content=system_message)
            )
        
        logger.info(f"Started new conversation: {conversation_id}")
        return conversation_id
    
    async def send_message(
        self, 
        conversation_id: str, 
        message: str,
        stream: bool = False,
        **kwargs
    ) -> str:
        """Send a message to a conversation (queued)"""
        if not self._initialized:
            await self.initialize()
        
        if conversation_id not in self.conversations:
            raise ValueError(f"Conversation {conversation_id} not found")
        
        # Add user message to conversation
        user_message = ConversationMessage(role="user", content=message)
        self.conversations[conversation_id].append(user_message)
        
        # Submit request to queue
        request_id = await self.queue_manager.submit_request(
            self._send_message_async,
            conversation_id,
            message,
            stream,
            **kwargs
        )
        
        logger.info(f"Queued message for conversation {conversation_id}, request {request_id}")
        return request_id
    
    async def send_message_immediate(
        self, 
        conversation_id: str, 
        message: str,
        stream: bool = False,
        **kwargs
    ) -> ConversationResponse:
        """Send a message immediately (bypasses queue)"""
        if not self._initialized:
            await self.initialize()
        
        if conversation_id not in self.conversations:
            raise ValueError(f"Conversation {conversation_id} not found")
        
        # Add user message to conversation
        user_message = ConversationMessage(role="user", content=message)
        self.conversations[conversation_id].append(user_message)
        
        # Send message directly
        response = await self._send_message_async(conversation_id, message, stream, **kwargs)
        
        # Add assistant response to conversation
        assistant_message = ConversationMessage(role="assistant", content=response.message)
        self.conversations[conversation_id].append(assistant_message)
        
        return response
    
    async def _send_message_async(
        self, 
        conversation_id: str, 
        message: str,
        stream: bool = False,
        **kwargs
    ) -> ConversationResponse:
        """Internal method to send message to Genie"""
        if not self.client:
            raise RuntimeError("Databricks client not initialized")
        
        # Get conversation history
        messages = self.conversations.get(conversation_id, [])
        
        # Send to Genie
        response = await self.client.send_conversation(
            messages=messages,
            conversation_id=conversation_id,
            stream=stream,
            **kwargs
        )
        
        # Add assistant response to conversation
        assistant_message = ConversationMessage(role="assistant", content=response.message)
        self.conversations[conversation_id].append(assistant_message)
        
        return response
    
    async def get_response(self, request_id: str, timeout: Optional[float] = None) -> ConversationResponse:
        """Get the response for a queued request"""
        request = await self.queue_manager.wait_for_request(request_id, timeout)
        
        if request.status == RequestStatus.COMPLETED:
            return request.kwargs.get('result')
        elif request.status == RequestStatus.FAILED:
            raise Exception(f"Request failed: {request.error}")
        else:
            raise Exception(f"Request in unexpected status: {request.status}")
    
    async def stream_message(
        self, 
        conversation_id: str, 
        message: str,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """Stream a message response"""
        if not self._initialized:
            await self.initialize()
        
        if conversation_id not in self.conversations:
            raise ValueError(f"Conversation {conversation_id} not found")
        
        # Add user message to conversation
        user_message = ConversationMessage(role="user", content=message)
        self.conversations[conversation_id].append(user_message)
        
        # Get conversation history
        messages = self.conversations[conversation_id]
        
        # Stream response
        full_response = ""
        async for chunk in self.client.stream_conversation(messages, conversation_id, **kwargs):
            full_response += chunk
            yield chunk
        
        # Add assistant response to conversation
        assistant_message = ConversationMessage(role="assistant", content=full_response)
        self.conversations[conversation_id].append(assistant_message)
    
    def get_conversation_history(self, conversation_id: str) -> List[ConversationMessage]:
        """Get conversation history"""
        return self.conversations.get(conversation_id, [])
    
    def get_conversation_ids(self) -> List[str]:
        """Get all conversation IDs"""
        return list(self.conversations.keys())
    
    async def delete_conversation(self, conversation_id: str):
        """Delete a conversation"""
        if conversation_id in self.conversations:
            del self.conversations[conversation_id]
            logger.info(f"Deleted conversation: {conversation_id}")
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return self.queue_manager.get_stats()
    
    async def wait_for_all_requests(self, timeout: Optional[float] = None):
        """Wait for all queued requests to complete"""
        start_time = datetime.now()
        
        while True:
            stats = self.queue_manager.get_stats()
            if stats["queue_size"] == 0 and stats["active_requests"] == 0:
                break
                
            if timeout and (datetime.now() - start_time).total_seconds() > timeout:
                raise asyncio.TimeoutError("Timeout waiting for requests to complete")
                
            await asyncio.sleep(0.1)


