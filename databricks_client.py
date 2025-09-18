"""
Databricks Client for Genie Conversation API with rate limiting support
"""
import asyncio
import time
import random
from typing import Dict, List, Optional, Any, AsyncGenerator
from dataclasses import dataclass
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from loguru import logger
from config import settings


@dataclass
class ConversationMessage:
    """Represents a message in a conversation"""
    role: str  # "user", "assistant", "system"
    content: str
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class ConversationResponse:
    """Response from a Genie conversation"""
    message: str
    conversation_id: str
    attachments: Optional[List[Dict[str, Any]]] = None
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class DatabricksGenieClient:
    """Client for interacting with Databricks Genie conversation API"""
    
    def __init__(self, workspace_client: Optional[WorkspaceClient] = None, space_id: Optional[str] = None):
        self.workspace_client = workspace_client or WorkspaceClient(
            host=settings.databricks_host,
            token=settings.databricks_token
        )
        self.space_id = space_id or settings.genie_space_id
        self._conversations: Dict[str, Any] = {}
        
    async def __aenter__(self):
        """Async context manager entry"""
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        pass
    
    def clear_conversations(self):
        """Clear the conversation cache to avoid ownership issues"""
        self._conversations.clear()
        logger.info("Cleared conversation cache")
    
    def _exponential_backoff(self, func, *args, max_retries: int = None, base_delay: float = None, **kwargs):
        """Execute function with exponential backoff for rate limiting"""
        max_retries = max_retries or settings.max_retries
        base_delay = base_delay or settings.initial_backoff
        
        retries = 0
        while retries < max_retries:
            try:
                return func(*args, **kwargs)
            except DatabricksError as e:
                # Check if it's a rate limit error (429)
                if hasattr(e, 'http_status_code') and e.http_status_code == 429:
                    wait_time = min(
                        base_delay * (settings.backoff_multiplier ** retries),
                        settings.max_backoff
                    )
                    # Add jitter
                    wait_time += random.uniform(0, 0.1 * wait_time)
                    
                    logger.warning(f"Rate limit exceeded. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    # For other errors (like PermissionDenied, conversation ownership), don't retry
                    raise
        raise Exception(f"Max retries ({max_retries}) exceeded for function {func.__name__}")
    
    async def list_spaces(self) -> List[Dict[str, Any]]:
        """List all accessible Genie spaces"""
        try:
            spaces = self._exponential_backoff(self.workspace_client.genie.list_spaces)
            return [
                {
                    "space_id": space.space_id,
                    "title": space.title,
                    "description": getattr(space, 'description', None)
                }
                for space in spaces.spaces
            ]
        except Exception as e:
            logger.error(f"Failed to list Genie spaces: {e}")
            raise
    
    async def get_default_space_id(self) -> str:
        """Get the first available space ID if none is specified"""
        if self.space_id:
            return self.space_id
        
        spaces = await self.list_spaces()
        if not spaces:
            raise ValueError("No Genie spaces found. Please create a Genie space first.")
        
        # Use the first available space
        default_space_id = spaces[0]["space_id"]
        logger.info(f"Using default space: {spaces[0]['title']} ({default_space_id})")
        return default_space_id
    
    async def send_message(
        self, 
        message: str, 
        conversation_id: Optional[str] = None,
        **kwargs
    ) -> ConversationResponse:
        """Send a message to Genie and get response"""
        try:
            if conversation_id and conversation_id in self._conversations:
                # Continue existing conversation - Genie maintains context automatically
                space_id = await self.get_default_space_id()
                result = self._exponential_backoff(
                    self.workspace_client.genie.create_message_and_wait,
                    space_id,
                    conversation_id,
                    message
                )
            else:
                # Get space ID (use default if not specified)
                space_id = await self.get_default_space_id()
                
                # Start new conversation
                result = self._exponential_backoff(
                    self.workspace_client.genie.start_conversation_and_wait,
                    space_id,
                    message
                )
                conversation_id = result.conversation_id
                self._conversations[conversation_id] = result
            
            # Extract response content
            response_text = ""
            attachments = []
            
            # The result is a GenieMessage object
            if hasattr(result, 'attachments') and result.attachments:
                for attachment in result.attachments:
                    if hasattr(attachment, 'text') and attachment.text:
                        response_text += attachment.text.content + "\n"
                    elif hasattr(attachment, 'query') and attachment.query:
                        # Get the SQL query
                        sql_query = attachment.query.query
                        response_text += f"Generated SQL: {sql_query}\n"
                        
                        # Fetch actual query results if statement_id is available
                        if hasattr(attachment.query, 'statement_id') and attachment.query.statement_id:
                            try:
                                statement_result = self._exponential_backoff(
                                    self.workspace_client.statement_execution.get_statement,
                                    attachment.query.statement_id
                                )
                                
                                if (hasattr(statement_result, 'result') and 
                                    statement_result.result and 
                                    hasattr(statement_result.result, 'data_array')):
                                    
                                    # Format the results as a table
                                    data_array = statement_result.result.data_array
                                    if data_array:
                                        response_text += "\nQuery Results:\n"
                                        response_text += self._format_query_results(statement_result)
                                        response_text += "\n"
                                
                            except Exception as e:
                                logger.warning(f"Failed to fetch query results: {e}")
                                response_text += f"(Unable to fetch results: {e})\n"
                    
                    # Store attachment details
                    attachment_dict = {}
                    if hasattr(attachment, 'text') and attachment.text:
                        attachment_dict['text'] = attachment.text.content
                    if hasattr(attachment, 'query') and attachment.query:
                        attachment_dict['query'] = attachment.query.query
                        if hasattr(attachment.query, 'statement_id'):
                            attachment_dict['statement_id'] = attachment.query.statement_id
                    if attachment_dict:
                        attachments.append(attachment_dict)
            
            return ConversationResponse(
                message=response_text.strip(),
                conversation_id=conversation_id,
                attachments=attachments if attachments else None
            )
            
        except Exception as e:
            logger.error(f"Failed to send message to Genie: {e}")
            raise
    
    def _format_query_results(self, statement_result) -> str:
        """Format query results as a readable table"""
        try:
            if not (hasattr(statement_result, 'result') and 
                   statement_result.result and 
                   hasattr(statement_result.result, 'data_array')):
                return "No results available"
            
            data_array = statement_result.result.data_array
            if not data_array:
                return "No data returned"
            
            # Get column information
            columns = []
            if (hasattr(statement_result, 'manifest') and 
                statement_result.manifest and 
                hasattr(statement_result.manifest, 'schema') and
                statement_result.manifest.schema and
                hasattr(statement_result.manifest.schema, 'columns')):
                columns = [col.name for col in statement_result.manifest.schema.columns]
            
            # If no column info, use generic column names
            if not columns:
                columns = [f"Column_{i+1}" for i in range(len(data_array[0]) if data_array else 0)]
            
            # Format as table
            formatted = ""
            
            # Header
            header = " | ".join(f"{col:^15}" for col in columns)
            formatted += header + "\n"
            formatted += "-" * len(header) + "\n"
            
            # Data rows
            for row in data_array:
                formatted_row = " | ".join(f"{str(cell):^15}" for cell in row)
                formatted += formatted_row + "\n"
            
            return formatted
            
        except Exception as e:
            logger.error(f"Error formatting query results: {e}")
            return f"Error formatting results: {e}"
    
    async def send_conversation(
        self, 
        messages: List[ConversationMessage],
        conversation_id: Optional[str] = None,
        **kwargs
    ) -> ConversationResponse:
        """Send a full conversation to Genie"""
        if not messages:
            raise ValueError("No messages provided")
        
        # Find the last user message
        last_user_message = None
        for message in reversed(messages):
            if message.role == "user":
                last_user_message = message.content
                break
        
        if not last_user_message:
            raise ValueError("No user message found in conversation")
        
        # For continued conversations, Genie maintains context automatically
        # We just need to send the new message with the conversation_id
        return await self.send_message(last_user_message, conversation_id, **kwargs)
    
    async def stream_conversation(
        self, 
        messages: List[ConversationMessage],
        conversation_id: Optional[str] = None,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """Stream a conversation response (simplified for Genie)"""
        # For now, we'll simulate streaming by getting the full response and yielding it in chunks
        # In a real implementation, you might want to use the Genie streaming API if available
        response = await self.send_conversation(messages, conversation_id, **kwargs)
        
        # Simulate streaming by yielding the response in chunks
        message = response.message
        chunk_size = 50  # Characters per chunk
        
        for i in range(0, len(message), chunk_size):
            chunk = message[i:i + chunk_size]
            yield chunk
            await asyncio.sleep(0.05)  # Small delay to simulate streaming


