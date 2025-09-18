"""
Databricks Genie Conversation API Demo
=====================================

A clean demonstration of how to interact with Databricks Genie using the SDK
with proper rate limiting and query result retrieval.

Key Features:
- Uses proper Genie SDK with space_id
- Fetches actual query results via statement execution API
- Implements exponential backoff for 429 rate limiting
- Queues requests with worker threads
- Formats results as readable tables
"""

import asyncio
import os
from genie_conversation import GenieConversationManager


async def main():
    """Main demo function"""
    print("🚀 Databricks Genie Conversation API Demo")
    print("=" * 60)
    
    # Check environment variables
    required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these environment variables:")
        print("export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'")
        print("export DATABRICKS_TOKEN='your-personal-access-token'")
        print("export GENIE_SPACE_ID='your-genie-space-id'  # Optional - will use first available")
        return
    
    print("✅ Environment configured")
    print(f"   - Host: {os.getenv('DATABRICKS_HOST')}")
    space_id = os.getenv('GENIE_SPACE_ID')
    if space_id:
        print(f"   - Space ID: {space_id}")
    else:
        print("   - Space ID: Will use first available space")
    
    # Initialize conversation manager
    manager = GenieConversationManager()
    
    try:
        await manager.initialize()
        conversation_id = await manager.start_conversation()
        
        print(f"\n💬 Started conversation: {conversation_id}")
        
        # Demo questions that will generate SQL queries
        questions = [
            "Which products generate the highest total sales value?",
            "Show me the top 3 retailers by total sales",
            "What is the average order value?",
            "How many different products do we have?"
        ]
        
        print(f"\n📊 Running {len(questions)} demo questions...")
        print("=" * 60)
        
        for i, question in enumerate(questions, 1):
            print(f"\n📝 Question {i}: {question}")
            print("🤖 Answer:")
            print("-" * 40)
            
            try:
                # Send message and get response
                response = await manager.send_message_immediate(conversation_id, question)
                print(response.message)
                
            except Exception as e:
                print(f"❌ Error: {e}")
            
            print("-" * 40)
            
            # Small delay between questions
            await asyncio.sleep(1)
        
        # Show queue statistics
        stats = manager.get_queue_stats()
        print(f"\n📊 Queue Statistics:")
        print(f"   - Total requests processed: {stats['total_requests']}")
        print(f"   - Completed requests: {stats['completed_requests']}")
        print(f"   - Worker threads: {stats['workers']}")
        
        print(f"\n✅ Demo completed successfully!")
        print(f"💡 This demonstrates:")
        print(f"   - Genie SDK integration with space_id")
        print(f"   - SQL query generation and execution")
        print(f"   - Statement execution API for results")
        print(f"   - Rate limiting with exponential backoff")
        print(f"   - Request queuing with worker threads")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await manager.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
