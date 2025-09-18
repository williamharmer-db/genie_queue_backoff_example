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
import re
from genie_conversation import GenieConversationManager


def _extract_results_from_response(response_content: str) -> str:
    """Extract just the meaningful results from assistant response, not the SQL query"""
    # Look for "Query Results:" section
    if "Query Results:" in response_content:
        # Extract everything after "Query Results:"
        results_section = response_content.split("Query Results:")[1]
        # Remove any trailing SQL query parts
        results_section = results_section.split("Generated SQL:")[0]
        return results_section.strip()
    
    # If no query results, look for other meaningful content
    # Remove "Generated SQL:" sections
    content = re.sub(r'Generated SQL:.*?(?=\n\n|\nQuery Results:|$)', '', response_content, flags=re.DOTALL)
    
    # Clean up extra whitespace
    content = re.sub(r'\n\s*\n', '\n', content).strip()
    
    return content if content else "No results available"


async def interactive_conversation(manager: GenieConversationManager, conversation_id: str):
    """Interactive conversation mode"""
    print(f"\n💬 Interactive Conversation Mode")
    print("=" * 60)
    print("💡 Type your questions and press Enter. Type 'quit', 'exit', or 'bye' to end.")
    print("   Examples:")
    print("   - 'What is the total revenue?'")
    print("   - 'Show me the top 5 products by sales'")
    print("   - 'What about the average order value?'")
    print("   - 'How many customers do we have?'")
    print("-" * 60)
    
    while True:
        try:
            # Get user input
            user_input = input("\n👤 You: ").strip()
            
            # Check for exit commands
            if user_input.lower() in ['quit', 'exit', 'bye', 'q']:
                print("👋 Goodbye! Thanks for using the Genie Conversation API demo.")
                break
            
            if not user_input:
                print("🤖 Please enter a question or type 'quit' to exit.")
                continue
            
            # Send message and get response
            print("🤖 Genie is thinking...")
            response = await manager.send_message_immediate(conversation_id, user_input)
            
            # Display response
            print(f"\n🤖 Genie: {response.message}")
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye! Thanks for using the Genie Conversation API demo.")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print("Please try again or type 'quit' to exit.")


async def canned_demo(manager: GenieConversationManager, conversation_id: str):
    """Run the canned demo with predefined questions"""
    # Demo questions that demonstrate continued conversation context
    questions = [
        "Which products generate the highest total sales value?",
        "Show me the top 3 retailers by total sales", 
        "What is the average order value?",
        "How many different products do we have?",
        "Can you show me more pricing details about the top product from the first question?",
        "What about the second highest product?",
        "How do these top products compare in terms of sales?"
    ]
    
    print(f"\n📊 Running {len(questions)} demo questions...")
    print("=" * 60)
    print("💡 Notice how later questions reference previous answers (continued conversation)")
    print("   - Questions 1-4: Initial questions to establish context")
    print("   - Questions 5-7: Reference previous answers using conversation_id")
    print("   - Genie maintains conversation context automatically")
    
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
    
    # Choose demo mode
    print(f"\n🎯 Choose Demo Mode:")
    print("1. Interactive Conversation - Ask your own questions")
    print("2. Canned Demo - Run predefined questions with rate limiting demo")
    
    while True:
        try:
            choice = input("\nEnter your choice (1 or 2): ").strip()
            if choice in ['1', '2']:
                break
            else:
                print("Please enter 1 or 2")
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            return
    
    # Initialize conversation manager
    manager = GenieConversationManager()
    
    try:
        await manager.initialize()
        
        # Clear any existing conversations to avoid ownership issues
        if manager.client:
            manager.client.clear_conversations()
        
        conversation_id = await manager.start_conversation()
        
        print(f"\n💬 Started conversation: {conversation_id}")
        
        # Run the selected demo mode
        if choice == '1':
            # Interactive conversation mode
            await interactive_conversation(manager, conversation_id)
        else:
            # Canned demo mode
            await canned_demo(manager, conversation_id)
            
            # Demo rate limiting and queuing (only for canned demo)
            print(f"\n🚀 Rate Limiting Demo - Queuing Multiple Requests")
            print("=" * 60)
            print("💡 This simulates a scenario where you send multiple requests quickly")
            print("   (In production, >5 requests/minute may trigger 429 rate limiting)")
            
            # Create multiple questions to demonstrate queuing
            rapid_questions = [
                "What is the total revenue?",
                "Which region has the highest sales?",
                "What is the most popular product category?",
                "How many orders were placed last month?",
                "What is the average order size?",
                "Which customer segment is most valuable?",
                "What is the profit margin by product?",
                "How has sales performance changed over time?"
            ]
            
            print(f"\n📤 Queuing {len(rapid_questions)} requests rapidly...")
            
            # Queue all requests quickly (this will demonstrate the queuing system)
            request_ids = []
            for i, question in enumerate(rapid_questions, 1):
                print(f"   📤 Queuing request {i}: {question[:50]}...")
                request_id = await manager.send_message(conversation_id, question)
                request_ids.append(request_id)
            
            print(f"\n⏳ Processing {len(request_ids)} queued requests...")
            print("💡 Notice how requests are processed by worker threads with rate limiting")
            
            # Wait for all responses and show progress
            completed_count = 0
            for i, request_id in enumerate(request_ids, 1):
                try:
                    print(f"   ⏳ Waiting for request {i}/{len(request_ids)}...")
                    response = await manager.get_response(request_id, timeout=120)
                    completed_count += 1
                    print(f"   ✅ Completed {completed_count}/{len(request_ids)}: {response.message[:100]}...")
                    
                except Exception as e:
                    print(f"   ❌ Request {i} failed: {e}")
            
            print(f"\n🎉 Rate limiting demo completed!")
            print(f"   📊 Successfully processed {completed_count}/{len(request_ids)} requests")
            
            # Show queue statistics
            stats = manager.get_queue_stats()
            print(f"\n📊 Queue Statistics:")
            print(f"   - Total requests processed: {stats['total_requests']}")
            print(f"   - Completed requests: {stats['completed_requests']}")
            print(f"   - Worker threads: {stats['workers']}")
        
        # Show conversation history (for both modes)
        print(f"\n📚 Conversation History:")
        print("=" * 60)
        history = manager.get_conversation_history(conversation_id)
        for i, msg in enumerate(history, 1):
            role_emoji = "👤" if msg.role == "user" else "🤖"
            
            if msg.role == "assistant":
                # For assistant messages, show just the results, not the SQL query
                display_content = _extract_results_from_response(msg.content)
            else:
                # For user messages, show the full question
                display_content = msg.content
            
            print(f"{i}. {role_emoji} {msg.role.title()}: {display_content[:100]}...")
        
        print(f"\n✅ Demo completed successfully!")
        print(f"💡 This demonstrates:")
        print(f"   - Genie SDK integration with space_id")
        print(f"   - Continued conversation context (conversation_id)")
        print(f"   - SQL query generation and execution")
        print(f"   - Statement execution API for results")
        if choice == '2':
            print(f"   - Rate limiting with exponential backoff (429 handling)")
            print(f"   - Request queuing with worker threads")
        print(f"   - Production-ready error handling and retry logic")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await manager.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
