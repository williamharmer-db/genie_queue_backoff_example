"""
Test Rate Limiting and 429 Handling
===================================

This script demonstrates how the system handles rate limiting and 429 responses.
It can be used to test the backoff mechanisms in a controlled environment.
"""

import asyncio
import time
from genie_conversation import GenieConversationManager


async def test_rate_limiting():
    """Test rate limiting with rapid requests"""
    print("🧪 Testing Rate Limiting and 429 Handling")
    print("=" * 50)
    
    manager = GenieConversationManager()
    
    try:
        await manager.initialize()
        conversation_id = await manager.start_conversation()
        
        # Create many questions to potentially trigger rate limiting
        questions = [
            "What is the total revenue?",
            "Which region has the highest sales?", 
            "What is the most popular product?",
            "How many customers do we have?",
            "What is the average order value?",
            "Which product category is most profitable?",
            "How has sales changed over time?",
            "What is the customer retention rate?",
            "Which marketing channel is most effective?",
            "What is the inventory turnover rate?"
        ]
        
        print(f"📤 Sending {len(questions)} requests rapidly...")
        print("💡 This may trigger rate limiting (429 responses) in production")
        print("   The system will automatically retry with exponential backoff")
        
        start_time = time.time()
        
        # Queue all requests quickly
        request_ids = []
        for i, question in enumerate(questions, 1):
            print(f"   📤 Queuing request {i}: {question}")
            request_id = await manager.send_message(conversation_id, question)
            request_ids.append(request_id)
        
        print(f"\n⏳ Processing {len(request_ids)} queued requests...")
        print("🔍 Watch for rate limiting messages in the logs")
        
        # Wait for all responses
        completed_count = 0
        failed_count = 0
        
        for i, request_id in enumerate(request_ids, 1):
            try:
                print(f"   ⏳ Waiting for request {i}/{len(request_ids)}...")
                response = await manager.get_response(request_id, timeout=180)
                completed_count += 1
                print(f"   ✅ Completed {completed_count}/{len(request_ids)}")
                
            except Exception as e:
                failed_count += 1
                print(f"   ❌ Request {i} failed: {e}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n📊 Rate Limiting Test Results:")
        print(f"   - Total requests: {len(request_ids)}")
        print(f"   - Completed: {completed_count}")
        print(f"   - Failed: {failed_count}")
        print(f"   - Total time: {total_time:.2f} seconds")
        print(f"   - Average time per request: {total_time/len(request_ids):.2f} seconds")
        
        # Show queue statistics
        stats = manager.get_queue_stats()
        print(f"\n📈 Queue Statistics:")
        print(f"   - Total requests processed: {stats['total_requests']}")
        print(f"   - Completed requests: {stats['completed_requests']}")
        print(f"   - Worker threads: {stats['workers']}")
        
        if completed_count == len(request_ids):
            print(f"\n🎉 All requests completed successfully!")
            print(f"   💡 Rate limiting was handled gracefully")
        else:
            print(f"\n⚠️  Some requests failed - check logs for details")
            print(f"   💡 This is normal when testing rate limits")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await manager.cleanup()


if __name__ == "__main__":
    asyncio.run(test_rate_limiting())
