"""
Test Conversation Flow
=====================

This script tests the conversation flow to verify that continued conversations
are working correctly with the proper Genie SDK methods.
"""

import asyncio
from databricks_client import DatabricksGenieClient


async def test_conversation_flow():
    """Test the conversation flow"""
    print("🧪 Testing Conversation Flow")
    print("=" * 50)
    
    client = DatabricksGenieClient()
    
    try:
        await client.__aenter__()
        
        # Test 1: Start a new conversation
        print("📝 Test 1: Starting new conversation...")
        response1 = await client.send_message("What is the total revenue?")
        print(f"✅ Conversation started: {response1.conversation_id}")
        print(f"   Response: {response1.message[:100]}...")
        
        # Test 2: Continue the conversation
        print(f"\n📝 Test 2: Continuing conversation {response1.conversation_id}...")
        response2 = await client.send_message(
            "What about the top 3 products?", 
            conversation_id=response1.conversation_id
        )
        print(f"✅ Conversation continued: {response2.conversation_id}")
        print(f"   Response: {response2.message[:100]}...")
        
        # Test 3: Verify conversation IDs match
        if response1.conversation_id == response2.conversation_id:
            print(f"\n✅ SUCCESS: Conversation IDs match - continued conversation working!")
        else:
            print(f"\n❌ FAILED: Conversation IDs don't match")
            print(f"   Original: {response1.conversation_id}")
            print(f"   Continued: {response2.conversation_id}")
        
        # Test 4: Another continued message
        print(f"\n📝 Test 3: Another continued message...")
        response3 = await client.send_message(
            "Show me more details about the first product", 
            conversation_id=response1.conversation_id
        )
        print(f"✅ Third message sent: {response3.conversation_id}")
        print(f"   Response: {response3.message[:100]}...")
        
        print(f"\n🎉 Conversation flow test completed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await client.__aexit__(None, None, None)


if __name__ == "__main__":
    asyncio.run(test_conversation_flow())
