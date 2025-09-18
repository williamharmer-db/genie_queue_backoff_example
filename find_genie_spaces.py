"""
Find Genie Spaces Utility
========================

Simple utility to list available Genie spaces in your Databricks workspace.
"""

import os
from databricks.sdk import WorkspaceClient
from config import settings


def main():
    """List all accessible Genie spaces"""
    print("🔍 Finding Genie Spaces")
    print("=" * 40)
    
    # Check environment variables
    required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nSet them with:")
        print("export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'")
        print("export DATABRICKS_TOKEN='your-personal-access-token'")
        return
    
    try:
        # Connect to Databricks
        client = WorkspaceClient(
            host=settings.databricks_host,
            token=settings.databricks_token
        )
        
        print(f"✅ Connected to: {settings.databricks_host}")
        
        # List spaces
        spaces = client.genie.list_spaces()
        
        if not spaces.spaces:
            print("❌ No Genie spaces found")
            return
        
        print(f"\n📋 Found {len(spaces.spaces)} Genie spaces:")
        print("-" * 40)
        
        for i, space in enumerate(spaces.spaces, 1):
            print(f"{i}. {space.title}")
            print(f"   ID: {space.space_id}")
        
        print(f"\n💡 To use a space, set:")
        print(f"export GENIE_SPACE_ID='{spaces.spaces[0].space_id}'")
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
