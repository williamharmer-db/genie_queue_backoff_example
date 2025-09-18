# Databricks Genie Conversation API

A clean, production-ready solution for interacting with Databricks Genie using the SDK with proper rate limiting and query result retrieval.

## 🎯 Key Features

- **Proper Genie SDK Integration**: Uses `space_id` instead of serving endpoints
- **Query Result Retrieval**: Fetches actual data via statement execution API
- **Rate Limiting**: Exponential backoff for 429 responses
- **Request Queuing**: Asynchronous processing with worker threads
- **Formatted Output**: Readable table formatting for query results

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
Copy the example file and add your credentials:
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

The `.env` file is ignored by git for security, but `.env.example` is tracked as a template.
```bash
# Required
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token

# Optional - will use first available space if not provided
GENIE_SPACE_ID=your-genie-space-id

# Optional - rate limiting configuration
MAX_RETRIES=5
INITIAL_BACKOFF=1.0
MAX_BACKOFF=60.0
BACKOFF_MULTIPLIER=2.0

# Optional - queue configuration
MAX_QUEUE_SIZE=1000
WORKER_THREADS=4

# Optional - logging level
LOG_LEVEL=INFO
```

Or set environment variables directly:
```bash
export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'
export DATABRICKS_TOKEN='your-personal-access-token'
export GENIE_SPACE_ID='your-genie-space-id'  # Optional
```

### 3. Find Your Genie Space (Optional)
```bash
python find_genie_spaces.py
```

### 4. Run Demo
```bash
python demo.py
```

## 📁 Project Structure

```
genie_conversation_api/
├── config.py              # Configuration management
├── databricks_client.py   # Genie SDK client with query result retrieval
├── queue_manager.py       # Rate limiting and request queuing
├── genie_conversation.py  # High-level conversation interface
├── demo.py               # Clean demonstration script
├── find_genie_spaces.py  # Utility to find available spaces
├── requirements.txt      # Dependencies
└── README.md            # This file
```

## 💡 Core Solution

The solution demonstrates the complete data flow:

1. **User asks question** → Genie generates SQL
2. **SQL executes** → Returns `statement_id`
3. **Fetch results** → `client.statement_execution.get_statement(statement_id)`
4. **Format output** → Readable table with data

### Example Output
```
📝 Question: Which products generate the highest total sales value?
🤖 Answer:
Generated SQL: SELECT `ProductName`, `TotalValue` FROM `nehap`.`demo_data`.`amyskitchen_distribution` ORDER BY `TotalValue` DESC LIMIT 5;

Query Results:
  ProductName   |   TotalValue   
---------------------------------
Quinoa & Kale Bowl |     2271.50    
  Vegan Pizza   |     2197.25    
 Organic Chili  |     2145.00    
```

## 🔧 Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | Yes |
| `DATABRICKS_TOKEN` | Personal access token | Yes |
| `GENIE_SPACE_ID` | Specific Genie space ID | No* |

*If not provided, uses the first available space

## 🛡️ Rate Limiting

- **Exponential Backoff**: 1s → 2s → 4s → 8s → 16s → 32s → 60s max
- **Jitter**: Random variation to prevent thundering herd
- **429 Detection**: Automatic retry with backoff
- **Queue Management**: Configurable worker threads

## 📊 Usage Example

```python
import asyncio
from genie_conversation import GenieConversationManager

async def main():
    manager = GenieConversationManager()
    
    try:
        await manager.initialize()
        conversation_id = await manager.start_conversation()
        
        response = await manager.send_message_immediate(
            conversation_id, 
            "Which products generate the highest sales?"
        )
        
        print(response.message)  # Includes SQL + formatted results
        
    finally:
        await manager.cleanup()

asyncio.run(main())
```

## 🔍 Key Implementation Details

### Query Result Retrieval
```python
# Get statement_id from Genie response
statement_id = result.attachments[0].query.statement_id

# Fetch actual results
statement_result = client.statement_execution.get_statement(statement_id)

# Extract data
data_array = statement_result.result.data_array
```

### Rate Limiting
```python
# Exponential backoff with jitter
wait_time = min(
    base_delay * (multiplier ** retry_count),
    max_backoff
) + random.uniform(0, 0.1 * wait_time)
```

## 🎯 What This Solves

- ✅ **Proper SDK Usage**: Uses Genie SDK correctly with `space_id`
- ✅ **Complete Data Flow**: Gets actual query results, not just SQL
- ✅ **Production Ready**: Handles rate limiting and errors gracefully
- ✅ **Clean Architecture**: Modular, maintainable code structure
- ✅ **Customer Ready**: Focused, non-redundant implementation

## 📚 References

- [Databricks SDK Documentation](https://docs.databricks.com/dev-tools/sdk-python.html)
- [Genie Conversation API](https://docs.databricks.com/gcp/en/genie/conversation-api)
- [Statement Execution API](https://docs.databricks.com/sql/api/statement-execution.html)