# Databricks Genie Conversation API Flow

This document explains the step-by-step process of how our solution interacts with the Databricks Genie Conversation API, from user question to formatted results.

## 🔄 Complete Data Flow Overview

```
User Question → Genie SDK → SQL Generation → Statement Execution → Result Formatting → User Response
```

## 📋 Detailed Step-by-Step Process

### Step 1: Initialize Connection
```python
# Create Databricks workspace client
client = WorkspaceClient(
    host="https://your-workspace.cloud.databricks.com",
    token="your-personal-access-token"
)
```

**What happens:**
- Authenticate with Databricks workspace using personal access token
- Establish connection to Genie service
- Validate permissions and space access

### Step 2: Start Conversation
```python
# Start new conversation in Genie space
result = client.genie.start_conversation_and_wait(
    space_id="your-genie-space-id",
    message="Which products generate the highest sales?"
)
```

**What happens:**
- Genie receives the user's natural language question
- Genie analyzes the question against the available data schema
- Genie generates appropriate SQL query
- SQL query is executed automatically
- Genie returns a `GenieMessage` object with results

### Step 3: Extract Response Structure
```python
# The GenieMessage contains:
result = GenieMessage(
    id="message-id",
    conversation_id="conversation-id", 
    content="Which products generate the highest sales?",  # User's original question
    attachments=[  # ← The AI response is here!
        GenieAttachment(
            query=GenieQueryAttachment(
                query="SELECT ProductName, TotalValue FROM table ORDER BY TotalValue DESC LIMIT 5",
                statement_id="01f09499-e3f7-1704-aeb7-267d963af87c"  # ← Key for getting results!
            )
        )
    ]
)
```

**What happens:**
- `result.content` = User's original question (not the AI response)
- `result.attachments[].query.query` = Generated SQL query
- `result.attachments[].query.statement_id` = ID to fetch actual results
- `result.conversation_id` = For continuing the conversation

### Step 4: Fetch Actual Query Results
```python
# Use statement execution API to get the actual data
statement_result = client.statement_execution.get_statement(
    statement_id="01f09499-e3f7-1704-aeb7-267d963af87c"
)
```

**What happens:**
- The `statement_id` from Step 3 is used to fetch the actual query results
- This is where the real data comes from, not from the Genie response directly
- Returns a `StatementResponse` with the actual data

### Step 5: Extract Data from Statement Response
```python
# The StatementResponse contains:
statement_result = StatementResponse(
    status=StatementStatus(state="SUCCEEDED"),
    result=ResultData(
        data_array=[
            ["Quinoa & Kale Bowl", "2271.50"],
            ["Vegan Pizza", "2197.25"],
            ["Organic Chili", "2145.00"]
        ]
    ),
    manifest=ResultManifest(
        schema=ResultSchema(
            columns=[
                ColumnInfo(name="ProductName", type_name="STRING"),
                ColumnInfo(name="TotalValue", type_name="DECIMAL")
            ]
        )
    )
)
```

**What happens:**
- `statement_result.result.data_array` = The actual query results as array of arrays
- `statement_result.manifest.schema.columns` = Column metadata (names, types)
- `statement_result.status.state` = Execution status ("SUCCEEDED", "FAILED", etc.)

### Step 6: Format Results for User
```python
# Format the data as a readable table
formatted_table = """
  ProductName   |   TotalValue   
---------------------------------
Quinoa & Kale Bowl |     2271.50    
  Vegan Pizza   |     2197.25    
 Organic Chili  |     2145.00    
"""
```

**What happens:**
- Extract column names from schema
- Format data array into readable table
- Combine SQL query + formatted results for complete response

## 🔑 Key Points for Customers

### Why This Approach?
1. **Complete Data Retrieval**: We get the actual query results, not just the SQL
2. **Proper SDK Usage**: Uses the official Genie SDK with `space_id`
3. **Rate Limiting**: Handles 429 responses with exponential backoff
4. **Error Handling**: Graceful handling of failures and timeouts

### Critical Understanding
- **Genie generates SQL** but doesn't return the actual data in the response
- **Statement execution API** is required to get the actual query results
- **Two-step process**: Genie conversation → Statement execution
- **Rate limiting** is essential for production use

### Common Pitfalls to Avoid
1. ❌ **Don't look for data in `result.content`** - that's the user's question
2. ❌ **Don't use serving endpoints** - use Genie SDK with `space_id`
3. ❌ **Don't ignore rate limiting** - implement proper backoff strategies
4. ❌ **Don't skip statement execution** - that's where the actual data is

## 🛠️ Implementation in Our Solution

### Rate Limiting with Exponential Backoff
```python
def _exponential_backoff(self, func, *args, max_retries=5, base_delay=1.0):
    retries = 0
    while retries < max_retries:
        try:
            return func(*args)
        except DatabricksError as e:
            if e.http_status_code == 429:  # Rate limited
                wait_time = min(
                    base_delay * (2 ** retries),  # Exponential backoff
                    max_backoff
                ) + random.uniform(0, 0.1 * wait_time)  # Add jitter
                time.sleep(wait_time)
                retries += 1
            else:
                raise
```

### Request Queuing
```python
# Queue requests to prevent overwhelming the API
request_id = await queue_manager.submit_request(
    send_message_to_genie,
    conversation_id,
    message
)
```

### Complete Response Assembly
```python
# Combine SQL query + formatted results
response_text = f"Generated SQL: {sql_query}\n\n"
response_text += "Query Results:\n"
response_text += formatted_table
```

## 📊 Example Complete Flow

### Input
```
User: "Which products generate the highest total sales value?"
```

### Step-by-Step Execution
1. **Genie SDK Call**: `client.genie.start_conversation_and_wait(space_id, message)`
2. **SQL Generation**: `SELECT ProductName, TotalValue FROM table ORDER BY TotalValue DESC LIMIT 5`
3. **Statement Execution**: `client.statement_execution.get_statement(statement_id)`
4. **Data Extraction**: `[["Quinoa & Kale Bowl", "2271.50"], ["Vegan Pizza", "2197.25"]]`
5. **Formatting**: Convert to readable table
6. **Response Assembly**: Combine SQL + formatted results

### Output
```
Generated SQL: SELECT ProductName, TotalValue FROM table ORDER BY TotalValue DESC LIMIT 5

Query Results:
  ProductName   |   TotalValue   
---------------------------------
Quinoa & Kale Bowl |     2271.50    
  Vegan Pizza   |     2197.25    
 Organic Chili  |     2145.00    
```

## 🎯 Summary

Our solution provides a **complete, production-ready implementation** that:

- ✅ Uses the proper Genie SDK with `space_id`
- ✅ Fetches actual query results via statement execution API
- ✅ Implements robust rate limiting with exponential backoff
- ✅ Queues requests to prevent API overload
- ✅ Formats results as readable tables
- ✅ Handles errors gracefully
- ✅ Provides comprehensive logging and monitoring

This approach ensures reliable, scalable interaction with Databricks Genie in production environments.
