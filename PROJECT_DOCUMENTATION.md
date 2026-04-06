# NLQ Agent Project Documentation

## What this project does

This project is a Streamlit app that lets users:
- ask questions in plain English about MySQL data
- generate safe SQL from those questions
- upload an Excel file with formulas
- convert the Excel logic into SQL
- run the SQL on MySQL
- get a human-readable answer
- view charts when they ask for a plot or visualization

The app also creates detailed log files for each request.


## Main app file

The current main app is:
[app.py](C:/Users/shradha/Documents/NLQ_agent/app.py)

Run it with:

```powershell
streamlit run app.py
```


## Main flows

### 1. Chat NLQ tab

This tab lets the user ask questions directly about the database.

Flow:
1. User asks a question in plain English.
2. The app reads schema and metadata.
3. A metadata catalog is used to understand likely columns and values.
4. LangChain table context is also collected when available.
5. The AI generates a safe SQL `SELECT` query.
6. The SQL is checked and run on MySQL.
7. The app shows:
   - a human answer
   - a table of rows
   - a chart if the user asked for a plot or graph
8. A log file is saved in `logs/`.


### 2. Excel -> SQL -> Run tab

This tab lets the user upload an Excel template and generate SQL from its headers and formulas.

Flow:
1. User uploads an Excel file.
2. The app reads:
   - sheet name
   - headers
   - Excel column mapping
   - formulas
3. User selects the MySQL table.
4. The AI converts Excel formulas into SQL expressions.
5. The SQL is checked and run on MySQL.
6. The app shows:
   - a human answer
   - a table of rows
   - a chart if the user asks for a visualization in the Excel result question box
7. A log file is saved in `logs/`.

This tab now also includes:
- `Rows to return` with default `10`
- `Ask a question about the generated result (optional)`

That means the Excel flow can now:
- generate SQL from the Excel template
- run it
- explain the result in plain language
- draw a chart from the result when requested


## Root folder overview

### [app.py](C:/Users/shradha/Documents/NLQ_agent/app.py)
- Main Streamlit app.
- Contains both the chat tab and the Excel tab.
- Handles SQL generation, result display, chart rendering, and logging.

### [PROJECT_DOCUMENTATION.md](C:/Users/shradha/Documents/NLQ_agent/PROJECT_DOCUMENTATION.md)
- This file.
- Explains the current project structure and recent updates.

### [README.md](C:/Users/shradha/Documents/NLQ_agent/README.md)
- Basic setup notes.
- May need small cleanup if it does not fully match the latest app flow.

### [requirements.txt](C:/Users/shradha/Documents/NLQ_agent/requirements.txt)
- Python dependencies for the project.
- Includes Streamlit, OpenAI-related support, Pandas, Plotly, SQLAlchemy, and LangChain packages.

### [sqlite_db.py](C:/Users/shradha/Documents/NLQ_agent/sqlite_db.py)
- Utility script to load CSV data into MySQL.
- Cleans column names and uploads data to a table.

### [dummy_inventory_kpi_template.xlsx](C:/Users/shradha/Documents/NLQ_agent/dummy_inventory_kpi_template.xlsx)
- Dummy Excel file for testing the Excel-to-SQL flow.
- Includes base columns and KPI formula columns.

### [Current Inventory .xlsx](C:/Users/shradha/Documents/NLQ_agent/Current%20Inventory%20.xlsx)
- Existing Excel file in the project folder.

### [Current_Inventory.csv](C:/Users/shradha/Documents/NLQ_agent/Current_Inventory.csv)
- Existing CSV file used for data loading and testing.


## Important folders

### `nlq/`

This folder contains the main Python modules.

### `logs/`

Stores one `.log` file per user request.

### `metadata/`

Stores the generated metadata catalog:
[catalog.json](C:/Users/shradha/Documents/NLQ_agent/metadata/catalog.json)

### `uploads/`

Stores uploaded Excel files during app usage.


## `nlq/` package overview

### [nlq/config.py](C:/Users/shradha/Documents/NLQ_agent/nlq/config.py)
- Reads `.env`
- Builds the `Settings` object
- Stores DB connection values and allowed tables

### [nlq/db.py](C:/Users/shradha/Documents/NLQ_agent/nlq/db.py)
- Connects to MySQL
- Reads schema from `INFORMATION_SCHEMA.COLUMNS`
- Runs SQL queries
- Fetches distinct values and value stats for metadata building

### [nlq/agent.py](C:/Users/shradha/Documents/NLQ_agent/nlq/agent.py)
- Main AI wrapper
- Uses OpenAI client
- Generates SQL
- Generates human answers
- Repairs broken SQL when needed

### [nlq/sql_safety.py](C:/Users/shradha/Documents/NLQ_agent/nlq/sql_safety.py)
- Validates SQL
- Allows only `SELECT`
- Blocks unsafe SQL words
- Adds `LIMIT` if missing
- Now correctly detects existing `LIMIT` even across line breaks

### [nlq/excel_parser.py](C:/Users/shradha/Documents/NLQ_agent/nlq/excel_parser.py)
- Reads uploaded Excel files
- Extracts:
  - headers
  - header to column mapping
  - formulas
  - sheet details

### [nlq/query_logging.py](C:/Users/shradha/Documents/NLQ_agent/nlq/query_logging.py)
- Creates one structured log file per request
- Stores JSON line logs
- Adds basic derived intent information

### [nlq/intent_resolution.py](C:/Users/shradha/Documents/NLQ_agent/nlq/intent_resolution.py)
- Resolves likely user intent from metadata
- Matches likely columns and values
- Uses metadata-driven matching instead of only static rules

### [nlq/metadata_catalog.py](C:/Users/shradha/Documents/NLQ_agent/nlq/metadata_catalog.py)
- Builds a metadata catalog from the live database
- Stores schema, column types, distinct counts, and distinct values for low-cardinality text columns
- Saves the catalog to JSON

### [nlq/langchain_context.py](C:/Users/shradha/Documents/NLQ_agent/nlq/langchain_context.py)
- Adds optional LangChain database context
- Uses `SQLDatabase.get_table_info()`
- Gives extra schema context for better SQL generation

### [nlq/db_langchain.py](C:/Users/shradha/Documents/NLQ_agent/nlq/db_langchain.py)
- Alternate LangChain-based SQL pipeline
- Can generate schema context and run SQL
- Not the main active path in `app.py`, but parts of it support LangChain integration

### [nlq/__init__.py](C:/Users/shradha/Documents/NLQ_agent/nlq/__init__.py)
- Package marker


## Metadata catalog

The app now uses a metadata catalog to improve intent and value matching.

Main idea:
- read table names
- read column names
- read column types
- collect distinct values for low-cardinality text columns
- use those values to better map user phrases to real DB values

Example:
- user asks: `how many semifinished products are there in material types?`
- metadata catalog may show `material_type` contains `Semifinished products`
- app can match the full phrase instead of only `semifinished`

Catalog file:
[catalog.json](C:/Users/shradha/Documents/NLQ_agent/metadata/catalog.json)


## LangChain support

The project now also includes LangChain support for extra DB context.

What it is used for:
- table info
- usable tables
- extra schema context during SQL generation

Main file:
[langchain_context.py](C:/Users/shradha/Documents/NLQ_agent/nlq/langchain_context.py)

Important note:
- the app does not fully run as a LangChain agent
- but it does use LangChain database context to improve the prompt


## Logging system

Each request creates one `.log` file in `logs/`.

Example log file name:
`20260406_123808_522611_chat_how_many_columns_are_there_in_my_database_9f325929.log`

Logs are stored as JSON lines.

Common log steps:
- `log_created`
- `schema_context`
- `metadata_catalog`
- `langchain_context`
- `derived_intent`
- `value_resolution`
- `sql_generated`
- `sql_truncated`
- `sql_repaired`
- `sql_validated`
- `sql_executed`
- `sql_executed_after_repair`
- `human_response_generated`
- `visualization_requested`
- `visualization_plan`
- `visualization_rendered`
- `visualization_failed`
- `sql_rejected`
- `sql_execution_failed`
- `sql_repair_failed`


## Chat tab details

The chat tab now does more than simple SQL generation.

It includes:
- schema context
- metadata catalog summary
- LangChain table context
- value resolution for likely filters
- optional visualization support

If the user asks for a chart, graph, plot, trend, histogram, scatter, or pie chart, the app tries to build a Plotly chart from the SQL result.


## Excel tab details

The Excel tab now supports:
- uploaded Excel templates
- formula extraction
- SQL generation from Excel logic
- SQL repair if generated SQL is broken or cut off
- user question about the generated result
- visualization on the returned result

It also uses:
- larger SQL token budget for long formula-driven SQL
- retry and repair logic when SQL is incomplete or fails


## Dummy Excel file

Test file:
[dummy_inventory_kpi_template.xlsx](C:/Users/shradha/Documents/NLQ_agent/dummy_inventory_kpi_template.xlsx)

It contains:
- real base columns from `dummy_current_inventory2`
- KPI columns such as:
  - `total_stock`
  - `stock_gap_vs_safety`
  - `coverage_ratio`
  - `inventory_health`
  - `demand_risk_flag`
  - `estimated_value_proxy`

This file is meant for testing the Excel-to-SQL path in the app.


## Visualization support

Both tabs now support charts when the user asks for them.

The app uses Plotly and tries to understand phrases like:
- `plot total_stock by material_type`
- `show a bar chart of demand by product_category`
- `top 10 materials by shelf_stock`
- `scatter x-axis shelf_stock y-axis demand`
- `line chart of coverage_ratio by material_type`

Visualization logic is in:
[app.py](C:/Users/shradha/Documents/NLQ_agent/app.py)


## Recent updates made in this project

### 1. Per-question structured logging
- Added one log file per request
- Stored in `logs/`
- Includes step-by-step debugging details

### 2. Intent logging cleanup
- Split old schema/intention confusion into better log steps
- Added `schema_context`
- Added `derived_intent`

### 3. Dynamic metadata catalog
- Added metadata-driven value matching
- Replaced only-static intent/value handling

### 4. LangChain context support
- Added optional LangChain table info into the prompt

### 5. Excel SQL repair flow
- Added retry and repair logic for broken or truncated Excel SQL

### 6. Better SQL limit handling
- Default row handling was improved
- Existing `LIMIT` detection now works correctly

### 7. Excel result question support
- User can now ask a plain-language question about the Excel-generated result

### 8. Visualization support
- Added chart rendering in both tabs
- Added question-aware chart selection


## Dependencies

Main packages used:
- `streamlit`
- `pandas`
- `plotly`
- `pymysql`
- `openpyxl`
- `python-dotenv`
- `SQLAlchemy`
- `langchain-community`
- `langchain-core`
- `langchain-openai`


## Useful commands

### Run the app
```powershell
streamlit run app.py
```

### Install dependencies
```powershell
pip install -r requirements.txt
```

### Quick compile check
```powershell
python -m py_compile app.py nlq\agent.py nlq\db.py nlq\query_logging.py nlq\intent_resolution.py nlq\metadata_catalog.py nlq\langchain_context.py nlq\db_langchain.py nlq\sql_safety.py
```


## Environment variables you likely need

Important `.env` values:
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `OPENAI_FALLBACK_MODEL`
- `OPENAI_MAX_TOKENS_SQL`
- `OPENAI_MAX_TOKENS_ANSWER`
- `OPENAI_MAX_TOKENS_REPAIR`
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_DB`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `ALLOW_TABLES`
- `EXCEL_FILE_PATH`
- `EXCEL_SHEET_NAME`


## Short summary

This project is now a Streamlit NLQ + Excel-to-SQL app that:
- turns English into SQL
- uses metadata to better understand values
- uses LangChain context for richer schema grounding
- supports Excel formula translation
- repairs broken SQL when needed
- creates one structured log file per request
- answers results in plain language
- draws charts when the user asks for visual output
