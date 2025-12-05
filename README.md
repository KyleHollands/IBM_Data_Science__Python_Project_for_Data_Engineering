# IBM Data Science - Python Project for Data Engineering

## Project Overview

In this project, I developed an automated ETL (Extract, Transform, Load) pipeline to compile and analyze data on the world's largest banks. Key activities included:

* **Web Scraping**: Extracted bank data from Wikipedia using BeautifulSoup to identify the top 10 largest banks by market capitalization.
* **Data Transformation**: Applied currency conversions (GBP, EUR, INR) using exchange rate data and Pandas for efficient data manipulation.
* **Data Storage**: Saved processed data to both CSV format and SQLite database for flexible data access and querying.
* **Database Operations**: Implemented SQL queries to analyze and retrieve specific information from the database.
* **Process Logging**: Created comprehensive logging system to track ETL pipeline execution stages.

This project showcases proficiency in Python, web scraping, data engineering principles, and database management while demonstrating the ability to build automated data pipelines for real-world applications.

## Project Details: World's Largest Banks Data Pipeline

This hands-on project focused on creating an automated system to extract, transform, and load information about the world's top 10 largest banks ranked by market capitalization. The pipeline is designed to be executed quarterly to generate updated financial reports.

### Key Components

#### 1. Data Extraction
- Implemented **web scraping** using BeautifulSoup and requests libraries
- Extracted bank data from Wikipedia's "List of largest banks" page
- Targeted specific HTML table using span ID `"By_market_capitalization"`
- Parsed bank names and market capitalization values in USD billions

#### 2. Data Transformation
- Loaded exchange rate information from CSV file
- Converted market capitalization to multiple currencies:
  - **GBP** (British Pound Sterling)
  - **EUR** (Euro)
  - **INR** (Indian Rupee)
- Applied currency conversions using dictionary mapping
- Rounded all values to 2 decimal places for consistency

#### 3. Data Loading
- **CSV Export**: Saved transformed data to `Largest_banks_data.csv`
- **Database Storage**: Loaded data into SQLite database (`Banks.db`)
- Created table `Largest_Banks` with all transformed columns
- Implemented `to_sql()` with replace functionality for easy updates

#### 4. Database Querying
- Executed multiple SQL queries for data analysis:
  - Retrieved all records from the database
  - Calculated average market capitalization in GBP
  - Selected top 5 banks by name
- Displayed query results in formatted tables using Pandas

#### 5. Process Logging
- Implemented comprehensive logging system with timestamps
- Tracked each stage of the ETL pipeline:
  - Extraction initiation and completion
  - Transformation process
  - CSV file creation
  - Database connection and data loading
  - Query execution
  - Process completion
- Logged entries saved to `code_log.txt` with format: `YYYY-Mon-DD HH:MM:SS, Message`

### Technologies Used

- **Python Libraries**:
  - `requests`: HTTP requests for web page retrieval
  - `BeautifulSoup`: HTML parsing and web scraping
  - `pandas`: Data manipulation, transformation, and analysis
  - `sqlite3`: Database creation and management
  - `numpy`: Numerical operations support
  - `datetime`: Timestamp generation for logging

### Project Structure

The project is organized into modular functions:
1. **log_progress()**: Logs pipeline execution stages with timestamps
2. **extract()**: Scrapes bank data from Wikipedia
3. **transform()**: Applies currency conversions using exchange rates
4. **load_to_csv()**: Exports data to CSV file
5. **load_to_db()**: Loads data into SQLite database
6. **run_query()**: Executes SQL queries and displays results
7. **main()**: Orchestrates the complete ETL workflow

### Configuration Parameters

| Parameter | Value |
|-----------|-------|
| Code name | banks_project.py |
| Data URL | https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks |
| Exchange rate CSV | Project Files/exchange_rate.csv |
| Initial Attributes | Name, MC_USD_Billion |
| Final Attributes | Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion |
| Output CSV | Largest_banks_data.csv |
| Database name | Banks.db |
| Table name | Largest_Banks |
| Log file | code_log.txt |

### Skills Demonstrated

- ETL pipeline design and implementation
- Web scraping with HTML parsing
- Data transformation and currency conversion
- CSV file handling and data export
- SQLite database creation and management
- SQL query execution and result presentation
- Structured logging for process tracking
- Modular function design for maintainability
- Error handling and exception management

### Outcome

Successfully created a fully automated ETL pipeline that extracts current bank market capitalization data, transforms it into multiple currencies, and stores it in both CSV and database formats. The system includes comprehensive logging and querying capabilities, making it suitable for quarterly financial report generation and analysis.

### Sample Output

**Top 5 Banks by Market Capitalization:**

| Name | MC_USD_Billion | MC_GBP_Billion | MC_EUR_Billion | MC_INR_Billion |
|------|----------------|----------------|----------------|----------------|
| JPMorgan Chase | 432.92 | 346.34 | 402.62 | 35910.71 |
| Bank of America | 231.52 | 185.22 | 215.31 | 19204.58 |
| Industrial and Commercial Bank of China | 194.56 | 155.65 | 180.94 | 16138.75 |
| Agricultural Bank of China | 160.68 | 128.54 | 149.43 | 13328.41 |
| HDFC Bank | 157.91 | 126.33 | 146.86 | 13098.63 |