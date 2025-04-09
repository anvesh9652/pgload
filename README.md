# `load`: Faster Loading for CSV & JSONL Data into PostgreSQL

Loading large `CSV` or `JSONL` files into PostgreSQL can often be a slow process. `load` is a command-line tool designed to help speed this up. It leverages PostgreSQL's efficient `COPY` command and parallel processing to handle large datasets, including multi-gigabyte files with millions of rows, more effectively than standard single-file insertion methods.

This tool can be particularly helpful when you need to regularly import large data files and want to automate parts of the process, like table creation.

## Features

*   **Faster CSV Imports:** Uses PostgreSQL's native `COPY` command for efficient bulk loading of `CSV` data.
*   **JSONL File Support:** Can process `JSONL` files (where each line is a JSON object). It converts the data to `CSV` format on the fly and then uses the `COPY` command to load. While the conversion adds some overhead compared to direct `CSV` loading, it's still designed to handle large `JSONL` files effectively.
*   **Handles Large Files:** Tested with multi-gigabyte files containing millions of rows (see examples below).
*   **Concurrent File Loading:** Speeds up loading multiple files by processing them concurrently using 8 internal workers.
*   **File Pattern Matching:** Accepts multiple file paths and supports glob patterns (e.g., `data/*.csv`) for easily selecting files.
*   **Automatic Table Creation:** Analyzes the input file(s) to infer a schema and create the target PostgreSQL table if it doesn't exist.
    *   *Supported data types for auto-schema:* `TEXT`, `NUMERIC`, `JSON`. (This covers common cases but may need manual adjustment for more complex types).
*   **Schema Inference Tuning:** Provides an option (`lookup size`) to adjust how many rows are sampled for determining data types, allowing a trade-off between speed and accuracy.
*   **Compressed File Handling:** Reads `.gz` compressed `CSV` and `JSONL` files directly, avoiding a separate decompression step.

## Installation

Ensure you have Go installed.

```sh
go install github.com/anvesh9652/pgload/cmd/load@latest
```

*(Make sure your Go binary path (`$GOPATH/bin` or `$HOME/go/bin`) is included in your system's `PATH` environment variable.)*

**View Help and Options:**
```sh
load -h
```

### Command-Line Flags

Use these flags to customize the loading process:

| Flag(s)          | Description                                                                       | Default Value     |
| :--------------- | :-------------------------------------------------------------------------------- | :---------------- |
| `-d`, `--database` | Database name to connect to.                                                      | `"postgres"`      |
| `-f`, `--format`   | Input file format. Options: `csv`, `jsonl`, `both`.                               | `"csv"`           |
| `-h`, `--help`     | Show the help message and exit.                                                   | N/A               |
| `-l`, `--lookup`   | Number of initial rows to scan for automatic schema detection (type inference). | `400`             |
| `-P`, `--pass`     | Password for the specified PostgreSQL user.                                       | (none)            |
| `-p`, `--port`     | PostgreSQL server port number (if not using `-u` or default).                     | `5432` |
| `-r`, `--reset`    | Reset (DROP and recreate) tables if they already exist.                           | `true`            |
| `-s`, `--schema`   | Target schema name in the database.                                               | `"public"`        |
| `-t`, `--type`     | Column type strategy: `dynamic` (infer types) or `alltext` (use TEXT for all).  | `"dynamic"`       |
| `-u`, `--url`      | Full connection string/URL for the PostgreSQL server (e.g., `hostname:port`).     | `"localhost:5432"`|
| `-U`, `--user`     | Username for connecting to PostgreSQL.                                            | `"postgres"`      |
| `-v`, `--version`  | Show the application version and exit.                                            | N/A               |


**Example Commands:**

```sh
# Load multiple CSV files (including a compressed one).
# Assumes default format ('csv') and connection settings.
load file1.csv file2.csv file3.csv.gz

# Load multiple JSON/JSONL files (including compressed).
# Explicitly specifies the format using -f jsonl.
load -f jsonl file1.json file2.jsonl file3.json.gz

# Load a CSV file, specifying a non-default PostgreSQL port (54321).
load -p 54321 data.csv

# Load a mix of CSV and JSONL files (-f both).
# Specifies a non-default port and uses a glob pattern (*) to include files.
load -f both -p 54321 data.csv data.json all_files/*

# Load CSV files matching patterns, using specific connection parameters:
# User 'test', Password '123', Database 'temp', Schema 'testing',
# and connects to PostgreSQL at 'localhost:123'.
load -U test -P 123 -d temp -s testing -u "localhost:123" file_2*.csv test1.csv dummy/*/*.csv
```

*(Note: Table names are inferred from filenames.)*

## Loading Speed Stats 

These examples show `load`'s performance loading large files on specific hardware (**MacBook Pro 15-inch, M1 Pro, 10 cores, 16GB RAM**). Your results may vary based on your hardware, database configuration, and network.

<details>
    <summary><b><code>JSONL</code> File Loading Stats</b></summary>

*   **3.3 Million Rows / 4.5GB Uncompressed JSONL:** ~55 seconds
    ```
    ❯ load -f jsonl /path/to/usage_data_3m.json
    status=SUCCESS rows_inserted=3.30M file_size=4.5GB file=/path/to/usage_data_3m.json ... took=54.72s
    ```
*   **4.0 Million Rows / 5.5GB Uncompressed JSONL:** ~1 minute 2 seconds
    ```
    ❯ load -f jsonl /path/to/usage_data_4m.json
    status=SUCCESS rows_inserted=4.00M file_size=5.5GB file=/path/to/usage_data_4m.json ... took=1m2.03s
    ```
*   **5.5 Million Rows / 7.5GB Uncompressed JSONL:** ~1 minute 33 seconds
    ```
    ❯ load -f jsonl /path/to/usage_data_5_5m.json
    status=SUCCESS rows_inserted=5.50M file_size=7.5GB file=/path/to/usage_data_5_5m.json ... took=1m33.15s
    ```
*   **12.55 Million Rows / 17GB Uncompressed JSONL:** ~3 minutes 7 seconds
    ```
    ❯ load -f jsonl /path/to/usage-data.json
    status=SUCCESS rows_inserted=12.55M file_size=17GB file=/path/to/usage-data.json ... took=3m6.60s
    ```
*   **12.55 Million Rows / 486MB Compressed (`.gz`) JSONL:** ~3 minutes 11 seconds
    ```
    ❯ load -s gz -f jsonl /path/to/usage-data.json.gz
    status=SUCCESS rows_inserted=12.55M file_size=486MB file=/path/to/usage-data.json.gz ... took=3m10.61s
    ```

*(Note: Example output slightly condensed. Full paths replaced.)*
</details>

<details>
    <summary><b><code>CSV</code> File Loading Stats</b></summary>

*   **`JetBrains IDE(goland)` ~2min vs `timescaledb-parallel-copy` ~43.5 sec(avg) vs `load` ~41 sec**
<br></br>
![alt text](/images/jetbrains.png)
<br></br>
![alt text](/images/load.png)
<br></br>
## Timescale-db-stats

*  Created the table `converted_3m_timescale` with the same columns listed in the command, with each column type set as TEXT.

* Runs with different configurations

    ```sh
    ❯ go run main.go --connection="host=localhost port=5432 user=postgres sslmode=disable" --table converted_3m_timescale --schema test3 --file "converted_3m.csv" "billing_account_id, service, sku, usage_start_time, usage_end_time, project, labels, system_labels, location, resource, tags, price, subscription, transaction_type, export_time, cost, currency, currency_conversion_rate, usage, credits, invoice, cost_type, adjustment_info, cost_at_list" --skip-header true            
    2025/04/10 00:08:14 Copy command: COPY "test3"."converted_3m_timescale" FROM STDIN WITH DELIMITER ','  CSV
    2025/04/10 00:08:59 total rows 3300001
    COPY 3300001 took 45.508942916s
    ```

    ```sh
    ❯ go run main.go --connection="host=localhost port=5432 user=postgres sslmode=disable" --table converted_3m_timescale --schema test3 --file "converted_3m.csv" "billing_account_id, service, sku, usage_start_time, usage_end_time, project, labels, system_labels, location, resource, tags, price, subscription, transaction_type, export_time, cost, currency, currency_conversion_rate, usage, credits, invoice, cost_type, adjustment_info, cost_at_list" --skip-header true --workers 8
    2025/04/10 00:02:45 Copy command: COPY "test3"."converted_3m_timescale" FROM STDIN WITH DELIMITER ','  CSV
    2025/04/10 00:03:26 total rows 3300001
    COPY 3300001 took 41.389381459s
    ```

    ```sh
    ❯ go run main.go --connection="host=localhost port=5432 user=postgres sslmode=disable" --table converted_3m_timescale --schema test3 --file "converted_3m.csv" "billing_account_id, service, sku, usage_start_time, usage_end_time, project, labels, system_labels, location, resource, tags, price, subscription, transaction_type, export_time, cost, currency, currency_conversion_rate, usage, credits, invoice, cost_type, adjustment_info, cost_at_list" --skip-header true --workers 5
    2025/04/10 00:04:04 Copy command: COPY "test3"."converted_3m_timescale" FROM STDIN WITH DELIMITER ','  CSV
    2025/04/10 00:04:49 total rows 3300001
    COPY 3300001 took 45.222426583s
    ```

    ```sh
    go run main.go --connection="host=localhost port=5432 user=postgres sslmode=disable" --table converted_3m_timescale --schema test3 --file "converted_3m.csv" "billing_account_id, service, sku, usage_start_time, usage_end_time, project, labels, system_labels, location, resource, tags, price, subscription, transaction_type, export_time, cost, currency, currency_conversion_rate, usage, credits, invoice, cost_type, adjustment_info, cost_at_list" --skip-header true --workers 5 --batch-size 10000
    2025/04/10 00:06:01 Copy command: COPY "test3"."converted_3m_timescale" FROM STDIN WITH DELIMITER ','  CSV
    2025/04/10 00:06:42 total rows 3300001
    COPY 3300001 took 42.070157s
    ```

*(CSV loading examples will be added here. Generally, expect faster times than JSONL due to the direct use of `COPY` without the conversion step.)*
</details>


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.