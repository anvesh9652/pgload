This command-line tool helps you load `CSV` and `JSONL` files into a PostgreSQL table much faster.

## Features
- Supports glob pattern matching
- Allows you to pass multiple files or multiple valid glob patterns as arguments
- Automatically creates a table with the proper schema (currently only creates schema with TEXT, INT, and FLOAT - for my case, these are enough)
- Can load up to 8 files concurrently to speed up data loading
- You can adjust the lookup size, which helps the tool to figure out accurate types for the table schema (higher gives more accuracy)
- For `CSV` files, uses PostgreSQL `COPY` command, allowing you to load files with millions of rows and sizes in gigabytes
- For `JSONL` files, it first converts the data into `CSV` and then follows the regular `CSV` data loading process. Due to this conversion, it might not be as performant as `CSV` loading, but it can still handle millions of rows.

Note: `JSONL` means the files where each row is a `JSON` object

## How to install
```sh
go install github.com/anvesh9652/side-projects/dataload/cmd/load@latest
```

## Help and Example Commands
```sh
load -h
```


## Stats for `jsonl` file loading
**Mac 15 inch - M1 Pro** || **10 cores and 16gb RAM**
```bash
➜ side-projects • own ✗ git:(support-for-gz-files) ✗ 
❯ load -s gz -f "both" /Users/agali/Downloads/temp/my_data/usage_data_3m.json 
status=SUCCESS rows_inserted=3.30M file_size=4.5GB file=/Users/agali/Downloads/temp/my_data/usage_data_3m.json
msg="final load stats" data_format="JSONL" total=1 success=1 failed=0 total_rows_inserted=3.30M took=54.725207917s

➜ side-projects • own ✗ git:(support-for-gz-files) ✗ 
❯ load -s gz -f "both" /Users/agali/Downloads/temp/my_data/usage_data_4m.json
status=SUCCESS rows_inserted=4.00M file_size=5.5GB file=/Users/agali/Downloads/temp/my_data/usage_data_4m.json
msg="final load stats" data_format="JSONL" total=1 success=1 failed=0 total_rows_inserted=4.00M took=1m2.032765709s

➜ side-projects • own ✗ git:(support-for-gz-files) ✗ 
❯ load -s gz -f "both" /Users/agali/Downloads/temp/my_data/usage_data_5_5m.json
status=SUCCESS rows_inserted=5.50M file_size=7.5GB file=/Users/agali/Downloads/temp/my_data/usage_data_5_5m.json
msg="final load stats" data_format="JSONL" total=1 success=1 failed=0 total_rows_inserted=5.50M took=1m33.149445208s

➜ side-projects • own ✗ git:(support-for-gz-files) ✗ 
❯ load -s gz -f "both" /Users/agali/Downloads/temp/my_data/usage-data.json   
status=SUCCESS rows_inserted=12.55M file_size=17GB file=/Users/agali/Downloads/temp/my_data/usage-data.json
msg="final load stats" data_format="JSONL" total=1 success=1 failed=0 total_rows_inserted=12.55M took=3m6.597653s

➜ side-projects • own ✗ git:(support-for-gz-files) ✗ 
❯ load -s gz -f "both" /Users/agali/Downloads/temp/my_data/usage-data.json.gz
status=SUCCESS rows_inserted=12.55M file_size=486MB file=/Users/agali/Downloads/temp/my_data/usage-data.json.gz
msg="final load stats" data_format="JSONL" total=1 success=1 failed=0 total_rows_inserted=12.55M took=3m10.609226875s
```