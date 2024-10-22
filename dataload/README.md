This command-line tool helps you load CSV files into a PostgreSQL table much faster.

## Features
- Supports glob pattern matching
- Allows you to pass multiple files or multiple valid glob patterns as arguments
- Automatically creates a table with the proper schema (currently only creates schema with TEXT, INT, and FLOAT - for my case these are enough)
- Can load up to 8 files concurrently to speed up data loading
- You can adjust the lookup size which helps the tool to figure out accurate types for the table schema (higher gives more accuracy)
- Uses PostgreSQL `COPY` command, allowing you to load files with millions of rows and sizes in gigabytes

## How to install
```sh
go install github.com/anvesh9652/side-projects/data-load/cmd/load
```

## For any help
```sh
load -h
```