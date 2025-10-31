# DuckDB::DBD utility

## getinfo.c

This utility uses the DuckDB ODBC driver to retrieve the values used to populate `DBD::DuckDB::GetInfo`.

    make
    LD_PRELOAD=libduckdb_odbc.so ./getinfo

* License: MIT
* Copyright (c) 2025 Giuseppe Di Terlizzi

## parse-duckdb-h.pl

This utility uses the DuckDB header (`duckdb.h`) to generate the list of C APIs exposed by the `libduckdb.so` library.

    perl parse-duckdb-h.pl --header path/duckdb.h --ffi   # Return the %DUCKDB_FUNCTIONS hash used in DBD::DuckDB::FFI
    perl parse-duckdb-h.pl --header path/duckdb.h --list  # Returns the list of DuckDB functions
    perl parse-duckdb-h.pl --header path/duckdb.h --pod   # Create simple POD documentation

* License: Artistic 2.0
* Copyright (c) 2025 Giuseppe Di Terlizzi
