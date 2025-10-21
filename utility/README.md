# DuckDB ODBC GetInfo Utility

This utility uses the DuckDB ODBC driver to retrieve the values used to populate `DBD::DuckDB::GetInfo`.

    make
    LD_PRELOAD=libduckdb_odbc.so ./getinfo

* License: MIT
* Copyright (c) 2025 Giuseppe Di Terlizzi
