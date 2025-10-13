[![Release](https://img.shields.io/github/release/giterlizzi/perl-DBD-DuckDB.svg)](https://github.com/giterlizzi/perl-DBD-DuckDB/releases) [![Actions Status](https://github.com/giterlizzi/perl-DBD-DuckDB/workflows/linux/badge.svg)](https://github.com/giterlizzi/perl-DBD-DuckDB/actions) [![License](https://img.shields.io/github/license/giterlizzi/perl-DBD-DuckDB.svg)](https://github.com/giterlizzi/perl-DBD-DuckDB) [![Starts](https://img.shields.io/github/stars/giterlizzi/perl-DBD-DuckDB.svg)](https://github.com/giterlizzi/perl-DBD-DuckDB) [![Forks](https://img.shields.io/github/forks/giterlizzi/perl-DBD-DuckDB.svg)](https://github.com/giterlizzi/perl-DBD-DuckDB) [![Issues](https://img.shields.io/github/issues/giterlizzi/perl-DBD-DuckDB.svg)](https://github.com/giterlizzi/perl-DBD-DuckDB/issues) [![Coverage Status](https://coveralls.io/repos/github/giterlizzi/perl-DBD-DuckDB/badge.svg)](https://coveralls.io/github/giterlizzi/perl-DBD-DuckDB)

# DBD::DuckDB - DBI Driver for DuckDB

## Synopsis

```.pl
use DBI;

my $dbh = DBI->connect('dbi:DuckDB:dbname=:memory:');

my $sth = $dbh->prepare(q{SELECT * FROM read_csv('myfile.csv')});
$sth->execute;

while (my $row = $sth->fetchrow_hashref) {
    # ...
}

```


## Install

Using Makefile.PL:

To install `DBD::DuckDB` distribution, run the following commands.

    perl Makefile.PL
    make
    make test
    make install

Using App::cpanminus:

    cpanm DBD::DuckDB


## Documentation

 - `perldoc DBD::DuckDB`
 - https://metacpan.org/release/DBD::DuckDB
 - https://duckdb.org/


## Copyright

 - Copyright 2024-2025 © Giuseppe Di Terlizzi
