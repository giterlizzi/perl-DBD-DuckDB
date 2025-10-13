#perl -T

use strict;
use warnings;

use Test::More;
use lib 't/lib';
use DuckDBTest;

use DBD::DuckDB::Type qw(:all);

my $dbh = connect_ok;

SCOPE: {

    ok $dbh->do('CREATE TABLE people (id INTEGER, name VARCHAR)') == 0, 'Create table';

    my $appender = $dbh->x_duckdb_appender('people');

    for (my $i = 1; $i <= 1_000; $i++) {

        $appender->append($i,      DUCKDB_TYPE_INTEGER);
        $appender->append('Larry', DUCKDB_TYPE_VARCHAR);
        $appender->end_row;

    }

    $appender->destroy;

    my $sth = $dbh->prepare('SELECT * FROM people');
    $sth->execute;

    is @{$sth->fetchall_arrayref}, 1_000;

}

done_testing;
