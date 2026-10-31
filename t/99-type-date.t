#perl -T

use strict;
use warnings;

use Test::More;

use lib 't/lib';
use DuckDBTest;

my $dbh = connect_ok;

my @TESTS = (
    ["SELECT DATE '1992-09-20'", [['1992-09-20']]],

    ["SELECT DATE '1992-03-22' + 5",              [['1992-03-27']]],
    ["SELECT DATE '1992-03-22' + INTERVAL 5 DAY", [['1992-03-27 00:00:00']]],
    [
        "SELECT DATE '1992-03-22' + INTERVAL (d.days) DAY FROM (VALUES (5), (11)) d(days)",
        [['1992-03-27 00:00:00'], ['1992-04-02 00:00:00']]
    ],

    ["SELECT DATE '1992-03-27' - DATE '1992-03-22'", [[5]]],
    ["SELECT DATE '1992-03-27' - INTERVAL 5 DAY",    [['1992-03-22 00:00:00']]],
    [
        "SELECT DATE '1992-03-27' - INTERVAL (d.days) DAY FROM (VALUES (5), (11)) d(days)",
        [['1992-03-22 00:00:00'], ['1992-03-16 00:00:00']]
    ],

    ["SELECT '-infinity'::DATE", [['-infinity']]],
    ["SELECT 'epoch'::DATE",     [['1970-01-01']]],
    ["SELECT 'infinity'::DATE",  [['infinity']]],

    ["SELECT DATE '1969-12-31'",      [['1969-12-31']]],
    ["SELECT DATE '0001-01-01'",      [['0001-01-01']]],
    ["SELECT DATE '0001-12-31 (BC)'", [['0001-12-31 (BC)']]],
    ["SELECT DATE '0044-03-15 (BC)'", [['0044-03-15 (BC)']]],
);

foreach my $test (@TESTS) {
    my $desc     = $test->[0];
    my $got      = $dbh->selectall_arrayref($test->[0]);
    my $expected = $test->[1];
    is_deeply $got, $expected, $desc;
}

SCOPE: {

    # Every value as DuckDB writes it, from 30000 BC to 30000 AD
    my $sql = q{SELECT d, CAST(d AS VARCHAR) FROM (SELECT DATE '1970-01-01' + CAST(i * 7919 AS INTEGER) AS d FROM range(-1400, 1400) t(i))};
    my @diff = grep { $_->[0] ne $_->[1] } @{$dbh->selectall_arrayref($sql)};

    is_deeply \@diff, [], 'DATE matches CAST(... AS VARCHAR)';

}

done_testing;
