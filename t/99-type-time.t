#perl -T

use strict;
use warnings;

use Test::More;
use lib 't/lib';
use DuckDBTest;

my $dbh = connect_ok;

$dbh->do(q/SET timezone='UTC'/);

my @TESTS = (
    ["SELECT TIME '1992-09-20 11:30:00.123456'",         '11:30:00.123456'],
    ["SELECT TIMETZ '1992-09-20 11:30:00.123456'",       '11:30:00.123456+00'],
    ["SELECT TIMETZ '1992-09-20 11:30:00.123456-02:00'", '13:30:00.123456+00'],
    ["SELECT TIMETZ '1992-09-20 11:30:00.123456+05:30'", '06:00:00.123456+00'],

    ["SELECT TIME '11:30:00.000005'", '11:30:00.000005'],
    ["SELECT TIME '11:30:00.120'",    '11:30:00.12'],
    ["SELECT TIME '24:00:00'",        '24:00:00'],

    ["SELECT TIMETZ '11:30:00.000005+00'",   '11:30:00.000005+00'],
    ["SELECT time_tz FROM test_all_types()", '00:00:00+15:59:59'],

    # ["SELECT '15:30:00.123456789'::TIME_NS",             '15:30:00.123456789'] # ???
);

foreach my $test (@TESTS) {
    is $dbh->selectrow_arrayref($test->[0])->[0], $test->[1], $test->[0];
}

done_testing;
