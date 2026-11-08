#perl -T

use strict;
use warnings;

use Test::More;

use lib 't/lib';
use DuckDBTest;

my $dbh = connect_ok;

SCOPE: {

    my $sql = <<'END_SQL';
SELECT
    INTERVAL 1 YEAR,                -- single unit using YEAR keyword; stored as 12 months
    INTERVAL (random() * 10) YEAR,  -- parentheses necessary for variable amounts;
                                    -- stored as integer number of months
    INTERVAL '1 month 1 day',       -- string type necessary for multiple units; stored as (1 month, 1 day)
    '16 months'::INTERVAL,          -- string cast supported; stored as 16 months
    '48:00:00'::INTERVAL,           -- HH::MM::SS string supported; stored as (48 * 60 * 60 * 1e6 microseconds)
END_SQL

    my $row = $dbh->selectrow_arrayref($sql);

    is $row->[0], '1 year', 'interval 1 year';
    like $row->[1], qr/^\d+ year|0/, 'interval random years';
    is $row->[2], '1 month 1 day',   'interval 1 month 1 day';
    is $row->[3], '1 year 4 months', 'interval 16 months';
    is $row->[4], '48:00:00',        'interval 48 hours';

}

SCOPE: {

    my $got      = $dbh->selectall_arrayref("SELECT DATE '2000-01-01' + INTERVAL (i) MONTH FROM range(12) t(i)");
    my @expected = map { [sprintf '2000-%02d-01 00:00:00', $_] } 1 .. 12;

    is_deeply $got, \@expected, 'generate interval from range';

}

SCOPE: {

    my $sql = <<'END_SQL';
SELECT
    datepart('decade', INTERVAL 12 YEARS),                  -- returns 1
    datepart('year', INTERVAL 12 YEARS),                    -- returns 12
    datepart('second', INTERVAL 1_234 MILLISECONDS),        -- returns 1
    datepart('microsecond', INTERVAL 1_234 MILLISECONDS),   -- returns 1_234_000
END_SQL

    my $got      = $dbh->selectall_arrayref($sql);
    my $expected = [[1, 12, 1, 1234000]];

    is_deeply $got, $expected, 'datepart interval';

}

SCOPE: {

    my $sql = <<'END_SQL';
SELECT
    DATE '2000-01-01' + INTERVAL 1 YEAR,
    TIMESTAMP '2000-01-01 01:33:30' - INTERVAL '1 month 13 hours',
    TIME '02:00:00' - INTERVAL '3 days 23 hours', -- wraps; equals TIME '03:00:00'
END_SQL

    my $got      = $dbh->selectall_arrayref($sql);
    my $expected = [['2001-01-01 00:00:00', '1999-11-30 12:33:30', '03:00:00']];

    is_deeply $got, $expected, 'arithmetic with timestamps, dates and intervals';
}

SCOPE: {

    my @TESTS = (
        ["SELECT INTERVAL '-1 year'",                                 '-1 year'],
        ["SELECT INTERVAL '-17 months'",                              '-1 year -5 months'],
        ["SELECT INTERVAL '30 months'",                               '2 years 6 months'],
        ["SELECT INTERVAL '-2 days'",                                 '-2 days'],
        ["SELECT INTERVAL '1 month -1 day'",                          '1 month -1 day'],
        ["SELECT INTERVAL '1 year 2 months 3 days 04:05:06.000007'", '1 year 2 months 3 days 04:05:06.000007'],
        ["SELECT INTERVAL '1 day 01:00:00'",                          '1 day 01:00:00'],
        ["SELECT INTERVAL '-1 day -01:00:00'",                        '-1 day -01:00:00'],
        ["SELECT INTERVAL '00:00:00.000005'",                         '00:00:00.000005'],
        ["SELECT INTERVAL '-00:00:01.5'",                             '-00:00:01.5'],
        ["SELECT INTERVAL '0 seconds'",                               '00:00:00'],
    );

    foreach my $test (@TESTS) {
        is $dbh->selectrow_arrayref($test->[0])->[0], $test->[1], $test->[0];
    }

}

SCOPE: {

    # Every value as DuckDB writes it: months, days and time, each with
    # either sign
    my $sql = <<'END_SQL';
SELECT v, CAST(v AS VARCHAR) FROM (
    SELECT to_months(CAST(i % 41 - 20 AS INTEGER)) + to_days(CAST(i % 37 - 18 AS INTEGER))
         + to_microseconds(CAST(i * 7654321987 - 6000000000000 AS BIGINT)) AS v
    FROM range(0, 1600) t(i)
)
END_SQL

    my @diff = grep { $_->[0] ne $_->[1] } @{$dbh->selectall_arrayref($sql)};

    is_deeply \@diff, [], 'INTERVAL matches CAST(... AS VARCHAR)';

}

done_testing;
