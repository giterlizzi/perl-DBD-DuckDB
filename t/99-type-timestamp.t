#perl -T

use strict;
use warnings;

use Test::More;
use Time::Piece;

use lib 't/lib';
use DuckDBTest;

SCOPE: {

    local $ENV{TZ} = 'Europe/Berlin' unless $^O eq 'MSWin32';

    my $dbh = connect_ok;

    ok($dbh->do(q{SET TIMEZONE = 'Europe/Berlin'}), q{SET TIMEZONE = 'Europe/Berlin'});

    my @TESTS = (

        ["SELECT current_setting('TimeZone')", 'Europe/Berlin'],

        ["SELECT timezone('America/Denver', TIMESTAMP '2001-02-16 20:38:40')",   '2001-02-17 04:38:40+01'],
        ["SELECT timezone('America/Denver', TIMESTAMPTZ '2001-02-16 04:38:40')", '2001-02-15 20:38:40'],

        ["SELECT timezone('UTC', TIMESTAMP '2001-02-16 20:38:40+00:00')",           '2001-02-16 21:38:40+01'],
        ["SELECT timezone('UTC', TIMESTAMPTZ '2001-02-16 04:38:40 Europe/Berlin')", '2001-02-16 03:38:40'],
    );

    foreach my $test (@TESTS) {
        is $dbh->selectrow_arrayref($test->[0])->[0], $test->[1], $test->[0];
    }

}

SCOPE: {

    local $ENV{TZ} = 'UTC';

    my $dbh = connect_ok;
    ok($dbh->do(q{SET TIMEZONE = 'UTC'}), q{SET TIMEZONE = 'UTC'});

    my @TESTS = (

        ["SELECT current_setting('TimeZone')", 'UTC'],

        ["SELECT TIMESTAMP_NS '1992-09-20 11:30:00.123456789'", '1992-09-20 11:30:00.123456789'],
        ["SELECT TIMESTAMP '1992-09-20 11:30:00.123456789'",    '1992-09-20 11:30:00.123456'],
        ["SELECT TIMESTAMP_MS '1992-09-20 11:30:00.123456789'", '1992-09-20 11:30:00.123'],
        ["SELECT TIMESTAMP_S '1992-09-20 11:30:00.123456789'",  '1992-09-20 11:30:00'],

        # TODO: require TZ env when no timezone is set in TIMESTAMPTZ
        ["SELECT TIMESTAMPTZ '1992-09-20 11:30:00.123456789'",       '1992-09-20 11:30:00.123456+00'],
        ["SELECT TIMESTAMPTZ '1992-09-20 12:30:00.123456789+01:00'", '1992-09-20 11:30:00.123456+00'],

        ["SELECT '-infinity'::TIMESTAMP", '-infinity'],
        ["SELECT 'epoch'::TIMESTAMP",     '1970-01-01 00:00:00'],
        ["SELECT 'infinity'::TIMESTAMP",  'infinity'],

        ["SELECT 'infinity'::TIMESTAMP_S",     'infinity'],
        ["SELECT '-infinity'::TIMESTAMP_MS",   '-infinity'],
        ["SELECT 'infinity'::TIMESTAMP_NS",    'infinity'],
        ["SELECT '-infinity'::TIMESTAMPTZ",    '-infinity'],

        ["SELECT TIMESTAMP '1992-09-20 11:30:00.000005'",       '1992-09-20 11:30:00.000005'],
        ["SELECT TIMESTAMP '1992-09-20 11:30:00.5'",            '1992-09-20 11:30:00.5'],
        ["SELECT TIMESTAMP_MS '1992-09-20 11:30:00.005'",       '1992-09-20 11:30:00.005'],
        ["SELECT TIMESTAMP_MS '1992-09-20 11:30:00'",           '1992-09-20 11:30:00'],
        ["SELECT TIMESTAMP_NS '1992-09-20 11:30:00.000000005'", '1992-09-20 11:30:00.000000005'],
        ["SELECT TIMESTAMPTZ '1992-09-20 11:30:00.000005+00'",  '1992-09-20 11:30:00.000005+00'],

        ["SELECT TIMESTAMP '1969-12-31 23:59:59.5'",         '1969-12-31 23:59:59.5'],
        ["SELECT TIMESTAMP '1900-01-01 00:00:00.25'",        '1900-01-01 00:00:00.25'],
        ["SELECT TIMESTAMP_MS '1969-12-31 23:59:59.5'",      '1969-12-31 23:59:59.5'],
        ["SELECT TIMESTAMP_NS '1969-12-31 23:59:59.5'",      '1969-12-31 23:59:59.5'],
        ["SELECT TIMESTAMPTZ '1969-12-31 23:59:59.5+00'",    '1969-12-31 23:59:59.5+00'],
        ["SELECT CAST(DATE '0044-03-15 (BC)' AS TIMESTAMP)", '0044-03-15 (BC) 00:00:00'],

        ["SELECT TIMESTAMP '1992-03-22 01:02:03' + INTERVAL 5 DAY", '1992-03-27 01:02:03'],
        ["SELECT TIMESTAMP '1992-03-27' - TIMESTAMP '1992-03-22'",  "5 days"],
        ["SELECT TIMESTAMP '1992-03-22 01:02:03' - INTERVAL 5 DAY", '1992-03-17 01:02:03'],
    );

    foreach my $test (@TESTS) {
        is $dbh->selectrow_arrayref($test->[0])->[0], $test->[1], $test->[0];
    }
}

SKIP: {

    skip 'the local time zone follows $ENV{TZ} only off Windows', 3 if $^O eq 'MSWin32';

    # Local mean time: Asia/Kolkata was 5:53:28 ahead of UTC in 1834
    local $ENV{TZ} = 'Asia/Kolkata';

    my $dbh = connect_ok;
    ok($dbh->do(q{SET TIMEZONE = 'Asia/Kolkata'}), q{SET TIMEZONE = 'Asia/Kolkata'});

    my $sql = q{SELECT TIMESTAMPTZ '1834-12-07 15:27:31.855723+00'};
    is $dbh->selectrow_arrayref($sql)->[0], '1834-12-07 21:20:59.855723+05:53', $sql;

}

SCOPE: {

    local $ENV{TZ} = 'UTC';

    my $dbh = connect_ok;
    ok($dbh->do(q{SET TIMEZONE = 'UTC'}), q{SET TIMEZONE = 'UTC'});

    # Every value as DuckDB writes it, before and after the epoch, with and
    # without fractions, in every precision
    # step in microseconds: about 3 years (2700 BC to 6700 AD), for
    # TIMESTAMP_NS about two months (its range is 1677 to 2262)
    my %step = (TIMESTAMP_NS => 4_938_271_605_493);

    foreach my $type (qw(TIMESTAMP TIMESTAMP_S TIMESTAMP_MS TIMESTAMP_NS TIMESTAMPTZ)) {

        my $step = $step{$type} // 98_765_432_109_877;
        my $sql  = qq{SELECT v, CAST(v AS VARCHAR) FROM (SELECT CAST(make_timestamp(i * $step) AS $type) AS v FROM range(-1500, 1500) t(i))};
        my @diff = grep { $_->[0] ne $_->[1] } @{$dbh->selectall_arrayref($sql)};

        is_deeply \@diff, [], "$type matches CAST(... AS VARCHAR)";

    }

}

done_testing;
