#perl -T

use strict;
use warnings;

use Test::More;
use lib 't/lib';
use DuckDBTest;

# Some FFI::Platypus builds cannot attach a function that returns a record by
# value (Strawberry Perl 5.24, gcc 4.9.2); DBD::DuckDB::FFI then leaves
# duckdb_from_date and its siblings unbound. Reading DATE, TIME, TIMETZ and
# TIMESTAMP values must not need them, so this test removes them and compares
# every decoded value with DuckDB's own text.

my $dbh = connect_ok;

$dbh->do(q/SET timezone='UTC'/);

{
    no strict 'refs';
    undef &{"DBD::DuckDB::FFI::$_"} for qw(duckdb_from_date duckdb_from_time duckdb_from_time_tz duckdb_from_timestamp);
}

sub same_as_cast {

    my ($label, $sql) = @_;

    my $rows = $dbh->selectall_arrayref($sql);
    my @diff = grep { !defined $_->[0] || $_->[0] ne $_->[1] } @$rows;

    ok @$rows && !@diff, sprintf '%s: %d values as DuckDB writes them', $label, scalar @$rows
        or diag explain [@diff[0 .. ($#diff < 4 ? $#diff : 4)]];

}

# One full 400-year Gregorian cycle, and every day around each turn of a
# century from 1600 to 2400, where the leap year rules differ
same_as_cast 'DATE, every 13th day of 1600-03-01 .. 2000-02-29',
    q{SELECT d, CAST(d AS VARCHAR) FROM (SELECT DATE '1600-03-01' + CAST(i AS INTEGER) AS d FROM range(0, 146097, 13) t(i))};

same_as_cast 'DATE, every day of December .. March around 1600, 1700 .. 2400',
    q{SELECT d, CAST(d AS VARCHAR) FROM (SELECT make_date(CAST(y AS INTEGER), 12, 1) + CAST(i AS INTEGER) AS d
        FROM range(1599, 2400, 100) c(y), range(0, 121) t(i))};

# Every day around year 0 (DuckDB has one, written "0001-01-01 (BC)")
same_as_cast 'DATE, every day of 0003 BC .. 0003 AD',
    q{SELECT d, CAST(d AS VARCHAR) FROM (SELECT DATE '0003-01-01 (BC)' + CAST(i AS INTEGER) AS d FROM range(0, 2200) t(i))};

# Wide steps over the whole range DuckDB writes back as a date
same_as_cast 'DATE, every 9973rd day of -5877641 .. 5881580',
    q{SELECT d, CAST(d AS VARCHAR) FROM (SELECT DATE '1970-01-01' + CAST(i AS INTEGER) AS d FROM range(-2147483646, 2147483646, 9973 * 997) t(i))};

same_as_cast 'DATE, edges',
    q{SELECT d, CAST(d AS VARCHAR) FROM (VALUES (DATE '1970-01-01'), (DATE '1969-12-31'), (DATE '2000-02-29'), (DATE '1900-02-28'),
        (DATE '1900-03-01'), (DATE '2100-02-28'), (DATE '0001-01-01'), (DATE '0001-12-31 (BC)'), (DATE '0044-03-15 (BC)'),
        (DATE 'infinity'), (DATE '-infinity')) t(d)};

same_as_cast 'TIMESTAMP, before and after the epoch and BC',
    q{SELECT ts, CAST(ts AS VARCHAR) FROM (VALUES (TIMESTAMP '1969-12-31 23:59:59.5'), (TIMESTAMP '1970-01-01 00:00:00'),
        (TIMESTAMP '2024-02-29 12:00:00.000005'), (TIMESTAMP '0044-03-15 (BC) 12:00:00.5'), (TIMESTAMP '1600-02-29 23:59:59')) t(ts)};

same_as_cast 'TIME, every 997th second of the day and the edges',
    q{SELECT t, CAST(t AS VARCHAR) FROM (SELECT TIME '00:00:00' + INTERVAL (i) SECOND AS t FROM range(0, 86400, 997) r(i)
        UNION ALL SELECT * FROM (VALUES (TIME '00:00:00.000001'), (TIME '23:59:59.999999'), (TIME '24:00:00'), (TIME '11:30:00.12')) v(t))};

# TIMETZ keeps its offset; DuckDB allows -15:59:59 .. +15:59:59
my @timetz;
for (my $offset = -57_599; $offset <= 57_599; $offset += 1_009) {
    for my $time ('00:00:00', '11:30:00.000005', '23:59:59.999999') {
        my $abs = abs $offset;
        push @timetz, sprintf "('%s%s%02d:%02d:%02d')", $time, ($offset < 0 ? '-' : '+'), $abs / 3_600, $abs % 3_600 / 60, $abs % 60;
    }
}
same_as_cast 'TIMETZ, offsets across the allowed range',
    'SELECT t, CAST(t AS VARCHAR) FROM (SELECT CAST(s AS TIMETZ) AS t FROM (VALUES ' . join(', ', @timetz) . ') v(s))';

same_as_cast 'TIMETZ, the edges of the offset range',
    q{SELECT t, CAST(t AS VARCHAR) FROM (VALUES (CAST('24:00:00+15:59:59' AS TIMETZ)), (CAST('00:00:00-15:59:59' AS TIMETZ)),
        (CAST('12:00:00+00' AS TIMETZ))) v(t)};

done_testing;
