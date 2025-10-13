#perl -T

use strict;
use warnings;

use Test::More;
use lib 't/lib';
use DuckDBTest;

my $create_table_sql = <<'END_SQL';
CREATE TABLE tbl (
    id INTEGER DEFAULT nextval('id_sequence'),
    s VARCHAR
)
END_SQL

my $dbh = connect_ok;

ok $dbh->do(q{CREATE SEQUENCE id_sequence START 1}) == 0, 'Create sequence';

ok $dbh->do($create_table_sql) == 0, 'Create table with sequence';

ok $dbh->do(q{INSERT INTO tbl (s) VALUES ('hello'), ('world')}) == 2, 'Insert data';

SCOPE: {

    my $sth = $dbh->prepare('SELECT * FROM tbl');
    $sth->execute;

    is_deeply $sth->fetchall_arrayref, [[1, 'hello'], [2, 'world']], 'Fetch all rows';
}

SCOPE: {

    my $sth = $dbh->prepare('SELECT MAX(id) FROM tbl');
    $sth->execute;

    ok $sth->fetchrow_arrayref->[0] == 2, 'last insert id #1';

}

SCOPE: {

    my $sth = $dbh->prepare(q{INSERT INTO tbl (s) VALUES ('hi') RETURNING id});
    $sth->execute();

    ok $sth->fetchrow_hashref->{id} == 3, 'last insert id #2';

}

done_testing;
