package DBD::DuckDB::Appender;

use strict;
use warnings;

use DBD::DuckDB::FFI qw(:all);

sub new {

    my ($class, %params) = @_;

    my $dbh    = delete $params{dbh}   or Carp::croak 'Missing DB handler';
    my $table  = delete $params{table} or Carp::croak 'Missing appender table';
    my $schema = delete $params{schema} // 'main';

    my $rc = duckdb_appender_create($dbh->{duckdb_conn}, $schema, $table, \my $appender);
    return $dbh->set_err(1, "duckdb_appender_create failed for '$schema.$table'") if $rc;

    my $name    = $dbh->quote_identifier($schema, $table);
    my $columns = [];

    my $sth = $dbh->prepare("PRAGMA table_info($name)");
    $sth->execute;

    my $rows = $sth->fetchall_arrayref;
    @$columns = map { $_->[1] } @$rows if $rows;

    my $self = {dbh => $dbh, appender => $appender, table => $table, schema => $schema, columns => $columns};

    return bless $self, $class;
}

sub append {

    my ($self, $val, $type) = @_;

    return $self->{dbh}->set_err(1, 'appender flushed') if $self->{closed};

}

sub append_row {

    my ($self, @args) = @_;

    my %data = @args == 1 && ref($args[0]) eq 'HASH' ? %{$args[0]} : @args;

    my $cols = $self->{columns} // [];
    $self->begin_row or return;

    for my $c (@$cols) {
        if   (exists $data{$c}) { $self->append($data{$c}) or return }
        else                    { $self->append_null       or return }
    }

    $self->end_row;

}

sub begin_row {
    my $self = shift;
    my $rc   = duckdb_appender_begin_row($self->{duckdb_appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_begin_row failed') if $rc;
    return 1;
}

sub end_row {
    my $self = shift;
    my $rc   = duckdb_appender_end_row($self->{duckdb_appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_end_row failed') if $rc;
    return 1;
}

sub flush {

    my $self = shift;

    my $rc = duckdb_appender_flush($self->{duckdb_appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_flush failed') if $rc;
    return 1;

}

sub finish {
    my $self = shift;
    my $rc   = duckdb_appender_destroy($self->{duckdb_appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_destroy failed') if $rc;
    return 1;
}

sub DESTROY { shift->finish }

1;
