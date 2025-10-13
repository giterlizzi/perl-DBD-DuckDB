package DBD::DuckDB::Appender;

use strict;
use warnings;

use DBD::DuckDB::FFI  qw(:all);
use DBD::DuckDB::Type qw(:all);


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

sub error { duckdb_appender_error(shift->{appender}) }


sub append {

    my ($self, $value, $type) = @_;

    return $self->{dbh}->set_err(1, 'appender flushed') if $self->{closed};

    unless (defined $value) {
        return duckdb_append_null($self->{appender});
    }

    if ($type == DUCKDB_TYPE_VARCHAR) {
        return duckdb_append_varchar($self->{appender}, $value);
    }

    if ($type == DUCKDB_TYPE_INTEGER) {
        return duckdb_append_int32($self->{appender}, $value);
    }

}

sub begin_row {
    my $self = shift;
    my $rc   = duckdb_appender_begin_row($self->{appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_begin_row failed') if $rc;
    return 1;
}

sub end_row {
    my $self = shift;
    my $rc   = duckdb_appender_end_row($self->{appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_end_row failed') if $rc;
    return 1;
}

sub flush {
    my $self = shift;
    my $rc   = duckdb_appender_flush($self->{appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_flush failed') if $rc;
    return 1;

}

sub close {
    my $self = shift;
    my $rc   = duckdb_appender_close($self->{appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_flush failed') if $rc;
    return 1;
}

sub destroy {
    my $self = shift;
    my $rc   = duckdb_appender_destroy(\$self->{appender});
    return $self->{dbh}->set_err(1, 'duckdb_appender_destroy failed') if $rc;
    $self->{closed} = 1;
    return 1;
}

sub DESTROY {
    my $self = shift;
    $self->destroy unless $self->{closed};
}

1;
