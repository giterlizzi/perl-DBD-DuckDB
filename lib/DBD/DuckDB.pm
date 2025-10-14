package DBD::DuckDB {

    use strict;
    use warnings;
    use DBI ();

    use DBD::DuckDB::FFI qw(duckdb_library_version);

    our $VERSION = '0.12';
    $VERSION =~ tr/_//d;

    our $drh;
    our $methods_are_installed;

    DBD::DuckDB::FFI->init unless $drh;

    sub driver {

        return $drh if $drh;

        DBI->setup_driver('DBD::DuckDB');

        unless ($methods_are_installed) {

            DBD::DuckDB::db->install_method('x_duckdb_appender');
            DBD::DuckDB::db->install_method('x_duckdb_version');

            $methods_are_installed++;

        }

        my ($class, $attr) = @_;
        $class .= "::dr";

        my $lib_version = duckdb_library_version();

        $drh = DBI::_new_drh(
            $class,
            {
                Name        => 'DuckDB',
                Version     => $VERSION,
                Attribution => "DBD::DuckDB $VERSION using DuckDB $lib_version by Giuseppe Di Terlizzi",

            }
        ) or return undef;
        return $drh;
    }

    sub CLONE { undef $drh }

}


package    # hide from PAUSE
    DBD::DuckDB::dr {

    use strict;
    use warnings;
    use DBI;
    use base qw(DBD::_::dr);

    use DBD::DuckDB::FFI qw(:all);

    our $imp_data_size = 0;

    sub connect {

        my ($drh, $dsn, $user, $pass, $attr) = @_;

        my $driver_prefix = 'duckdb_';

        foreach my $var (split /;/, $dsn) {

            my ($attr_name, $attr_value) = split '=', $var, 2;
            return $drh->set_err($DBI::stderr, "Can't parse DSN part '$var'") unless defined $attr_value;

            $attr_name = $driver_prefix . $attr_name unless $attr_name =~ /^$driver_prefix/o;

            $attr->{$attr_name} = $attr_value;
        }

        $attr->{duckdb_dbname}                   //= ':memory:';
        $attr->{duckdb_checkpoint_on_disconnect} //= 1;

        my $dbh = DBI::_new_dbh($drh, {Name => $dsn});

        my ($db, $conn, $rc);

        $rc = duckdb_open($attr->{duckdb_dbname}, \$db);

        return $dbh->set_err(1, "Can't connect to $dsn: duckdb_open failed") if $rc;

        $rc = duckdb_connect($db, \$conn);
        return $dbh->set_err(1, "Can't connect to $dsn: duckdb_connect failed") if $rc;

        $dbh->{duckdb_conn}    = $conn;
        $dbh->{duckdb_db}      = $db;
        $dbh->{duckdb_version} = duckdb_library_version();

        $dbh->STORE('Active', 1);

        return $dbh;

    }

    sub disconnect_all {1}

    sub STORE {
        my ($drh, $attr, $value) = @_;

        if ($attr =~ /^duckdb_/) {
            $drh->{$attr} = $value;
            return 1;
        }

        $drh->SUPER::STORE($attr, $value);
    }

    sub FETCH {
        my ($drh, $attr) = @_;

        if ($attr =~ /^duckdb_/) {
            return $drh->{$attr};
        }

        $drh->SUPER::FETCH($attr);
    }

}

package    # hide from PAUSE
    DBD::DuckDB::db {

    use strict;
    use warnings;
    use DBI  qw(:sql_types);
    use base qw(DBD::_::db);

    use DBD::DuckDB::FFI qw(:all);
    use DBD::DuckDB::Appender;


    our $imp_data_size = 0;

    sub x_duckdb_version { DBD::DuckDB::FFI::duckdb_library_version() }

    sub x_duckdb_appender {

        my ($dbh, $table, $schema) = @_;

        Carp::croak('Usage: $dbh->duckdb_appender($table [, $schema ])') unless $table;
        $schema //= 'main';

        return DBD::DuckDB::Appender->new(schema => $schema, table => $table, dbh => $dbh);

    }

    sub get_info {
        my ($dbh, $info_type) = @_;

        require DBD::DuckDB::GetInfo;
        my $v = $DBD::DuckDB::GetInfo::info{int($info_type)};
        $v = $v->($dbh) if ref $v eq 'CODE';
        return $v;
    }

    sub disconnect {

        my $dbh = shift;

        my $conn = delete $dbh->{duckdb_conn};
        my $db   = delete $dbh->{duckdb_db};

        if ($dbh->FETCH('duckdb_checkpoint_on_disconnect') && $dbh->FETCH('AutoCommit')) {
            my $rc = duckdb_query($conn, 'CHECKPOINT');
            return $dbh->set_err(1, 'failed to save checkpoint') if $rc;
        }

        duckdb_disconnect(\$conn);
        duckdb_close(\$db);

        $dbh->STORE('Active', 0);

        return 1;

    }

    sub prepare {

        my ($dbh, $sql, $attr) = @_;

        my ($outer, $sth) = DBI::_new_sth($dbh, {Statement => $sql});
        return $sth unless ($sth);

        my $rc = duckdb_prepare($dbh->{duckdb_conn}, $sql, \my $stmt);

        if ($rc) {
            $dbh->set_err(1, duckdb_prepare_error($stmt) // 'duckdb_prepare failed');
            return;
        }

        $sth->{duckdb_stmt} = $stmt;
        return $outer;

    }

    sub quote {

        my ($self, $value, $data_type) = @_;

        return "NULL" unless defined $value;

        $value =~ s/'/''/g;
        return "'$value'";

    }

    sub ping {

        my $dbh = shift;

        my $file = $dbh->FETCH('duckdb_dbname');

        return 0 if $file && !-f $file;
        return $dbh->FETCH('Active') ? 1 : 0;

    }

    # SQL/CLI (ISO/IEC JTC 1/SC 32 N 0595), 6.63 Tables
    sub table_info {

        my ($dbh, $catalog, $schema, $table, $type, $attr) = @_;

        my @where = ();
        my @bind  = ();

        my $like = sub {

            my ($col, $val) = @_;

            return if !defined $val || $val eq '' || $val eq '%';

            push @where, ($val =~ /[%_]/) ? "$col LIKE ? ESCAPE '\\'" : "$col = ?";
            push @bind, $val;

        };

        $type = 'BASE TABLE' if (defined $type && uc($type) eq 'TABLE');

        $like->('table_catalog', $catalog) if defined $catalog;
        $like->('table_schema',  $schema)  if defined $schema;
        $like->('table_name',    $table)   if defined $table;
        $like->('table_type',    uc $type) if defined $type;

        my $sql = q{
            SELECT
                table_catalog AS TABLE_CAT,
                table_schema  AS TABLE_SCHEM,
                table_name    AS TABLE_NAME,
                CASE table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS TABLE_TYPE,
                CAST(NULL AS VARCHAR) AS REMARKS
            FROM information_schema.tables
        };

        $sql .= ' WHERE ' . join(' AND ', @where) if @where;
        $sql .= ' ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME';

        my $sth = $dbh->prepare($sql) or return;
        $sth->execute(@bind)          or return;
        return $sth;

    }

    sub primary_key_info {

        my ($dbh, $catalog, $schema, $table) = @_;

        my @where = ();
        my @bind  = ();

        my $like = sub {

            my ($col, $val) = @_;

            return if !defined $val || $val eq '' || $val eq '%';

            push @where, ($val =~ /[%_]/) ? "$col LIKE ? ESCAPE '\\'" : "$col = ?";
            push @bind, $val;

        };

        $like->('kc.table_catalog', $catalog) if defined $catalog;
        $like->('kc.table_schema',  $schema)  if defined $schema;
        $like->('kc.table_name',    $table)   if defined $table;

        my $sql = q{
            SELECT
                kc.table_catalog    AS TABLE_CAT,
                kc.table_schema     AS TABLE_SCHEM,
                kc.table_name       AS TABLE_NAME,
                kc.column_name      AS COLUMN_NAME,
                kc.ordinal_position AS KEY_SEQ,
                tc.constraint_name  AS PK_NAME
            FROM information_schema.table_constraints   AS tc
            JOIN information_schema.key_column_usage    AS kc
                ON  kc.constraint_catalog = tc.constraint_catalog
                AND kc.constraint_schema  = tc.constraint_schema
                AND kc.constraint_name    = tc.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
        };

        $sql .= ' AND ' . join(' AND ', @where) if @where;
        $sql .= ' ORDER BY kc.table_catalog, kc.table_schema, kc.table_name, kc.ordinal_position';

        my $sth = $dbh->prepare($sql) or return;
        $sth->execute(@bind)          or return;
        return $sth;

    }

    sub foreign_key_info {
        my ($dbh, $pk_catalog, $pk_schema, $pk_table, $fk_catalog, $fk_schema, $fk_table) = @_;

    }

    sub rollback {

        my $dbh = shift;

        if ($dbh->FETCH('AutoCommit')) {
            if ($dbh->FETCH('Warn')) {
                warn("Rollback ineffective while AutoCommit is on");
            }
            return;
        }

        $dbh->do('ROLLBACK');
        $dbh->do('BEGIN TRANSACTION');

        return 1;

    }

    sub commit {

        my $dbh = shift;

        if ($dbh->FETCH('AutoCommit')) {
            warn("Commit ineffective while AutoCommit is on") if ($dbh->FETCH('Warn'));
            return;
        }

        $dbh->do('COMMIT');

        return 1;

    }

    sub STORE {
        my ($dbh, $attr, $value) = @_;

        if ($attr =~ /^duckdb_/) {
            $dbh->{$attr} = $value;
            return 1;
        }

        if ($attr eq 'AutoCommit') {

            my $old_value = $dbh->{AutoCommit};
            my $never_set = !$old_value;

            if (!$old_value && $value && $never_set) {

                # DuckDB auto commit
            }
            elsif (!$old_value && $value) {
                $dbh->trace_msg("    -> DuckDB: commit changes\n");
                $dbh->do('COMMIT');
            }
            elsif ($old_value && !$value || !$old_value && !$value && $never_set) {
                $dbh->trace_msg("    -> DuckDB: start transaction\n");
                $dbh->do('BEGIN TRANSACTION');
            }

            $dbh->{AutoCommit} = $value;
            return 1;

        }

        $dbh->SUPER::STORE($attr, $value);
    }

    sub FETCH {
        my ($dbh, $attr) = @_;

        if ($attr =~ /^duckdb_/) {
            return $dbh->{$attr};
        }

        return $dbh->{AutoCommit} if $attr eq 'AutoCommit';
        return $dbh->SUPER::FETCH($attr);
    }

    sub DESTROY {
        my $dbh = shift;
        $dbh->disconnect if $dbh->FETCH('Active');
    }

}


package    # hide from PAUSE
    DBD::DuckDB::st {

    use strict;
    use warnings;
    use DBI  qw(:sql_types);
    use base qw(DBD::_::st);

    use Carp;
    use Config;
    use Time::Piece;

    use FFI::Platypus::Buffer qw( scalar_to_buffer buffer_to_scalar );

    use DBD::DuckDB::FFI       qw(:all);
    use DBD::DuckDB::Constants qw(:all);

    our $imp_data_size = 0;


    sub _sql_type {

        my ($attr, $value) = @_;

        return $attr         if defined $attr        && !ref $attr;
        return $attr->{TYPE} if ref($attr) eq 'HASH' && exists $attr->{TYPE};
        return SQL_INTEGER   if defined $value       && $value =~ /^-?\d+\z/;
        return SQL_DOUBLE    if defined $value       && $value =~ /^-?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?\z/;
        return SQL_BOOLEAN   if defined $value       && $value =~ /^(?:true|false|0|1)\z/i;
        return SQL_VARCHAR;

    }

    sub bind_param {

        my ($sth, $i, $value, $attr) = @_;

        my $sql_type    = _sql_type($attr, $value);
        my $duckdb_stmt = $sth->{duckdb_stmt};

        if (!defined $value) {
            my $rc = duckdb_bind_null($duckdb_stmt, $i);
            return $rc ? $sth->set_err(1, "duckdb_bind_null failed at $i") : 1;
        }

        if ($sql_type == SQL_INTEGER) {
            my $rc = duckdb_bind_int64($duckdb_stmt, $i, int($value));
            return $rc ? $sth->set_err(1, "duckdb_bind_int64 failed at $i") : 1;
        }

        if ($sql_type == SQL_DOUBLE) {
            my $rc = duckdb_bind_double($duckdb_stmt, $i, 0.0 + $value);
            return $rc ? $sth->set_err(1, "duckdb_bind_double failed at $i") : 1;
        }

        if ($sql_type == SQL_BOOLEAN) {
            my $rc = duckdb_bind_bool($duckdb_stmt, $i, ($value ? 1 : 0));
            return $rc ? $sth->set_err(1, "duckdb_bind_bool failed at $i") : 1;
        }

        if ($sql_type == SQL_BLOB) {

            utf8::downgrade($value, 1);

            my ($pointer, $size) = scalar_to_buffer($value);
            my $rc = duckdb_bind_blob($duckdb_stmt, $i, $pointer, $size);

            return $rc ? $sth->set_err(1, "duckdb_bind_blob failed at $i") : 1;

        }

        my $rc = duckdb_bind_varchar($duckdb_stmt, $i, "$value");
        return $rc ? $sth->set_err(1, "duckdb_bind_varchar failed at $i") : 1;

    }

    sub rows { shift->{duckdb_rows} }

    sub execute {

        my ($sth, @bind) = @_;

        my $duckdb_stmt = $sth->{duckdb_stmt};

        for my $i (0 .. $#bind) {
            my $ok = $sth->bind_param($i + 1, $bind[$i]);
            return $ok if !defined $ok;
        }

        my $res = DBD::DuckDB::FFI::Result->new;
        my $rc  = duckdb_execute_prepared($duckdb_stmt, $res);

        return $sth->set_err(1, duckdb_result_error($res) // 'duckdb_execute_prepared failed') if $rc;

        my $res_type = duckdb_result_return_type($res);

        # cache result for fetching
        $sth->{duckdb_res}   = $res;
        $sth->{duckdb_cols}  = duckdb_column_count($res) || 0;
        $sth->{duckdb_rows}  = duckdb_row_count($res)    || 0;
        $sth->{duckdb_index} = 0;

        $sth->STORE('NUM_OF_FIELDS', $sth->{duckdb_cols});

        # fetchrow_hashref
        my @names         = map { duckdb_column_name($res, $_) // "column$_" } (0 .. $sth->{duckdb_cols} - 1);
        my @types         = map { duckdb_column_type($res, $_) } (0 .. $sth->{duckdb_cols} - 1);
        my @logical_types = map { duckdb_column_logical_type($res, $_) } 0 .. $sth->{duckdb_cols} - 1;

        $sth->{NAME}    = \@names;
        $sth->{NAME_lc} = [map lc, @names];
        $sth->{NAME_uc} = [map uc, @names];

        $sth->{duckdb_col_types}         = \@types;
        $sth->{duckdb_col_logical_types} = \@logical_types;

        # DML rows changed
        if ($res_type == DUCKDB_RESULT_TYPE_CHANGED_ROWS) {
            my $name = duckdb_column_name($res, 0) // '';
            if (lc($name) eq 'count') {
                $sth->{duckdb_rows} = duckdb_rows_changed($res);
            }
        }

        return '0E0';

    }

    sub finish {

        my $sth = shift;

        my $res  = delete $sth->{duckdb_res};
        my $stmt = delete $sth->{duckdb_stmt};

        duckdb_destroy_result($res);
        duckdb_destroy_prepare($stmt);

        foreach my $lt (@{$sth->{duckdb_col_logical_types}}) {
            duckdb_destroy_logical_type(\$lt);
        }

        $sth->{duckdb_col_logical_types} = [];

        return 1;

    }

    sub _fetch_vector_value {

        my ($vector, $row_idx, $logical_type) = @_;

        my $validity = duckdb_vector_get_validity($vector);
        return undef unless (duckdb_validity_row_is_valid($validity, $row_idx));

        my $vector_data = duckdb_vector_get_data($vector);
        my $type_id     = duckdb_get_type_id($logical_type);

        return _vector_u8($vector_data, $row_idx) ? \1 : \0 if ($type_id == DUCKDB_TYPE_BOOLEAN);
        return _vector_i8($vector_data, $row_idx)           if ($type_id == DUCKDB_TYPE_TINYINT);
        return _vector_i16($vector_data, $row_idx)          if ($type_id == DUCKDB_TYPE_SMALLINT);
        return _vector_i32($vector_data, $row_idx)          if ($type_id == DUCKDB_TYPE_INTEGER);
        return _vector_i64($vector_data, $row_idx)          if ($type_id == DUCKDB_TYPE_BIGINT);
        return _vector_u8($vector_data, $row_idx)           if ($type_id == DUCKDB_TYPE_UTINYINT);
        return _vector_u16($vector_data, $row_idx)          if ($type_id == DUCKDB_TYPE_USMALLINT);
        return _vector_u32($vector_data, $row_idx)          if ($type_id == DUCKDB_TYPE_UINTEGER);
        return _vector_u64($vector_data, $row_idx)          if ($type_id == DUCKDB_TYPE_UBIGINT);
        return _vector_f32($vector_data, $row_idx)          if ($type_id == DUCKDB_TYPE_FLOAT);
        return _vector_f64($vector_data, $row_idx)          if ($type_id == DUCKDB_TYPE_DOUBLE);

        return _vector_varchar($vector_data, $row_idx)
            if ($type_id == DUCKDB_TYPE_VARCHAR || $type_id == DUCKDB_TYPE_BLOB);

        return _vector_date($vector_data, $row_idx) if ($type_id == DUCKDB_TYPE_DATE);

        return _vector_timestamp($vector_data, $row_idx, $type_id)
            if ($type_id == DUCKDB_TYPE_TIMESTAMP || $type_id == DUCKDB_TYPE_TIMESTAMP_TZ);

        return _vector_array($logical_type, $vector, $row_idx)
            if ($type_id == DUCKDB_TYPE_ARRAY || $type_id == DUCKDB_TYPE_LIST);

        return _vector_struct($logical_type, $vector, $row_idx) if ($type_id == DUCKDB_TYPE_STRUCT);
        return _vector_union($logical_type, $vector, $row_idx)  if ($type_id == DUCKDB_TYPE_UNION);

        # TODO DUCKDB_TYPE_MAP

        Carp::carp "Unknown type ($type_id)";
        return undef;

    }

    sub _mem {
        my ($vector_data, $row_idx, $size) = @_;
        my $addr = $vector_data + $row_idx * $size;
        my $sv   = buffer_to_scalar($addr, $size);
        return $sv;
    }

    sub _vector_u8  { unpack 'C',  _mem(@_, 1) }
    sub _vector_i8  { unpack 'c',  _mem(@_, 1) }
    sub _vector_u16 { unpack 'S<', _mem(@_, 2) }
    sub _vector_i16 { unpack 's<', _mem(@_, 2) }
    sub _vector_u32 { unpack 'L<', _mem(@_, 4) }
    sub _vector_i32 { unpack 'l<', _mem(@_, 4) }
    sub _vector_u64 { unpack 'Q<', _mem(@_, 8) }
    sub _vector_i64 { unpack 'q<', _mem(@_, 8) }
    sub _vector_f32 { unpack 'f<', _mem(@_, 4) }
    sub _vector_f64 { unpack 'd<', _mem(@_, 8) }

    sub _vector_varchar {

        my ($vector_data, $row_idx) = @_;

        my $PTRSIZE = $Config{ptrsize};

        my $rec = buffer_to_scalar($vector_data + $row_idx * 16, 16);
        my $len = unpack('L<', substr($rec, 0, 4));

        return undef unless defined $len;

        if ($len <= 12) {
            return substr($rec, 4, $len);
        }
        else {
            my $ptr = unpack(($PTRSIZE == 8 ? 'Q<' : 'L<'), substr($rec, 8, $PTRSIZE));
            return undef unless $ptr;
            return buffer_to_scalar($ptr, $len);
        }

    }

    sub _vector_date {
        my ($vector_data, $row_idx) = @_;

        my $days  = _vector_i32($vector_data, $row_idx);
        my $epoch = 0 + 86400 * $days;

        my $t = Time::Piece->new($epoch);
        return $t->date;
    }

    sub _vector_timestamp {
        my ($vector_data, $row_idx, $type_id) = @_;
        my $epoch = int(_vector_i64($vector_data, $row_idx) / 1_000_000);
        return ($type_id == DUCKDB_TYPE_TIMESTAMP_TZ) ? localtime($epoch)->datetime : gmtime($epoch)->datetime;
    }

    sub _vector_array {

        my ($logical_type, $vector_data, $row_idx) = @_;

        my $child_logical_type = duckdb_array_type_child_type($logical_type);
        my $size               = duckdb_array_type_array_size($logical_type);
        my $child_vector       = duckdb_array_vector_get_child($vector_data);

        my $begin = $row_idx * $size;
        my $end   = $begin + $size;

        my @out = ();

        for (my $i = $begin; $i < $end; $i++) {
            push @out, _fetch_vector_value($child_vector, $i, $child_logical_type);
        }

        duckdb_destroy_logical_type(\$child_logical_type);

        return \@out;

    }

    sub _vector_struct {

        my ($logical_type, $vector_data, $row_idx) = @_;

        my $child_count = duckdb_struct_type_child_count($logical_type);

        my %struct = ();

        for (my $i = 0; $i < $child_count; ++$i) {

            my $name               = duckdb_struct_type_child_name($logical_type, $i);
            my $child_vector       = duckdb_struct_vector_get_child($vector_data, $i);
            my $child_logical_type = duckdb_struct_type_child_type($logical_type, $i);
            my $value              = _fetch_vector_value($child_vector, $row_idx, $child_logical_type);

            $struct{$name} = $value;

            duckdb_destroy_logical_type(\$child_logical_type);

        }

        return \%struct;

    }

    sub _vector_union {

        my ($logical_type, $vector_data, $row_idx) = @_;

        my $type_vector = duckdb_struct_vector_get_child($vector_data, 0);
        my $data        = duckdb_vector_get_data($type_vector);
        my $index       = int(_vector_u8($data, $row_idx));

        my $child_logical_type = duckdb_union_type_member_type($logical_type, $index);
        my $vector_value       = duckdb_struct_vector_get_child($vector_data, $index + 1);
        my $value              = _fetch_vector_value($vector_value, $row_idx, $child_logical_type);

        duckdb_destroy_logical_type(\$child_logical_type);
        return $value;

    }

    sub fetchrow_arrayref {

        my $sth = shift;

        my $res = $sth->{duckdb_res} or return undef;
        my $idx = $sth->{duckdb_index} // 0;

        if ($idx >= ($sth->{duckdb_rows} // 0)) {
            $sth->STORE(Active => 0);
            return undef;
        }

        my @row = ();

        use Data::Dumper;

        if (!defined $sth->{duckdb_chunk}) {

            $sth->{duckdb_chunk} = duckdb_fetch_chunk($sth->{duckdb_res});

            unless ($sth->{duckdb_chunk}) {
                $sth->STORE('Active', 0);
                return undef;
            }

            $sth->{duckdb_chunk_size} = duckdb_data_chunk_get_size($sth->{duckdb_chunk});
            $sth->{duckdb_chunk_row}  = 0;

            if ($sth->{duckdb_chunk_size} == 0) {
                $sth->STORE('Active', 0);
                return undef;
            }

        }

        for my $col (0 .. $sth->{duckdb_cols} - 1) {
            my $vector       = duckdb_data_chunk_get_vector($sth->{duckdb_chunk}, $col);
            my $logical_type = $sth->{duckdb_col_logical_types}->[$col];
            push @row, _fetch_vector_value($vector, $sth->{duckdb_chunk_row}, $logical_type);
        }

        $sth->{duckdb_chunk_row}++;
        $sth->{duckdb_index}++;


        if ($sth->{duckdb_chunk_row} >= $sth->{duckdb_chunk_size}) {
            my $tmp = $sth->{chunk};
            duckdb_destroy_data_chunk(\$tmp);
            $sth->{chunk} = undef;
        }

        #warn Dumper(\@row);

        map {s/\s+$//} @row if $sth->FETCH('ChopBlanks');
        return $sth->_set_fbav(\@row);

    }

    *fetch = \&fetchrow_arrayref;

    sub STORE {
        my ($sth, $attr, $value) = @_;

        if ($attr =~ /^duckdb_/) {
            $sth->{$attr} = $value;
            return 1;
        }

        $sth->SUPER::STORE($attr, $value);
    }

    sub FETCH {
        my ($sth, $attr) = @_;

        if ($attr =~ /^duckdb_/) {
            return $sth->{$attr};
        }

        $sth->SUPER::FETCH($attr);
    }

}

1;

__END__

=pod

=encoding utf-8

=head1 NAME

DBD::DuckDB - DuckDB DBI driver

=head1 SYNOPSIS

  use DBI;
  my $dbh = DBI->connect("dbi:DuckDB:dbname=$dbfile","","");

=head1 DESCRIPTION

DuckDB is a high-performance analytical database system. It is designed to be 
fast, reliable, portable, and easy to use. DuckDB provides a rich SQL dialect 
with support far beyond basic SQL. DuckDB supports arbitrary and nested 
correlated subqueries, window functions, collations, complex types (arrays, 
structs, maps), and several extensions designed to make SQL easier to use.

L<https://duckdb.org>


=head1 CLASS METHODS

=over

=item DBI->connect

This method creates a database handle by connecting to a database, and is the 
DBI equivalent of the "new" method.

The connection string is always of the form: "dbi:DuckDB:dbname=<dbfile>"

    my $dbh = DBI->connect("dbi:DucDB:dbname=$dbfile", "", "", $attr);

DuckDB creates a file per a database.

The file is opened in read/write mode, and will be created if it does not exist yet.

Although the database is stored in a single file, the directory containing the 
database file must be writable by DuckDB because the library will create 
several temporary files there.

If the filename C<$dbfile> is ":memory:", then a private, temporary in-memory 
database is created for the connection. This in-memory database will vanish 
when the database connection is closed. It is handy for your library tests.

Connect Attributes:

=over

=item * B<duckdb_checkpoint_on_disconnect>

=back

=back

=head1 DATABASE HANDLES

=over

=item $dbh->prepare

    $sth = $dbh->prepare($statement, \%attr);

=item $dbh->do

    $rv = $dbh->do($statement);
    $rv = $dbh->do($statement, \%attr);
    $rv = $dbh->do($statement, \%attr, @bind_values);

Prepare and execute a single statement. Returns the number of rows affected if 
the query was successful, returns undef if an error occurred, and returns -1 if 
the number of rows is unknown or not available. Note that this method will 
return B<0E0> instead of 0 for 'no rows were affected', in order to always 
return a true value if no error occurred.

=item $dbh->last_insert_id

DuckDB does not implement auto_increment of serial type columns it uses 
predefined sequences where the id numbers are either selected before insert, at 
insert time, or as part of the query.

    $dbh->do('CREATE SEQUENCE id_sequence START 1');

    $dbh->do( q{CREATE TABLE tbl (
        id INTEGER DEFAULT nextval('id_sequence'),
        s VARCHAR
    } );

    $dbh->do( q{INSERT INTO tbl (s) VALUES ('hello'), ('world')} );

See L<https://duckdb.org/docs/stable/sql/statements/create_sequence.html>.

=item $dbh->commit

    $rv = $dbh->commit;

=item $dbh->rollback;

    $rv = $dbh->rollback;

=item $dbh->begin_work

This method turns on transactions until the next call to "commit" or "rollback",
if AutoCommit is currently enabled. If it is not enabled, calling begin_work will
issue an error. Note that the transaction will not actually begin until the first
statement after begin_work is called.

Example:

    $dbh->{AutoCommit} = 1;
    $dbh->do('INSERT INTO foo VALUES (123)'); ## Changes committed immediately
    $dbh->begin_work();
    ## Not in a transaction yet, but AutoCommit is set to 0

    $dbh->do("INSERT INTO foo VALUES (345)");
    ## DuckDB actually issues two statements here:
    ## BEGIN;
    ## INSERT INTO foo VALUES (345)
    ## We are now in a transaction

    $dbh->commit();
    ## AutoCommit is now set to 1 again

=item $dbh->disconnect

=item $dbh->table_info

    $sth = $dbh->table_info($catalog, $schema, $table, $type, \%attr);

Returns all tables and schemas (databases) as specified in L<DBI/table_info>.
The schema and table arguments will do a C<LIKE> search. You can specify an
ESCAPE character by including an 'Escape' attribute in \%attr. The C<$type>
argument accepts a comma separated list of the following types 'TABLE',
'INDEX', 'VIEW' and 'TRIGGER' (by default all are returned).
Note that a statement handle is returned, and not a direct list of tables.
The following fields are returned:

=over

=item * B<TABLE_CAT>: The name of the catalog.

=item * B<TABLE_SCHEM>: The name of the schema (database) that the table or view is
in. The default schema is 'main' and other databases will be in the name given when
the database was attached.

B<TABLE_NAME>: The name of the table or view.

B<TABLE_TYPE>: The type of object returned. Will be one of 'TABLE', 'INDEX',
'VIEW', 'TRIGGER'.

=item $dbh->tables

=item $dbh->primary_key, $dbh->primary_key_info

    @names = $dbh->primary_key(undef, $schema, $table);
    $sth   = $dbh->primary_key_info(undef, $schema, $table, \%attr);

You can retrieve primary key names or more detailed information.

=item $dbh->ping

    my $bool = $dbh->ping;

Returns true if the database file exists (or the database is in-memory), and
the database connection is active.


=item $dbh->x_duckdb_version

Return the current DuckDB library version using C<duckdb_library_version> C
function.

=item $dbh->x_duckdb_appender

Appenders are the most efficient way of loading data into DuckDB from within 
the C interface, and are recommended for fast data loading. The appender is 
much faster than using prepared statements or individual INSERT INTO statements.

    $dbh->do('CREATE TABLE people (id INTEGER, name VARCHAR)');
    my $appender = $dbh->x_duckdb_appender('people');

    $appender->append(1, DUCKDB_TYPE_INTEGER);
    $appender->append('Mark', DUCKDB_TYPE_VARCHAR);
    $appender->end_row;

    # or

    $appeder->append_row(id => 1, name => 'Mark');

See L<DBD::DuckDB::Appender>.

=back


=head1 STATEMENT HANDLES

=over

=item $sth->bind_param

=item $sth->execute

=item $sth->rows

=item $sth->finish

=back


=head1 SUPPORT

=head2 Bugs / Feature Requests

Please report any bugs or feature requests through the issue tracker
at L<https://github.com/giterlizzi/perl-DBD-DuckDB/issues>.
You will be notified automatically of any progress on your issue.

=head2 Source Code

This is open source software.  The code repository is available for
public review and contribution under the terms of the license.

L<https://github.com/giterlizzi/perl-DBD-DuckDB>

    git clone https://github.com/giterlizzi/perl-DBD-DuckDB.git


=head1 AUTHOR

=over 4

=item * Giuseppe Di Terlizzi <gdt@cpan.org>

=back


=head1 LICENSE AND COPYRIGHT

This software is copyright (c) 2024-2025 by Giuseppe Di Terlizzi.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
