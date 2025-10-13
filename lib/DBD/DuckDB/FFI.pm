package DBD::DuckDB::FFI;

use strict;
use warnings;
use v5.10;

use FFI::Platypus 2.00;
use FFI::CheckLib qw(find_lib_or_die);

use Exporter 'import';

our @EXPORT_OK = qw(
    duckdb_appender_begin_row
    duckdb_appender_close
    duckdb_appender_create
    duckdb_appender_destroy
    duckdb_appender_destroy
    duckdb_appender_end_row
    duckdb_appender_flush
    duckdb_array_type_array_size
    duckdb_array_type_child_type
    duckdb_array_vector_get_child
    duckdb_bind_blob
    duckdb_bind_boolean
    duckdb_bind_double
    duckdb_bind_int32
    duckdb_bind_int64
    duckdb_bind_null
    duckdb_bind_varchar
    duckdb_close
    duckdb_column_count
    duckdb_column_logical_type
    duckdb_column_name
    duckdb_column_type
    duckdb_connect
    duckdb_data_chunk_get_size
    duckdb_data_chunk_get_vector
    duckdb_destroy_data_chunk
    duckdb_destroy_logical_type
    duckdb_destroy_prepare
    duckdb_destroy_result
    duckdb_disconnect
    duckdb_execute_prepared
    duckdb_fetch_chunk
    duckdb_free
    duckdb_get_type_id
    duckdb_library_version
    duckdb_list_vector_get_child
    duckdb_open
    duckdb_prepare
    duckdb_prepare_error
    duckdb_query
    duckdb_result_error
    duckdb_result_return_type
    duckdb_row_count
    duckdb_rows_changed
    duckdb_struct_type_child_count
    duckdb_struct_type_child_name
    duckdb_struct_type_child_type
    duckdb_struct_vector_get_child
    duckdb_union_type_member_count
    duckdb_union_type_member_type
    duckdb_validity_row_is_valid
    duckdb_vector_get_column_type
    duckdb_vector_get_data
    duckdb_vector_get_validity
);

our %EXPORT_TAGS = (all => [@EXPORT_OK]);

state $ffi = FFI::Platypus->new(api => 2, lib => find_lib_or_die(lib => 'duckdb', alien => 'Alien::DuckDB'));

sub init {

    $ffi->type(int      => 'duckdb_result_type');
    $ffi->type(int      => 'duckdb_state');
    $ffi->type(int      => 'duckdb_type');
    $ffi->type(int      => 'idx_t');
    $ffi->type(opaque   => 'duckdb_appender');
    $ffi->type(opaque   => 'duckdb_connection');
    $ffi->type(opaque   => 'duckdb_data_chunk');
    $ffi->type(opaque   => 'duckdb_database');
    $ffi->type(opaque   => 'duckdb_logical_type');
    $ffi->type(opaque   => 'duckdb_prepared_statement');
    $ffi->type(opaque   => 'duckdb_vector');
    $ffi->type(uint64_t => 'validity');

    $ffi->type('record(DBD::DuckDB::FFI::Result)' => 'duckdb_result');

    # Open Connect
    $ffi->attach(duckdb_connect         => ['duckdb_database', 'duckdb_connection*'] => 'duckdb_state');
    $ffi->attach(duckdb_open            => ['string', 'duckdb_database*']            => 'duckdb_state');
    $ffi->attach(duckdb_close           => ['duckdb_database*']                      => 'void');
    $ffi->attach(duckdb_disconnect      => ['duckdb_connection*']                    => 'void');
    $ffi->attach(duckdb_library_version => []                                        => 'string');

    # Query Execution
    $ffi->attach(duckdb_query               => ['duckdb_connection', 'string', 'duckdb_result*'] => 'duckdb_state');
    $ffi->attach(duckdb_destroy_result      => ['duckdb_result*']                                => 'void');
    $ffi->attach(duckdb_result_error        => ['duckdb_result*']                                => 'string');
    $ffi->attach(duckdb_row_count           => ['duckdb_result*']                                => 'idx_t');
    $ffi->attach(duckdb_rows_changed        => ['duckdb_result*']                                => 'idx_t');
    $ffi->attach(duckdb_column_count        => ['duckdb_result*']                                => 'idx_t');
    $ffi->attach(duckdb_column_name         => ['duckdb_result*', 'idx_t']                       => 'string');
    $ffi->attach(duckdb_column_type         => ['duckdb_result*', 'idx_t']                       => 'duckdb_type');
    $ffi->attach(duckdb_column_logical_type => ['duckdb_result*', 'idx_t'] => 'duckdb_logical_type');

    # Prepared Statements
    $ffi->attach(duckdb_prepare => ['duckdb_connection', 'string', 'duckdb_prepared_statement*'] => 'duckdb_state');
    $ffi->attach(duckdb_prepare_error   => ['duckdb_prepared_statement']                         => 'string');
    $ffi->attach(duckdb_destroy_prepare => ['duckdb_prepared_statement*']                        => 'void');

    # Bind Values to Prepared Statements
    $ffi->attach(duckdb_bind_blob    => ['duckdb_prepared_statement', 'idx_t', 'opaque', 'idx_t'] => 'duckdb_state');
    $ffi->attach(duckdb_bind_boolean => ['duckdb_prepared_statement', 'idx_t', 'uint8']           => 'duckdb_state');
    $ffi->attach(duckdb_bind_double  => ['duckdb_prepared_statement', 'idx_t', 'double']          => 'duckdb_state');
    $ffi->attach(duckdb_bind_int32   => ['duckdb_prepared_statement', 'idx_t', 'sint32']          => 'duckdb_state');
    $ffi->attach(duckdb_bind_int64   => ['duckdb_prepared_statement', 'idx_t', 'sint64']          => 'duckdb_state');
    $ffi->attach(duckdb_bind_null    => ['duckdb_prepared_statement', 'idx_t']                    => 'duckdb_state');
    $ffi->attach(duckdb_bind_varchar => ['duckdb_prepared_statement', 'idx_t', 'string']          => 'duckdb_state');

    # Execute Prepared Statements
    $ffi->attach('duckdb_execute_prepared' => ['duckdb_prepared_statement', 'duckdb_result*'] => 'duckdb_state');

    # Result Functions
    $ffi->attach(duckdb_fetch_chunk        => ['duckdb_result'] => 'duckdb_data_chunk');
    $ffi->attach(duckdb_result_return_type => ['duckdb_result'] => 'duckdb_result_type');

    # Helpers
    $ffi->attach(duckdb_free => ['opaque'] => 'void');

    # Validity Mask Functions
    $ffi->attach(duckdb_validity_row_is_valid => ['validity', 'idx_t'] => 'bool');

    # Logical Type Interface
    $ffi->attach(duckdb_struct_type_child_type  => ['duckdb_logical_type', 'idx_t'] => 'duckdb_logical_type');
    $ffi->attach(duckdb_destroy_logical_type    => ['duckdb_logical_type*']         => 'void');
    $ffi->attach(duckdb_union_type_member_count => ['duckdb_logical_type']          => 'idx_t');
    $ffi->attach(duckdb_union_type_member_type  => ['duckdb_logical_type', 'idx_t'] => 'duckdb_logical_type');
    $ffi->attach(duckdb_struct_type_child_name  => ['duckdb_logical_type', 'idx_t'] => 'string');
    $ffi->attach(duckdb_struct_type_child_count => ['duckdb_logical_type']          => 'idx_t');
    $ffi->attach(duckdb_array_type_array_size   => ['duckdb_logical_type']          => 'idx_t');
    $ffi->attach(duckdb_array_type_child_type   => ['duckdb_logical_type']          => 'duckdb_logical_type');
    $ffi->attach(duckdb_get_type_id             => ['duckdb_logical_type']          => 'duckdb_type');

    # Data Chunk Interface
    $ffi->attach(duckdb_destroy_data_chunk    => ['duckdb_data_chunk*']         => 'void');
    $ffi->attach(duckdb_data_chunk_get_vector => ['duckdb_data_chunk', 'idx_t'] => 'opaque');
    $ffi->attach(duckdb_data_chunk_get_size   => ['duckdb_data_chunk']          => 'idx_t');


    # Vector Interface
    $ffi->attach(duckdb_vector_get_column_type  => ['duckdb_vector'] => 'duckdb_logical_type');
    $ffi->attach(duckdb_struct_vector_get_child => ['duckdb_vector'] => 'duckdb_vector');
    $ffi->attach(duckdb_array_vector_get_child  => ['duckdb_vector'] => 'duckdb_vector');
    $ffi->attach(duckdb_list_vector_get_child   => ['duckdb_vector'] => 'duckdb_vector');
    $ffi->attach(duckdb_vector_get_validity     => ['duckdb_vector'] => 'uint64_t');
    $ffi->attach(duckdb_vector_get_data         => ['duckdb_vector'] => 'opaque');

    # Appender
    $ffi->attach(duckdb_appender_create    => [qw(duckdb_connection string string duckdb_appender*)] => 'duckdb_state');
    $ffi->attach(duckdb_appender_begin_row => ['duckdb_appender']                                    => 'duckdb_state');
    $ffi->attach(duckdb_appender_close     => ['duckdb_appender']                                    => 'duckdb_state');
    $ffi->attach(duckdb_appender_destroy   => ['duckdb_appender*']                                   => 'duckdb_state');
    $ffi->attach(duckdb_appender_end_row   => ['duckdb_appender']                                    => 'duckdb_state');
    $ffi->attach(duckdb_appender_flush     => ['duckdb_appender']                                    => 'duckdb_state');
    $ffi->attach(duckdb_append_null        => ['duckdb_appender']                                    => 'duckdb_state');
    $ffi->attach(duckdb_append_bool        => ['duckdb_appender', 'bool']                            => 'duckdb_state');

    return 1;

}


package    # hide from PAUSE
    DBD::DuckDB::FFI::Result {

    use strict;
    use warnings;
    use FFI::Platypus::Record qw(record_layout_1);

    # C struct (ABI v1.4):
    # //! A query result consists of a pointer to its internal data.
    # //! Must be freed with 'duckdb_destroy_result'.
    # typedef struct {
    #     // Deprecated, use `duckdb_column_count`.
    #     idx_t deprecated_column_count;
    #     // Deprecated, use `duckdb_row_count`.
    #     idx_t deprecated_row_count;
    #     // Deprecated, use `duckdb_rows_changed`.
    #     idx_t deprecated_rows_changed;
    #     // Deprecated, use `duckdb_column_*`-family of functions.
    #     duckdb_column *deprecated_columns;
    #     // Deprecated, use `duckdb_result_error`.
    #     char *deprecated_error_message;
    #     void *internal_data;
    # } duckdb_result;
    record_layout_1(
        size_t => 'deprecated_column_count',
        size_t => 'deprecated_row_count',
        size_t => 'deprecated_rows_changed',
        opaque => 'deprecated_columns',
        opaque => 'deprecated_error_message',
        opaque => 'internal_data',
    );

}


1;
