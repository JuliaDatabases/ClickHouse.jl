
macro server_capability(name::Symbol, revision::Int)

    has_func = :(@inline $name(rev) = rev >= $revision)
    rev_func_name = Symbol(name, "_rev")
    rev_func = :(@inline $rev_func_name() = $revision)
    return  quote
                $(esc(has_func))
                $(esc(rev_func))
            end


end

@server_capability has_temporary_tables 50264
@server_capability has_total_rows_in_progress 51554
@server_capability has_block_info 51903
@server_capability has_client_info 54032
@server_capability has_server_timezone 54058
@server_capability has_quota_key 54060
@server_capability has_server_display_name 54372
@server_capability has_version_patch 54401
@server_capability has_server_logs 54406
@server_capability has_column_dafaults_metadata 54410
@server_capability has_client_write_info 54420

const CLIENT_NAME = "ClickHouseJL"
const DBMS_VER_MAJOR = 19
const DBMS_VER_MINOR = 11
const DBMS_VER_REV = 54423
const DBMS_DEFAULT_BUFFER_SIZE = 1048576
const DBMS_DEFAULT_CONNECT_TIMEOUT = 5
const DBMS_DEFAULT_MAX_INSERT_BLOCK = 100000