struct ServerInfo
    server_name::String
    server_major_ver::VarUInt
    server_minor_ver::VarUInt
    server_rev::VarUInt

    # DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
    server_timezone::String
    # DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
    server_display_name::String
    # DBMS_MIN_REVISION_WITH_VERSION_PATCH
    server_version_patch::VarUInt
end

# This is a special case and can't use @ch_struct because we don't  
# know the server revision before reading this packet
function chread(sock::ClickHouseSock, ::Type{ServerInfo})
    server_name = chread(sock, String)
    server_major_ver = chread(sock, VarUInt)
    server_minor_ver = chread(sock, VarUInt)
    server_rev = chread(sock, VarUInt)
    rev = UInt64(server_rev)
    server_timezone = has_server_timezone(rev) ?
                        chread(sock, String) : "UTM"
    server_display_name = has_server_display_name(rev) ?
                        chread(sock, String) : ""
    server_version_patch = has_version_patch(rev) ?
                        chread(sock, VarUInt) : VarUInt(0)
    return ServerInfo(
            server_name,
            server_major_ver,
            server_minor_ver,
            server_rev,
            server_timezone,
            server_display_name,
            server_version_patch
            )
end

function chwrite(sock::ClickHouseSock, info::ServerInfo)
    chwrite(sock, info.server_name)
    chwrite(sock, info.server_major_ver)
    chwrite(sock, info.server_minor_ver)
    chread(sock, info.server_rev)
    rev = UInt64(info.server_rev)
    has_server_timezone(rev) && chwrite(sock, info.server_timezone)
    has_server_display_name(rev) && chwrite(sock, info.server_display_name)
    has_version_patch(rev) && chwrite(sock, info.server_version_patch)
end

@ch_struct struct ServerPong
end

@ch_struct struct ServerProgress
    rows::VarUInt
    bytes::VarUInt
    total_rows::VarUInt

    @has_client_write_info written_rows::VarUInt = VarUInt(0)
    @has_client_write_info written_bytes::VarUInt = VarUInt(0)
end

@ch_struct struct ServerProfileInfo
    rows::VarUInt
    blocks::VarUInt
    bytes::VarUInt
    applied_limit::Bool
    rows_before_limit::VarUInt
    calc_rows_before_limit::Bool
end

@ch_struct struct ServerEndOfStream
end

@ch_struct struct ServerTableColumns
    external_table_name::String
    columns::String
    sample_block::Block
end

@ch_struct struct ServerData
    data::Block
end

@ch_struct struct ServerTotals
    data::Block
end

@ch_struct struct ServerExtremes
    data::Block
end

struct ServerException
    code::UInt32
    name::String
    message::String
    stack_trace::String
    nested::Union{Nothing, ServerException}
end

@ch_struct struct ServerExceptionBase
    code::UInt32
    name::String
    message::String
    stack_trace::String
    has_nested::Bool
end

function chread(sock::ClickHouseSock, ::Type{ServerException})::ServerException
    base = chread(sock, ServerExceptionBase)
    nested = base.has_nested ? chread(sock, ServerException) : nothing
    ServerException(base.code, base.name, base.message, base.stack_trace, nested)
end

function cwrite(sock::ClickHouseSock, x::ServerException)::ServerException
    has_nested = !isnothing(x.nested)
    base = ServerExceptionBase(x.code, x.name, x.message, x.stack_trace, has_nested)
    chwrite(sock, base)
    has_nested && chwrite(sock, x.nested)
end
