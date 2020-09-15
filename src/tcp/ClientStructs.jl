@ch_struct struct ClientInfo
    query_kind::UInt8
    initial_user::String
    initial_query_id::String
    initial_address_string::String
    read_interface::UInt8
    os_user::String
    client_hostname::String
    client_name::String
    client_ver_major::VarUInt
    client_ver_minor::VarUInt
    client_rev::VarUInt
    @has_quota_key quota_key::String = ""
    @has_version_patch client_ver_patch::VarUInt = VarUInt(0)
end
ClientInfo() = ClientInfo(
    0x01,
    "",
    "",
    "0.0.0.0:0",
    0x01,
    "",
    "",
    CLIENT_NAME,
    DBMS_VER_MAJOR,
    DBMS_VER_MINOR,
    DBMS_VER_REV,
    "",
    2,
)

@ch_struct struct ClientHello
    client_name::String
    client_dbms_ver_major::VarUInt
    client_dbms_ver_minor::VarUInt
    client_dbms_ver_rev::VarUInt
    database::String
    username::String
    password::String
end

@ch_struct struct ClientPing
end


@ch_struct struct ClientQuery
    query_id::String
    @has_client_info client_info::ClientInfo = ClientInfo()
    settings::String
    query_stage::VarUInt
    compression::VarUInt
    query::String
end