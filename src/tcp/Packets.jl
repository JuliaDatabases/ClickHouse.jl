@enum ClientCodes::UInt64 begin
    CLIENT_HELLO = 0
    CLIENT_QUERY = 1
    CLIENT_DATA = 2
    CLIENT_CANCEL = 3
    CLIENT_PING = 4
    CLIENT_TABLE_STATUS_REQ = 5
    CLIENT_KEEP_ALIVE = 6
end

@enum ServerCodes::UInt64 begin
    SERVER_HELLO = 0
    SERVER_DATA = 1
    SERVER_EXCEPTION = 2
    SERVER_PROGRESS = 3
    SERVER_PONG = 4
    SERVER_END_OF_STREAM = 5
    SERVER_PROFILE_INFO = 6
    SERVER_TOTALS = 7
    SERVER_EXTREMES = 8
    SERVER_TABLES_STATUS_REPORT = 9
    SERVER_TABLES_LOG = 10
    SERVER_TABLE_COLUMNS = 11
end




@reg_packet CLIENT_HELLO ClientHello
@reg_packet CLIENT_QUERY ClientQuery
@reg_packet CLIENT_DATA Block
@reg_packet CLIENT_PING ClientPing
#TODO CANCEL(3), TABLE_STATUS_REQ(5), KEEP_ALIVE(6)

@reg_packet SERVER_HELLO ServerInfo
@reg_packet SERVER_DATA ServerData
@reg_packet SERVER_EXCEPTION ServerException
@reg_packet SERVER_PROGRESS ServerProgress
@reg_packet SERVER_PONG ServerPong
@reg_packet SERVER_END_OF_STREAM ServerEndOfStream
@reg_packet SERVER_PROFILE_INFO ServerProfileInfo
@reg_packet SERVER_TOTALS ServerTotals
@reg_packet SERVER_EXTREMES ServerExtremes
@reg_packet SERVER_TABLE_COLUMNS ServerTableColumns

#TODO SERVER_TABLES_STATUS_REPORT(9), SERVER_TABLES_LOG(10), SERVER_TABLE_COLUMNS(11)

function read_packet(sock::ClickHouseSock, ::Type{CodeT}) where {CodeT}
    opcode = CodeT(UInt64(chread(sock, VarUInt)))
    struct_type = packet_struct(Val(opcode))
    return chread(sock, struct_type)
end

read_client_packet(sock::ClickHouseSock) = read_packet(sock, ClientCodes)

function read_server_packet(sock::ClickHouseSock)
    packet = read_packet(sock, ServerCodes)

    if typeof(packet) == ServerException
        throw(ClickHouseServerException(packet.code, packet.name, packet.message))
    end

    packet
end
function write_packet(sock::ClickHouseSock, packet::T) where {T}

    chwrite(sock, VarUInt(packet_code(T)))
    res = chwrite(sock, packet)
    flush(sock)
    return res
end