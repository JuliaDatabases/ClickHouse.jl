using Sockets

# ============================================================================ #
# [Structs & constants]                                                        #
# ============================================================================ #

const CLIENT_NAME = "ClickHouseJL"
const DBMS_VER_MAJOR = 19
const DBMS_VER_MINOR = 11
const DBMS_VER_REV = 54423

"ClickHouse client socket. Created using `connect`."
mutable struct ClickHouseSock
    io::Sockets.TCPSocket
    tz::Union{String, Nothing}
    client_info::ClientInfo
end

# ============================================================================ #
# [Helpers]                                                                    #
# ============================================================================ #

function write_query(sock::ClickHouseSock, query::AbstractString)::Nothing
    query = ClientQuery("", sock.client_info, "", 2, 0, query)
    write_packet(sock.io, query)
    nothing
end

function make_block(columns::Array{Column})::Block
    num_columns = length(columns)
    num_rows = num_columns == 0 ? 0 : length(columns[1].data)
    @assert all(length(col.data) == num_rows for col ∈ columns)
    Block("", BlockInfo(), num_columns, num_rows, columns)
end

function columns2dict(cols::Array{Column})::Dict{String, Any}
    Dict(x.name => x.data for x ∈ cols)
end

# ============================================================================ #
# [Queries]                                                                    #
# ============================================================================ #

"Establish a connection to a given ClickHouse instance."
function connect(
    host::AbstractString = "localhost",
    port::Integer = 9000;
    database::AbstractString = "",
    username::AbstractString = "default",
    password::AbstractString = "",
)::ClickHouseSock
    sock = ClickHouseSock(
        Sockets.connect(host, port),
        nothing,
        ClientInfo(
            0x01,
            "",
            "",
            "0.0.0.0:0",
            0x01,
            "",
            "6b88a142c2bc",
            CLIENT_NAME,
            DBMS_VER_MAJOR,
            DBMS_VER_MINOR,
            DBMS_VER_REV,
            "",
            2,
        ),
    )

    # Say hello to the server!
    write_packet(sock.io, ClickHouse.ClientHello(
        CLIENT_NAME,
        DBMS_VER_MAJOR,
        DBMS_VER_MINOR,
        DBMS_VER_REV,
        database,
        username,
        password,
    ))

    # Read server info.
    server_info = read_server_packet(sock.io)
    server_info::ServerInfo
    sock.tz = server_info.server_timezone

    sock
end

"Send a ping request and wait for the response."
function ping(sock::ClickHouseSock)::Nothing
    write_packet(sock.io, ClientPing())
    pong = read_server_packet(sock.io)
    pong::ServerPong
    nothing
end

"Insert a single block into a table."
function insert(
    sock::ClickHouseSock,
    table::AbstractString,
    columns::Dict{String, Array{Any, 1}} = Dict(),
)::Nothing
    # TODO: We might want to escape the table name here...
    write_query(sock, "INSERT INTO $(table) VALUES")

    write_packet(sock.io, make_block(Column[]))

    if length(columns) > 0 && length(first(columns)) > 0
        write_packet(sock.io, make_block([
            Column(
                name,
                COL_TY_REV_MAP[typeof(first(column))],
                column,
            )
            for (name, column) ∈ columns
        ]))
    end

    write_packet(sock.io, make_block(Column[]))

    resp = read_server_packet(sock.io)
    resp::Block

    resp = read_server_packet(sock.io)
    resp::ServerEndOfStream

    nothing
end

"Execute a query, streaming the resulting blocks into a channel."
function select_into_channel(
    sock::ClickHouseSock,
    query::AbstractString,
    ch::Channel{Dict{String, Array{Any}}},
)::Nothing
    write_query(sock, query)
    write_packet(sock.io, make_block(Column[]))

    start_block = read_server_packet(sock.io)
    start_block::Block
    @assert UInt64(start_block.num_rows) == 0

    while true
        packet = read_server_packet(sock.io)

        handle(x::ServerProfileInfo) = true
        handle(x::ServerProgress) = true

        function handle(block::Block)
            if UInt64(block.num_rows) != 0
                put!(ch, columns2dict(block.columns))
                true
            else
                false
            end
        end

        if !handle(packet)
            break
        end
    end

    end_of_stream = read_server_packet(sock.io)
    end_of_stream::ServerEndOfStream

    nothing
end

# ============================================================================ #