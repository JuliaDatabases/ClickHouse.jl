using DataFrames
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
    write_packet(sock.io, make_block())
    nothing
end

function dict2columns(
    dict::Dict{Symbol, T} where T,
    valid_columns::Set{Symbol},
)::Vector{Column}
    @assert begin
        diff = symdiff(dict |> keys |> Set, valid_columns)
        isempty(diff)
    end "Mismatched columns: $(diff)"

    [
        Column(
            string(name),
            COL_TYPE_REV_MAP[typeof(first(column))],
            column,
        )
        for (name, column) ∈ dict
    ]
end

function columns2dict(cols::Vector{Column})::Dict{Symbol, Any}
    Dict(Symbol(x.name) => x.data for x ∈ cols)
end

function make_block(columns::Vector{Column} = Column[])::Block
    num_columns = length(columns)
    num_rows = num_columns == 0 ? 0 : length(columns[1].data)
    @assert all(length(col.data) == num_rows for col ∈ columns)
    Block("", BlockInfo(), num_columns, num_rows, columns)
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
            "",
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
    server_info = read_server_packet(sock.io)::ServerInfo
    sock.tz = server_info.server_timezone

    sock
end

"Send a ping request and wait for the response."
function ping(sock::ClickHouseSock)::Nothing
    write_packet(sock.io, ClientPing())
    read_server_packet(sock.io)::ServerPong
    nothing
end

"Execute a DDL query."
function execute(
    sock::ClickHouseSock,
    ddl_query::AbstractString,
)::Nothing
    write_query(sock, ddl_query)
    read_server_packet(sock.io)::ServerEndOfStream
    nothing
end

"""
Insert blocks into a table, reading from an iterable.
The iterable is expected to yield values of type `Dict{Symbol, Any}`.
"""
function insert(
    sock::ClickHouseSock,
    table::AbstractString,
    iter,
)::Nothing where T
    # TODO: We might want to escape the table name here...
    write_query(sock, "INSERT INTO $(table) VALUES")

    sample_block = read_server_packet(sock.io)::Block
    valid_columns = Set([Symbol(x.name) for x ∈ sample_block.columns])

    for block_dict ∈ iter
        columns = dict2columns(block_dict, valid_columns)
        block = make_block(columns)
        write_packet(sock.io, block)
    end

    # Empty block = end of data.
    write_packet(sock.io, make_block())

    read_server_packet(sock.io)::ServerEndOfStream

    nothing
end

"Execute a query, streaming the resulting blocks through a channel."
function select_channel(
    sock::ClickHouseSock,
    query::AbstractString,
)::Channel{Dict{Symbol, Any}}
    Channel(ctype = Dict{Symbol, Any}) do ch
        write_query(sock, query)

        sample_block = read_server_packet(sock.io)
        sample_block::Block
        @assert UInt64(sample_block.num_rows) == 0

        while true
            packet = read_server_packet(sock.io)

            handle(x::ServerProfileInfo) = true
            handle(x::ServerProgress) = true
            handle(x::ServerEndOfStream) = false

            function handle(block::Block)
                if UInt64(block.num_rows) != 0
                    put!(ch, columns2dict(block.columns))
                end
                true
            end

            if !handle(packet)
                break
            end
        end
    end
end

"Execute a query, flattening blocks into a single dict of column arrays."
function select(
    sock::ClickHouseSock,
    query::AbstractString,
)::Dict{Symbol, Any}
    result = Dict{Symbol, Any}()
    ch = select_channel(sock, query)
    for row ∈ ch, (col_name, col_data) ∈ row
        arr = get(result, col_name, nothing)
        if arr === nothing
            result[col_name] = col_data
        else
            append!(arr, col_data)
        end
    end
    result
end

"Execute a query, flattening blocks into a dataframe."
function select_df(
    sock::ClickHouseSock,
    query::AbstractString
)::DataFrame
    columns = pairs(select(sock, query))
    DataFrame(
        [x for (_, x) ∈ columns],
        [x for (x, _) ∈ columns],
    )
end

# ============================================================================ #