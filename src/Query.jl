using DataFrames
using ProgressMeter
using Sockets


# ============================================================================ #
# [Helpers]                                                                    #
# ============================================================================ #

function write_query(sock::ClickHouseSock, query::AbstractString)::Nothing
    query = ClientQuery("", sock.client_info, "", 2, 0, query)
    write_packet(sock, query)
    write_packet(sock, make_block())
    nothing
end

function dict2columns(
    dict::Dict{Symbol, T} where T,
    valid_columns::Dict{Symbol, String},
)::Vector{Column}
    @assert begin
        diff = symdiff(dict |> keys |> Set, valid_columns |> keys |> Set)
        isempty(diff)
    end "Mismatched columns: $(diff)"

    # TODO: Check if column types match.

    [
        Column(string(name), valid_columns[name], column)
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

"""
Check dirty state & mark as dirty.

This is intententionally not a `guard(x) do y .. end` function because the
closure would result in all captured variables to be boxed, resulting in
runtime overhead. It is also intentional that the dirty flag isn't cleared
in case of exceptions between `enter_dirty` and `exit_dirty`.
"""
function enter_dirty(sock::ClickHouseSock)::Nothing
    !sock.dirty || error(
        "The socket is dirty. This means that another query is either " *
        "currently in progress or an error left the socket in a dirty state. " *
        "Please create a new connection."
    )

    sock.dirty = true
    nothing
end

function exit_dirty(sock::ClickHouseSock)::Nothing
    @assert sock.dirty
    sock.dirty = false
    nothing
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
    sock = ClickHouseSock(Sockets.connect(host, port))

    # Say hello to the server!
    write_packet(sock, ClickHouse.ClientHello(
        CLIENT_NAME,
        DBMS_VER_MAJOR,
        DBMS_VER_MINOR,
        DBMS_VER_REV,
        database,
        username,
        password,
    ))

    # Read server info.
    server_info = read_server_packet(sock)::ServerInfo
    sock.server_tz = server_info.server_timezone

    sock
end

"Send a ping request and wait for the response."
function ping(sock::ClickHouseSock)::Nothing
    enter_dirty(sock)
    write_packet(sock, ClientPing())
    read_server_packet(sock)::ServerPong
    exit_dirty(sock)
    nothing
end

"Execute a DDL query."
function execute(
    sock::ClickHouseSock,
    ddl_query::AbstractString,
)::Nothing
    enter_dirty(sock)
    write_query(sock, ddl_query)
    read_server_packet(sock)::ServerEndOfStream
    exit_dirty(sock)
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
    enter_dirty(sock)

    # TODO: We might want to escape the table name here...
    write_query(sock, "INSERT INTO $(table) VALUES")

    packet = read_server_packet(sock)
    sample_block = if packet isa ServerTableColumns
        packet.sample_block
    else
        packet::Block
    end

    valid_columns = Dict(
        Symbol(x.name) => x.type
        for x ∈ sample_block.columns
    )

    for block_dict ∈ iter
        columns = dict2columns(block_dict, valid_columns)
        block = make_block(columns)
        write_packet(sock, block)
    end

    # Empty block = end of data.
    write_packet(sock, make_block())

    read_server_packet(sock)::ServerEndOfStream

    exit_dirty(sock)
    nothing
end

"Execute a query, invoking a callback for each block."
function select_callback(
    callback::Function,
    sock::ClickHouseSock,
    query::AbstractString;
    show_progress::Bool = false,
    progress_kwargs::Dict = Dict(),
)::Nothing
    enter_dirty(sock)
    write_query(sock, query)

    sample_block = read_server_packet(sock)
    sample_block::Block
    @assert UInt64(sample_block.num_rows) == 0

    progress_bar = nothing
    progress_rows::Int64 = 0
    while true
        packet = read_server_packet(sock)

        handle(x::ServerProfileInfo) = true
        handle(x::ServerEndOfStream) = false

        function handle(x::ServerProgress)
            if show_progress
                if progress_bar === nothing
                    progress_bar = Progress(
                        x.total_rows |> UInt64 |> Int64;
                        progress_kwargs...
                    )
                end
                rows = x.rows |> UInt64 |> Int64
                progress_rows += rows
                update!(progress_bar, progress_rows)
            end

            true
        end

        function handle(block::Block)
            if UInt64(block.num_rows) != 0
                block.columns |> columns2dict |> callback
            end
            true
        end

        if !handle(packet)
            break
        end
    end

    exit_dirty(sock)
end

"Execute a query, streaming the resulting blocks through a channel."
function select_channel(
    sock::ClickHouseSock,
    query::AbstractString;
    csize = 0,
    kwargs...,
)::Channel{Dict{Symbol, Any}}
    Channel(ctype = Dict{Symbol, Any}, csize = csize) do ch
        select_callback(sock, query; kwargs...) do row
            put!(ch, row)
        end
    end
end

"Execute a query, flattening blocks into a single dict of column arrays."
function select(
    sock::ClickHouseSock,
    query::AbstractString;
    kwargs...
)::Dict{Symbol, Any}
    result = Dict{Symbol, Any}()
    select_callback(sock, query; kwargs...) do block
        for (col_name, col_data) ∈ block
            arr = get(result, col_name, nothing)
            if arr === nothing
                result[col_name] = col_data
            else
                append!(arr, col_data)
            end
        end
    end
    result
end

"Execute a query, flattening blocks into a dataframe."
function select_df(
    sock::ClickHouseSock,
    query::AbstractString;
    kwargs...
)::DataFrame
    select(sock, query; kwargs...) |> pairs |> DataFrame
end

# ============================================================================ #