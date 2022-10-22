using DataFrames
using ProgressMeter
import Sockets


# ============================================================================ #
# [Helpers]                                                                    #
# ============================================================================ #

function write_query(sock::ClickHouseSock, query::AbstractString)::Nothing
    compression = compression_enabled(sock.settings)
    query = ClientQuery("", ClientInfo(), "", 2, compression, query)
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


"Send a ping request and wait for the response."
function ping(sock::ClickHouseSock)::Nothing
    @using_socket sock begin
        write_packet(sock, ClientPing())
        read_server_packet(sock)::ServerPong
    end
    nothing
end

"Execute a DDL query."
function execute(
    sock::ClickHouseSock,
    ddl_query::AbstractString,
)::Nothing
    @using_socket sock begin
        write_query(sock, ddl_query)
        while true
            packet = read_server_packet(sock)
            if packet isa ServerProgress
                # we just ignore these for DDL queries for now.
            elseif packet isa ServerEndOfStream
                break
            else
                error("Unexpected packet received: $(packet)")
            end
        end
    end
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
    @using_socket sock begin
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
    end
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
    @using_socket sock begin
        write_query(sock, query)

        sample_data::ServerData = read_server_packet(sock)
        sample_block = sample_data.data
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

            handle(p::Union{ServerData, ServerTotals, ServerExtremes}) =
                                        handle(p.data)

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
    end
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