Base.@kwdef struct CHSettings
    host::String = "localhost"
    port::Int = 9000
    database::String = ""
    username::String = "default"
    password::String = ""
    connection_timeout::Int = DBMS_DEFAULT_CONNECT_TIMEOUT
    max_insert_block_size::Int = DBMS_DEFAULT_MAX_INSERT_BLOCK
    send_buffer_size::Int = DBMS_DEFAULT_BUFFER_SIZE
end


mutable struct ClickHouseSock
    io ::Union{IO, Nothing}
    settings ::CHSettings
    cond ::ReentrantLock
    busy ::Bool
    server_name ::String
    server_rev ::Int
    server_timezone ::Union{String, Nothing}

    function ClickHouseSock(io, settings = CHSettings())
        return new(
            io,
            settings,
            ReentrantLock(),
            false,
            "", 0, nothing
        )
    end
end

function guarded(f, sock::ClickHouseSock)
    lock(sock.cond)
    try
        res = f(sock)
        unlock(sock.cond)
        return res
    catch e
        unlock(sock.cond)
        rethrow(e)
    end
end

macro guarded(sock, expr)
    quote
        lock($(esc(sock)).cond)
        local res = try
            $(esc(expr))
        catch e
            unlock($(esc(sock)).cond)
            rethrow(e)
        end
        unlock($(esc(sock)).cond)
        res
    end
end

is_connected(sock::ClickHouseSock) = @guarded sock !isnothing(sock.io)
is_busy(sock::ClickHouseSock) = @guarded sock sock.busy

set_busy!(sock::ClickHouseSock, value::Bool) =
                        @guarded sock sock.busy = value

function using_socket(f, sock::ClickHouseSock)
    @guarded sock begin
        isnothing(sock.io) && error("ClickHouseSock not connected")
        sock.busy && error("ClickHouseSock is busy")
        sock.busy = true
    end
    try
        res = f(sock)
        set_busy!(sock, false)
        return res
    catch e
        typeof(e) == ClickHouseServerException ?
                set_busy!(sock, false) :
                close(sock)

        rethrow(e)
    end
end

macro using_socket(sock, expr)
    quote
        @guarded $(esc(sock)) begin
            isnothing($(esc(sock)).io) && error("ClickHouseSock not connected")
            $(esc(sock)).busy && error("ClickHouseSock is busy")
            $(esc(sock)).busy = true
        end
        local res = try
            $(esc(expr))
        catch e
            typeof(e) == ClickHouseServerException ?
                    set_busy!($(esc(sock)), false) :
                    close($(esc(sock)))
            rethrow(e)
        end
        set_busy!($(esc(sock)), false)
        res
    end
end