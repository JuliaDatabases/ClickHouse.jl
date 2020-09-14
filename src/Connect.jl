Base.flush(sock::ClickHouseSock) = Base.flush(sock.io)

function Base.close(sock::ClickHouseSock)
    @guarded sock begin
        !isnothing(sock.io) && Base.close(sock.io)
        sock.io = nothing
        sock.busy = false
    end
end

function connect!(sock::ClickHouseSock; force = false)
    force && is_connected(sock) && close(sock)
    is_connected(sock) && return sock

    try
        set_busy!(sock, true)
        tcp = Sockets.TCPSocket()
        Base.buffer_writes(tcp, sock.settings.send_buffer_size)
        Sockets.connect!(tcp, sock.settings.host, sock.settings.port)

        timeout = Ref{Bool}(false)
        @async begin
            sleep(sock.settings.connection_timeout)
            if tcp.status == Sockets.StatusConnecting
                timeout[] = true
                tcp.status = Base.StatusClosing
                ccall(:jl_forceclose_uv, Nothing, (Ptr{Nothing},), tcp.handle)
            end
        end
        try
            Sockets.wait_connected(tcp)
        catch e
            if timeout[]
                error("Connection timeout")
            end
            rethrow(e)
        end

        @guarded sock sock.io = tcp

        write_packet(sock, ClientHello(
            CLIENT_NAME,
            DBMS_VER_MAJOR,
            DBMS_VER_MINOR,
            DBMS_VER_REV,
            @guarded(sock, sock.settings.database),
            @guarded(sock, sock.settings.username),
            @guarded(sock, sock.settings.password)
        ))
        info = read_packet(sock, ServerCodes)::ServerInfo
        @guarded sock begin
            sock.server_name = isempty(info.server_display_name) ?
                            info.server_name : info.server_display_name
            sock.server_rev = UInt64(info.server_rev)
            sock.server_timezone = info.server_timezone
            sock.busy = false
        end
    catch e
        close(sock)
        rethrow(e)
    end
    return sock
end

function connect(
    host::AbstractString = "localhost",
    port::Integer = 9000;
    database::AbstractString = "",
    username::AbstractString = "default",
    password::AbstractString = "",
    connection_timeout = DBMS_DEFAULT_CONNECT_TIMEOUT,
    max_insert_block_size = DBMS_DEFAULT_MAX_INSERT_BLOCK,
    send_buffer_size = DBMS_DEFAULT_BUFFER_SIZE
)::ClickHouseSock
    sock = ClickHouseSock(
        nothing,
        CHSettings(
            host = host,
            port = port,
            database = database,
            username = username,
            password = password,
            connection_timeout = connection_timeout,
            max_insert_block_size = max_insert_block_size,
            send_buffer_size = send_buffer_size
        )
    )

    return connect!(sock)
end
