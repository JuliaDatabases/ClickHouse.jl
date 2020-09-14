using ClickHouse: ClickHouseSock, CHSettings, using_socket, is_connected,
                is_busy, chwrite, chread, has_temporary_tables, ClientInfo,
                VarUInt, write_packet, read_packet, @using_socket, ClientHello


@testset "guarded" begin
    sock = ClickHouseSock(PipeBuffer())
    ClickHouse.@guarded sock sock.busy = true
    @test sock.busy == true
    @test ClickHouse.@guarded(sock, sock.busy) == true
    name = "test test"
    ClickHouse.@guarded sock begin
        sock.server_name = name
    end
    @test sock.server_name == "test test"
end
@testset "busy" begin
    sock = ClickHouseSock(nothing)

    try
        using_socket(sock) do s
            sleep(1)
        end
        @test false
    catch e
        @test e.msg == "ClickHouseSock not connected"
    end

    sock.io = PipeBuffer()

    a = @async @using_socket sock begin
        sleep(1)
    end
    sleep(0.2)
    @test is_busy(sock)
    try
        @using_socket sock begin
            sleep(1)
        end
        @test false
    catch e
        @test e.msg == "ClickHouseSock is busy"
    end
    wait(a)
    @test !is_busy(sock)
end

@testset "ch structs" begin
    c_info = ClientInfo(
        0,
        "user",
        "aaa-aaa",
        ":0",
        1,
        "osu",
        "host",
        "name",
        10,
        2,
        23331,
        "quota",
        10
    )

    sock = ClickHouseSock(PipeBuffer())
    chwrite(sock, c_info)
    res = chread(sock, ClientInfo)

    @test res == ClientInfo(
        0,
        "user",
        "aaa-aaa",
        ":0",
        1,
        "osu",
        "host",
        "name",
        10,
        2,
        23331,
        "",
        0
    )

    sock.server_rev = ClickHouse.has_quota_key_rev()
    chwrite(sock, c_info)
    res = chread(sock, ClientInfo)

    @test res == ClientInfo(
        0,
        "user",
        "aaa-aaa",
        ":0",
        1,
        "osu",
        "host",
        "name",
        10,
        2,
        23331,
        "quota",
        0
    )

    sock.server_rev = ClickHouse.has_version_patch_rev()
    chwrite(sock, c_info)
    res = chread(sock, ClientInfo)

    @test res == ClientInfo(
        0,
        "user",
        "aaa-aaa",
        ":0",
        1,
        "osu",
        "host",
        "name",
        10,
        2,
        23331,
        "quota",
        10
    )
end
@testset "ch packets" begin
    c_hello = ClientHello(
        "ddddd",
        10,
        10,
        10,
        "db",
        "us",
        "pass"
    )

    sock = ClickHouseSock(PipeBuffer())
    write_packet(sock, c_hello)
    res = read_packet(sock, ClickHouse.ClientCodes)
    @test res isa ClientHello


    sock = ClickHouseSock(PipeBuffer())
    write_packet(sock, ClickHouse.ServerPong())
    res = read_packet(sock, ClickHouse.ServerCodes)
    @test res isa ClickHouse.ServerPong
end