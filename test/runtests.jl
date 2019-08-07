using Test
using ClickHouse
using DataFrames

@test begin
    io = IOBuffer([0xC2, 0x0A])
    ctx = ClickHouse.ReadCtx(io, false)
    ClickHouse.chread(ctx, ClickHouse.VarUInt) == ClickHouse.VarUInt(0x542)
end

@test begin
    io = IOBuffer(UInt8[], read=true, write=true, maxsize=10)
    ctx = ClickHouse.WriteCtx(io, false)
    ClickHouse.chwrite(ctx, ClickHouse.VarUInt(100_500))
    seek(ctx.io, 0)
    read(ctx.io, 3) == [0x94, 0x91, 0x06]
end

@testset "Decode & re-encode client packets (SELECT 1)" begin
    # This .bin file was extracted from a tcpdump captured from a session
    # with the official ClickHouse command line client.
    data = read(open("select1/client-query.bin"), 100_000, all = true)
    io = IOBuffer(data)
    packets = []

    # Read packets.

    packet = ClickHouse.read_client_packet(io)
    push!(packets, packet)
    @test typeof(packet) == ClickHouse.ClientHello

    packet = ClickHouse.read_client_packet(io)
    push!(packets, packet)
    @test typeof(packet) == ClickHouse.ClientPing

    packet = ClickHouse.read_client_packet(io)
    push!(packets, packet)
    @test typeof(packet) == ClickHouse.ClientQuery

    packet = ClickHouse.read_client_packet(io)
    push!(packets, packet)
    @test typeof(packet) == ClickHouse.Block

    @test eof(io)

    # Re-encode them.

    io = IOBuffer(UInt8[], write = true, read = true, maxsize = 100_000)
    for packet ∈ packets
        ClickHouse.write_packet(io, packet)
    end

    seek(io, 0)
    reencoded_data = read(io, 100_000)

    @test reencoded_data == data
end

@testset "Decode server packets (SELECT 1)" begin
    io = open("select1/server-query-resp.bin")

    packet = ClickHouse.read_server_packet(io)
    @test typeof(packet) == ClickHouse.ServerInfo

    packet = ClickHouse.read_server_packet(io)
    @test typeof(packet) == ClickHouse.ServerPong

    packet = ClickHouse.read_server_packet(io)
    @test typeof(packet) == ClickHouse.Block

    packet = ClickHouse.read_server_packet(io)
    @test typeof(packet) == ClickHouse.Block

    packet = ClickHouse.read_server_packet(io)
    @test typeof(packet) == ClickHouse.ServerProfileInfo

    packet = ClickHouse.read_server_packet(io)
    @test typeof(packet) == ClickHouse.ServerProgress

    packet = ClickHouse.read_server_packet(io)
    @test typeof(packet) == ClickHouse.Block

    packet = ClickHouse.read_server_packet(io)
    @test typeof(packet) == ClickHouse.ServerEndOfStream

    @test eof(io)
end

@testset "Decode client packets (INSERT INTO woof VALUES (1))" begin
    io = open("insert1/client.bin")

    while !eof(io)
        packet = ClickHouse.read_client_packet(io)
    end

    @test true
end

@testset "Decode server packets (OHLC data)" begin
    io = open("insert-ohlc/server.bin")

    while !eof(io)
        packet = ClickHouse.read_server_packet(io)
    end
end

@testset "Decode server packets (exception)" begin
    io = open("error/server.bin")

    while !eof(io)
        try
            packet = ClickHouse.read_server_packet(io)
        catch exc
            if !isa(exc, ClickHouseServerException)
                rethrow()
            end
        end
    end
end

@testset "Decode & re-encode client packets (OHLC data)" begin
    data = read(open("insert-ohlc/client.bin"), 1_000_000, all = true)
    io = IOBuffer(data)

    # Read packets.

    packets = []
    while !eof(io)
        packet = ClickHouse.read_client_packet(io)
        push!(packets, packet)
    end

    @test eof(io)

    # Re-encode them.

    io = IOBuffer(UInt8[], write = true, read = true, maxsize = 1_000_000)
    for packet ∈ packets
        ClickHouse.write_packet(io, packet)
    end

    seek(io, 0)
    reencoded_data = read(io, 1_000_000)

    @test reencoded_data == data
end

@testset "Queries on localhost DB" begin
    table = "ClickHouseJL_Test"
    sock = connect("localhost", 9000)

    try
        execute(sock, """
            CREATE TABLE $(table)
                (lul UInt64, oof Float32, foo String)
            ENGINE = Memory
        """)
    catch exc
        exc::ClickHouseServerException
        occursin(r"Table .* already exists", exc.exc.message) || rethrow()
    end

    data = Dict(
        :lul => UInt64[42, 1337, 123],
        :oof => Float32[0., ℯ, π],
        :foo => String["aa", "bb", "cc"],
    )

    # Single block inserts.
    for _ ∈ 1:3
        insert(sock, table, [data])
    end

    # Multi block insert.
    insert(sock, table, repeat([data], 100))

    # SELECT -> Dict
    proj = ClickHouse.select(sock, "SELECT * FROM $(table) LIMIT 4")
    @test proj[:lul] == UInt64[42, 1337, 123, 42]
    @test proj[:oof] == Float32[0., ℯ, π, 0.]
    @test proj[:foo] == String["aa", "bb", "cc", "aa"]

    # SELECT -> DF
    proj_df = select_df(sock, "SELECT * FROM $(table) LIMIT 3, 3")
    exp_df = DataFrame(data)

    # Normalize column order.
    order = [:lul, :oof, :foo]
    proj_df = proj_df[:, order]
    exp_df = exp_df[:, order]

    @test proj_df == exp_df

    # Clean up.
    execute(sock, "DROP TABLE $(table)")
end
