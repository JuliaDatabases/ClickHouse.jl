using Test
using ClickHouse
using ClickHouse: read_client_packet, read_server_packet
using DataFrames
using Dates

@test begin
    sock = IOBuffer([0xC2, 0x0A]) |> ClickHouseSock
    ClickHouse.chread(sock, ClickHouse.VarUInt) == ClickHouse.VarUInt(0x542)
end

@test begin
    sock = IOBuffer(UInt8[], read=true, write=true, maxsize=10) |>
        ClickHouseSock
    ClickHouse.chwrite(sock, ClickHouse.VarUInt(100_500))
    seek(sock.io, 0)
    read(sock.io, 3) == [0x94, 0x91, 0x06]
end

@testset "Decode & re-encode client packets (SELECT 1)" begin
    # This .bin file was extracted from a tcpdump captured from a session
    # with the official ClickHouse command line client.
    data = read(open("select1/client-query.bin"), 100_000, all = true)
    sock = data |> IOBuffer |> ClickHouseSock
    packets = []

    # Read packets.

    packet = read_client_packet(sock)
    push!(packets, packet)
    @test typeof(packet) == ClickHouse.ClientHello

    packet = read_client_packet(sock)
    push!(packets, packet)
    @test typeof(packet) == ClickHouse.ClientPing

    packet = read_client_packet(sock)
    push!(packets, packet)
    @test typeof(packet) == ClickHouse.ClientQuery

    packet = read_client_packet(sock)
    push!(packets, packet)
    @test typeof(packet) == ClickHouse.Block

    @test eof(sock.io)

    # Re-encode them.

    sock = IOBuffer(UInt8[], write = true, read = true, maxsize = 100_000) |>
        ClickHouseSock

    for packet ∈ packets
        ClickHouse.write_packet(sock, packet)
    end

    seek(sock.io, 0)
    reencoded_data = read(sock.io, 100_000)

    @test reencoded_data == data
end

@testset "Decode server packets (SELECT 1)" begin
    sock = open("select1/server-query-resp.bin") |> ClickHouseSock

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerInfo

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerPong

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.Block

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.Block

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerProfileInfo

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerProgress

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.Block

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerEndOfStream

    @test eof(sock.io)
end

@testset "Decode client packets (INSERT INTO woof VALUES (1))" begin
    sock = open("insert1/client.bin") |> ClickHouseSock

    while !eof(sock.io)
        packet = read_client_packet(sock)
    end

    @test true
end

@testset "Decode server packets (OHLC data)" begin
    sock = open("insert-ohlc/server.bin") |> ClickHouseSock

    while !eof(sock.io)
        packet = read_server_packet(sock)
    end
end

@testset "Decode server packets (enums)" begin
    sock = open("enum/server.bin") |> ClickHouseSock

    while !eof(sock.io)
        packet = read_server_packet(sock)
    end
end

@testset "Decode & re-encode client packets (enums)" begin
    data = read(open("enum/client.bin"), 10_000, all = true)
    sock = IOBuffer(data) |> ClickHouseSock

    # Read packets.

    packets = []
    while !eof(sock.io)
        packet = read_client_packet(sock)
        push!(packets, packet)
    end

    @test eof(sock.io)

    # Re-encode them.

    sock = IOBuffer(UInt8[], write = true, read = true, maxsize = 10_000) |>
        ClickHouseSock
    for packet ∈ packets
        ClickHouse.write_packet(sock, packet)
    end

    seek(sock.io, 0)
    reencoded_data = read(sock.io, 10_000)

    @test reencoded_data == data
end

@testset "Decode server packets (exception)" begin
    sock = open("error/server.bin") |> ClickHouseSock

    while !eof(sock.io)
        try
            packet = read_server_packet(sock)
        catch exc
            if !isa(exc, ClickHouseServerException)
                rethrow()
            end
        end
    end
end

@testset "Decode & re-encode client packets (OHLC data)" begin
    data = read(open("insert-ohlc/client.bin"), 1_000_000, all = true)
    sock = IOBuffer(data) |> ClickHouseSock

    # Read packets.

    packets = []
    while !eof(sock.io)
        packet = read_client_packet(sock)
        push!(packets, packet)
    end

    @test eof(sock.io)

    # Re-encode them.

    sock = IOBuffer(UInt8[], write = true, read = true, maxsize = 1_000_000) |>
        ClickHouseSock
    for packet ∈ packets
        ClickHouse.write_packet(sock, packet)
    end

    seek(sock.io, 0)
    reencoded_data = read(sock.io, 1_000_000)

    @test reencoded_data == data
end

@testset "Queries on localhost DB" begin
    table = "ClickHouseJL_Test"
    sock = connect()

    try
        execute(sock, """
            CREATE TABLE $(table) (
                lul UInt64,
                oof Float32,
                foo String,
                ddd Date,
                enu Enum8('a' = 1, 'c' = 3, 'foobar' = 44, 'd' = 9)
            )
            ENGINE = Memory
        """)
    catch exc
        exc::ClickHouseServerException
        occursin(r"Table .* already exists", exc.exc.message) || rethrow()
        sock = connect()
    end

    td = today()
    data = Dict(
        :lul => UInt64[42, 1337, 123],
        :oof => Float32[0., ℯ, π],
        :foo => String["aa", "bb", "cc"],
        :ddd => DateTime[td, td, td],
        :enu => ["a", "c", "foobar"],
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
    @test proj[:ddd] == DateTime[td, td, td, td]

    # SELECT -> DF
    proj_df = select_df(sock, "SELECT * FROM $(table) LIMIT 3, 3")
    exp_df = DataFrame(data)

    # Normalize column order.
    order = [:lul, :oof, :foo, :ddd]
    proj_df = proj_df[:, order]
    exp_df = exp_df[:, order]

    @test proj_df == exp_df

    # Clean up.
    execute(sock, "DROP TABLE $(table)")
end
