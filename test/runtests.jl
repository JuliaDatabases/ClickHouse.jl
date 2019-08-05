using Test
using ClickHouse
import ClickHouse: VarUInt
using Sockets

@test begin
    io = IOBuffer([0xC2, 0x0A])
    ctx = ClickHouse.ReadCtx(io, false)
    ClickHouse.chread(ctx, VarUInt) == VarUInt(0x542)
end

@test begin
    io = IOBuffer(UInt8[], read=true, write=true, maxsize=10)
    ctx = ClickHouse.WriteCtx(io, false)
    ClickHouse.chwrite(ctx, VarUInt(100_500))
    seek(ctx.io, 0)
    read(ctx.io, 3) == [0x94, 0x91, 0x06]
end

@testset "Decode & re-encode client packets `SELECT 1;`" begin
    # This .bin file was extracted from a tcpdump captured from a session
    # with the official ClickHouse command line client.
    data = read(open("select1-client-query.bin"), 100_000, all = true)
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

@testset "Decode server packets `SELECT 1;`" begin
    io = open("select1-server-query-resp.bin")

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

@testset "Decode server packets `INSERT INTO woof VALUES (1);`" begin
    io = open("insert1-client.bin")

    while !eof(io)
        packet = ClickHouse.read_client_packet(io)
        @show packet
    end
end

# @testset "SELECT 1; on localhost DB" begin
#     sock = connect("localhost", 9000)
#     hello = ClickHouse.ClientHello("Julia", 1, 1, 54423, "", "default", "")
#     ClickHouse.write_packet(sock, hello)

#     server_info = ClickHouse.read_server_packet(sock)
#     @test typeof(server_info) == ClickHouse.ServerInfo
#     @test server_info.server_name == "ClickHouse"

#     ping = ClickHouse.ClientPing()
#     ClickHouse.write_packet(sock, ping)

#     pong = ClickHouse.read_server_packet(sock)
#     @test typeof(pong) == ClickHouse.ServerPong

#     query = ClickHouse.ClientQuery(
#         "",
#         ClickHouse.ClientInfo(
#             0x01,
#             "",
#             "",
#             "0.0.0.0:0",
#             0x01,
#             "",
#             "6b88a142c2bc",
#             "ClickHouse client",
#             19,
#             11,
#             54423,
#             "",
#             2
#         ),
#         "",
#         2,
#         0,
#         "SELECT 1",
#     )
#     ClickHouse.write_packet(sock, query)

#     block = ClickHouse.Block("", ClickHouse.BlockInfo(), 0, 0, [])
#     ClickHouse.write_packet(sock, block)

#     packet = ClickHouse.read_server_packet(sock)
#     @show packet

#     packet = ClickHouse.read_server_packet(sock)
#     @show packet

#     packet = ClickHouse.read_server_packet(sock)
#     @show packet
# end