using Test
using ClickHouse
using ClickHouse: read_client_packet, read_server_packet
using DataFrames
using Dates
using UUIDs
using DecFP

function recursive_miss_cmp(a::AbstractVector,b::AbstractVector)
    length(a) != length(b) && return false
    for i in 1:length(a)
        (!recursive_miss_cmp(a[i], b[i])) && return false
    end
    return true
end
function recursive_miss_cmp(a,b)
    return (ismissing(a) && ismissing(b)) ||
        (!ismissing(a == b) && a==b)
end

function mock_server_info()
    return ClickHouse.ServerInfo(
        "mock",
        19,
        11,
        54423,
        "UTC",
        "mock server",
        10
    )
end


include("defines.jl")
include("tcp.jl")
include("columns_io.jl")
include("cityhash.jl")
using CategoricalArrays
using Sockets: IPv4, IPv6

function miss_or_equal(a, b)
    return (ismissing(a) && ismissing(b)) ||
            (a==b)
end
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
    # with the official ClickHouse mand line client.
    data = read(open("select1/client-query.bin"), 100_000, all = true)
    sock = data |> IOBuffer |> ClickHouseSock
    sock.server_rev = ClickHouse.DBMS_VER_REV
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
    sock.server_rev = ClickHouse.DBMS_VER_REV

    for packet ∈ packets
        ClickHouse.write_packet(sock, packet)
    end

    seek(sock.io, 0)
    reencoded_data = read(sock.io, 100_000)

    @test reencoded_data == data
end

@testset "Decode server packets (SELECT 1)" begin
    sock = open("select1/server-query-resp.bin") |> ClickHouseSock
    sock.server_rev = ClickHouse.DBMS_VER_REV

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerInfo

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerPong

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerData

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerData

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerProfileInfo

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerProgress

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerData

    packet = read_server_packet(sock)
    @test typeof(packet) == ClickHouse.ServerEndOfStream

    @test eof(sock.io)
end

@testset "Decode client packets (INSERT INTO woof VALUES (1))" begin
    sock = open("insert1/client.bin") |> ClickHouseSock
    sock.server_rev = ClickHouse.DBMS_VER_REV

    while !eof(sock.io)
        packet = read_client_packet(sock)
    end

    @test true
end

@testset "Decode server packets (OHLC data)" begin
    sock = open("insert-ohlc/server.bin") |> ClickHouseSock
    sock.server_rev = ClickHouse.DBMS_VER_REV

    while !eof(sock.io)
        packet = read_server_packet(sock)
    end
end

@testset "Decode server packets (enums)" begin
    sock = open("enum/server.bin") |> ClickHouseSock
    sock.server_rev = ClickHouse.DBMS_VER_REV

    while !eof(sock.io)
        packet = read_server_packet(sock)
    end
end

@testset "Decode & re-encode client packets (enums)" begin
    data = read(open("enum/client.bin"), 10_000, all = true)
    sock = IOBuffer(data) |> ClickHouseSock
    sock.server_rev = ClickHouse.DBMS_VER_REV

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
    sock.server_rev = ClickHouse.DBMS_VER_REV
    for packet ∈ packets
        ClickHouse.write_packet(sock, packet)
    end

    seek(sock.io, 0)
    reencoded_data = read(sock.io, 10_000)

    @test reencoded_data == data
end

@testset "Decode server packets (exception)" begin
    sock = open("error/server.bin") |> ClickHouseSock
    sock.server_rev = ClickHouse.DBMS_VER_REV

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
    sock.server_rev = ClickHouse.DBMS_VER_REV

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
    sock.server_rev = ClickHouse.DBMS_VER_REV
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
                foo_fixed FixedString(5),
                ddd Date,
                dt DateTime,
                dt_tz DateTime('CET'),
                enu Enum8('a' = 1, 'c' = 3, 'foobar' = 44, 'd' = 9),
                uuid UUID,
                nn Nullable(Int64),
                ns Nullable(String),
                ne Nullable(Enum16('a' = 1, 'b' = 2)),
                las LowCardinality(String),
                lan LowCardinality(Nullable(String)),
                arrs Array(LowCardinality(String)),
                arrsn Array(Array(Int64)),
                arrsnn Array(Array(Nullable(Int64))),
                safunc SimpleAggregateFunction(sum, Int64),
                ip4 Nullable(IPv4),
                ip6 Nullable(IPv6),
                dt64 DateTime64(6),
                dt64_1 DateTime64(1),
                dt64_1tz DateTime64(1, 'GMT'),
                dec32 Decimal32(4),
                dec Decimal(11,4)
            )
            ENGINE = Memory
        """)
    catch exc
        rethrow(exc)
        #exc::ClickHouseServerException
        #occursin(r"Table .* already exists", exc.exc.message) || rethrow()
        #sock = connect()
    end
    NullInt = Union{Int64, Missing}
    td = today()
    data = Dict(
        :lul => UInt64[42, 1337, 123],
        :oof => Float32[0., ℯ, π],
        :foo => String["aa", "bb", "cc"],
        :foo_fixed => String["aaaaa", "bbb", "cc"],
        :ddd => Date[td, td, td],
        :dt => DateTime[td, td, td],
        :dt_tz => DateTime[td, td, td],
        :enu => ["a", "c", "foobar"],
        :uuid => [
            UUID("c187abfa-31c1-4131-a33e-556f23f7aa67"),
            UUID("f9a7e2b9-dc22-4ca6-b4fe-83ba551ea3bb"),
            UUID("dc986a81-9f1d-4d96-b618-6e8d034285c1")
             ],
        :nn => [10, missing, 20],
        :ns => [missing, "sst", "aaa"],
        :ne => CategoricalVector(["a", "b", missing]),
        :las => ["a", "b", "a"],
        :lan => [missing, "b", "a"],
        :arrs => [
            ["a", "b"],
            ["a"],
            ["v", "b"]
             ],
        :arrsn => [
            [[1,2], [3,4]],
            [[5,6],[7]],
            [[1], [2]]
            ],
        :arrsnn => [
            [NullInt[1,2], NullInt[3,4]],
            [NullInt[5,6],NullInt[7]],
            [NullInt[1], NullInt[missing]]
            ],
        :safunc => Int64[42, 1337, 123],
        :ip4 => [IPv4("127.0.0.2"), missing, IPv4("127.0.0.1")],
        :ip6 => [
            IPv6("2a02:aa08:e000:3100::2"),
            missing,
            IPv6("2a02:aa08:e000:3100::3")
        ],
        :dt64 => [
            DateTime(2020, 02, 02, 10, 5, 10, 320),
            DateTime(2020, 02, 02, 10, 5, 10, 322),
            DateTime(2020, 02, 02, 10, 5, 10, 323)
        ],
        :dt64_1 => [
            DateTime(2020, 02, 02, 10, 5, 10, 320),
            DateTime(2020, 02, 02, 10, 5, 10, 422),
            DateTime(2020, 02, 02, 10, 5, 10, 523)
        ],
        :dt64_1tz => [
            DateTime(2020, 02, 02, 10, 5, 11, 320),
            DateTime(2020, 02, 02, 10, 5, 11, 422),
            DateTime(2020, 02, 02, 10, 5, 10, 529)
        ],
        :dec32 => [
            Dec32("221.3213"),
            Dec32("225.3215"),
            Dec32("227.3219"),
        ],
        :dec => [
            Dec64("5432221.3213"),
            Dec64("6432221.4213"),
            Dec64("7432221.5213")
        ]
    )

    # Single block inserts.
    for _ ∈ 1:3
        insert(sock, table, [data])
    end

    # Multi block insert.
    insert(sock, table, repeat([data], 100))

    SELECT -> Dict
    proj = ClickHouse.select(sock, "SELECT * FROM $(table) LIMIT 4")
    @show proj

    @test proj[:lul] == UInt64[42, 1337, 123, 42]
    @test proj[:oof] == Float32[0., ℯ, π, 0.]
    @test proj[:foo] == String["aa", "bb", "cc", "aa"]
    @test proj[:foo_fixed] == String["aaaaa", "bbb  ", "cc   ", "aaaaa"]
    @test proj[:ddd] == Date[td, td, td, td]
    @test proj[:dt] == DateTime[td, td, td, td]
    @test proj[:dt_tz] == DateTime[td, td, td, td]
    @test proj[:uuid] == vcat(data[:uuid], data[:uuid][1:1])
    @test recursive_miss_cmp(proj[:nn], [10, missing, 20 , 10])
    @test recursive_miss_cmp(proj[:ns], [missing, "sst", "aaa" , missing])
    @test recursive_miss_cmp(proj[:ne], ["a", "b", missing, "a"])


    @test proj[:las] == ["a", "b", "a", "a"]

    @test recursive_miss_cmp(proj[:lan], [missing, "b", "a",  missing])

    @test proj[:arrsn] == [
        [[1,2], [3,4]],
        [[5,6],[7]],
        [[1], [2]],
        [[1,2], [3,4]],
    ]

    @test proj[:arrs] == [
        ["a", "b"],
        ["a"],
        ["v", "b"],
        ["a", "b"],
    ]

    @test recursive_miss_cmp(proj[:arrsnn],
        [
            [NullInt[1,2], NullInt[3,4]],
            [NullInt[5,6],NullInt[7]],
            [NullInt[1], NullInt[missing]],
            [NullInt[1,2], NullInt[3,4]],
        ]
    )

    @test proj[:safunc] == Int64[42, 1337, 123, 42]

    @test recursive_miss_cmp(proj[:ip4], [
        IPv4("127.0.0.2"),
        missing,
        IPv4("127.0.0.1"),
        IPv4("127.0.0.2")
        ])
    @test recursive_miss_cmp(proj[:ip6], [
        IPv6("2a02:aa08:e000:3100::2"),
        missing,
        IPv6("2a02:aa08:e000:3100::3"),
        IPv6("2a02:aa08:e000:3100::2")
    ])

    @test proj[:dt64] ==  [
        DateTime(2020, 02, 02, 10, 5, 10, 320),
        DateTime(2020, 02, 02, 10, 5, 10, 322),
        DateTime(2020, 02, 02, 10, 5, 10, 323),
        DateTime(2020, 02, 02, 10, 5, 10, 320)
    ]

    @test proj[:dt64_1] == [
            DateTime(2020, 02, 02, 10, 5, 10, 300),
            DateTime(2020, 02, 02, 10, 5, 10, 400),
            DateTime(2020, 02, 02, 10, 5, 10, 500),
            DateTime(2020, 02, 02, 10, 5, 10, 300),
        ]

    @test proj[:dt64_1tz] == [
        DateTime(2020, 02, 02, 10, 5, 11, 300),
        DateTime(2020, 02, 02, 10, 5, 11, 400),
        DateTime(2020, 02, 02, 10, 5, 10, 500),
        DateTime(2020, 02, 02, 10, 5, 11, 300),
    ]

    @test proj[:dec32] == [
        Dec32("221.3213"),
        Dec32("225.3215"),
        Dec32("227.3219"),
        Dec32("221.3213"),
    ]
    @test proj[:dec] == [
        Dec64("5432221.3213"),
        Dec64("6432221.4213"),
        Dec64("7432221.5213"),
        Dec64("5432221.3213"),

    ]

    # SELECT Tuple -> Dict

    proj = ClickHouse.select(sock, "SELECT tuple(ddd, tuple(lul, foo)) as tup FROM $(table) LIMIT 2")
    @test proj[:tup] == [
        (td, (UInt64(42), "aa")),
        (td, (UInt64(1337), "bb")),
    ]

    proj = ClickHouse.select(sock, "SELECT null as n, array() as arr FROM $(table) LIMIT 3")
    @test all(ismissing.(proj[:n]))
    @test all(proj[:arr] .== Ref(Missing[]))

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
