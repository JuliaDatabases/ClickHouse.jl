using ClickHouse: Column, chwrite, chread, read_col, VarUInt, parse_typestring
using Dates
using CategoricalArrays
using UUIDs

@testset "Parse type" begin
    r = parse_typestring("Int32")
    @test r.name == :Int32
    @test_throws ErrorException parse_typestring("KKKK")

    r = parse_typestring("   String  ")
    @test r.name == :String

    r = parse_typestring("   Enum8('a' = 10, 'b'=1, 'addd' = 45)  ")

    @test r.name == :Enum8
    @test length(r.args) == 3
    @test r.args[1] == "'a' = 10"
    @test r.args[2] == "'b'=1"
    @test r.args[3] == "'addd' = 45"

    r = parse_typestring(" FixedString(4)")
    @test r.name == :FixedString
    @test r.args[1] == "4"
    r = parse_typestring(" FixedString(44)")
    @test r.name == :FixedString
    @test r.args[1] == "44"

    r = parse_typestring("Tuple(Int64, String)")
    @test r.name == :Tuple
    @test r.args[1].name == :Int64
    @test r.args[2].name == :String

    r = parse_typestring("Tuple(Enum16('a' = 10), Tuple(Int32, Float32))")
    @test r.name == :Tuple
    @test r.args[1].name == :Enum16
    @test r.args[1].args[1] == "'a' = 10"
    @test r.args[2].name == :Tuple
    @test r.args[2].args[1].name == :Int32
    @test r.args[2].args[2].name == :Float32
end

@testset "Int columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = rand(Int64, nrows)
    column = Column("test", "Int64", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end

@testset "String columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = string.(rand(Int64, nrows))
    column = Column("test", "String", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end

@testset "Fixed String columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = string.(rand(["aaaa", "bbbb", "cccc", "dddd"], nrows))
    column = Column("test", "FixedString(4)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    data = string.(rand(["aaaaa", "bbbb", "cccc", "dddd"], nrows))
    column = Column("test", "FixedString(4)", data)
    @test_throws ErrorException chwrite(sock, column)


    sock = ClickHouseSock(PipeBuffer())
    data = string.(rand(["aaaa", "bbbb", "cccc", "dddd"], nrows))
    column = Column("test", "FixedString(5)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test all(s->length(s)==5, res.data)
    @test res.data == data.* " "
end

@testset "Date columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Date.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows))
    column = Column("test", "Date", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end

@testset "DateTime columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = DateTime.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows),
    rand(0:23, nrows), rand(0:59, nrows), rand(0:59, nrows))
    column = Column("test", "DateTime", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end

@testset "Enum columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = rand(["a","b","c"], nrows)
    column = Column("test", "Enum8('a'=1,'b'=3,'c'=10)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end

@testset "Enum columns categorial in" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = CategoricalVector(rand(["a","b","c"], nrows))
    column = Column("test", "Enum8('a'=1,'b'=3,'c'=10)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))

    @test res == column

end


@testset "UUID columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Vector{UUID}(undef, nrows)
    data .= uuid4()
    column = Column("test", "UUID", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end

@testset "Tuple columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = tuple.(rand(Int64, nrows), rand(Int8, nrows))
    column = Column("test", "Tuple(Int64, Int8)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = tuple.(rand(Int64, nrows), string.(rand(Int8, nrows)))
    column = Column("test", "Tuple(Int64, String)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = tuple.(
        rand(["aa", "bb", "ccc"], nrows),
        tuple.(rand(Int16, nrows))
        )
    column = Column("test", "Tuple(Enum16('aa' = 1, 'bb' = 2, 'ccc' = 10), Tuple(Int16))", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end