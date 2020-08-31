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

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = rand(Int32, nrows)
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

@testset "Nullable columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = rand(Int64, nrows)
    data = convert(Vector{Union{Int64, Missing}}, data)
    data[rand(1:nrows, 20)] .= missing
    column = Column("test", "Nullable(Int64)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test all(zip(data, res.data)) do t
        (a, b) = t
        return (ismissing(a) && ismissing(b)) ||
        (a == b)
    end

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = rand(Float64, nrows)
    data = convert(Vector{Union{Float64, Missing}}, data)
    data[rand(1:nrows, 20)] .= missing
    column = Column("test", "Nullable(Float64)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test all(zip(data, res.data)) do t
        (a, b) = t
        return (ismissing(a) && ismissing(b)) ||
        (a ≈ b)
    end

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = string.(rand(Int64, nrows))
    data = convert(Vector{Union{String, Missing}}, data)
    data[rand(1:nrows, 20)] .= missing
    column = Column("test", "Nullable(String)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test all(zip(data, res.data)) do t
        (a, b) = t
        return (ismissing(a) && ismissing(b)) ||
        (a == b)
    end


    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = CategoricalVector(rand(["a","b","c"], nrows))
    data = convert(CategoricalVector{Union{String, Missing}}, data)
    data[rand(1:nrows, 20)] .= missing

    column = Column("test", "Nullable(Enum8('a'=1,'b'=3,'c'=10))", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))

    @test all(zip(data, res.data)) do t
        (a, b) = t
        return (ismissing(a) && ismissing(b)) ||
        (a == b)
    end

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Date.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows))
    data = convert(Vector{Union{Date, Missing}}, data)
    data[rand(1:nrows, 20)] .= missing
    column = Column("test", "Nullable(Date)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test all(zip(data, res.data)) do t
        (a, b) = t
        return (ismissing(a) && ismissing(b)) ||
        (a == b)
    end

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = DateTime.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows),
    rand(0:23, nrows), rand(0:59, nrows), rand(0:59, nrows))
    data = convert(Vector{Union{DateTime, Missing}}, data)
    data[rand(1:nrows, 20)] .= missing
    column = Column("test", "Nullable(DateTime)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test all(zip(data, res.data)) do t
        (a, b) = t
        return (ismissing(a) && ismissing(b)) ||
        (a == b)
    end
end

@testset "LowCardinality columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 10
    data = rand(1:10, nrows)
    column = Column("test", "LowCardinality(Int64)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))

    @test res.data == data

    sock = ClickHouseSock(PipeBuffer())
    data = rand(1:10, nrows)
    data = convert(Vector{Union{Int64, Missing}}, data)
    data[rand(1:nrows, 5)] .= missing
    column = Column("test", "LowCardinality(Nullable(Int64))", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))

    @test all(zip(data, res.data)) do t
        (a, b) = t
        return (ismissing(a) && ismissing(b)) ||
        (a == b)
    end

    sock = ClickHouseSock(PipeBuffer())
    data = rand(["a", "b", "c"], nrows)
    column = Column("test", "LowCardinality(String)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))

    @test res.data == data

    sock = ClickHouseSock(PipeBuffer())
    data = CategoricalVector(rand(["a","b","c"], nrows))

    column = Column("test", "LowCardinality(Enum8('a'=1,'b'=3,'c'=10))", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res.data == data

end

@testset "Array collumns" begin

    nrows = 1000
    sock = ClickHouseSock(PipeBuffer())
    data = rand([[[1,2],[3]], [[3],[4,5]], [[6],[7,8]]], nrows)
    column = Column("test", "Array(Array(Int64))", data)
    chwrite(sock, column)

    res = read_col(sock, VarUInt(nrows))
    @test res.data == data

    sock = ClickHouseSock(PipeBuffer())
    data = rand([
        ["ab", "bc", "cd"],
        ["ab", "ed", "ab"]
    ], nrows)
    column = Column("test", "Array(String)", data)
    chwrite(sock, column)

    res = read_col(sock, VarUInt(nrows))
    @test res.data == data

    sock = ClickHouseSock(PipeBuffer())
    data = rand([
        ["ab", "bc", "cd"],
        ["ab", "ed", "ab"]
    ], nrows)
    column = Column("test", "Array(LowCardinality(String))", data)
    chwrite(sock, column)

    res = read_col(sock, VarUInt(nrows))
    @test res.data == data
    sock = ClickHouseSock(PipeBuffer())
    data = rand([
        [["ab"], [missing, "cd"]],
        [["ab", "ac"], [missing, "ab"]]
    ], nrows)
    sock = ClickHouseSock(PipeBuffer())
    column = Column("test", "Array(Array(Nullable(String)))", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test string(data) == string(res.data)
    @test recursive_miss_cmp(data, res.data)

    sock = ClickHouseSock(PipeBuffer())
    data = rand([
        [["ab"], [missing, "cd"]],
        [["ab", "ac"], [missing, "ab"]]
    ], nrows)
    sock = ClickHouseSock(PipeBuffer())
    column = Column("test", "Array(Array(LowCardinality(Nullable(String))))", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test recursive_miss_cmp(data, res.data)
end