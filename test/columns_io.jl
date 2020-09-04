using ClickHouse: Column, chwrite, chread,
         read_col, VarUInt, parse_typestring, result_type
using Dates
using CategoricalArrays
using UUIDs
import Sockets
using Sockets: IPv4, IPv6
using DecFP

@testset "Parse type" begin
    r = parse_typestring("Int32")
    @test r.name == :Int32
    @test_throws ErrorException parse_typestring("KKKK")

    r = parse_typestring("   String  ")
    @test r.name == :String
    @test result_type(r) == Vector{String}

    r = parse_typestring("   Enum8('a' = 10, 'b'=1, 'addd' = 45)  ")

    @test r.name == :Enum8
    @test length(r.args) == 3
    @test r.args[1] == "'a' = 10"
    @test r.args[2] == "'b'=1"
    @test r.args[3] == "'addd' = 45"
    @test result_type(r) == CategoricalVector{String}

    r = parse_typestring(" FixedString(4)")
    @test r.name == :FixedString
    @test r.args[1] == "4"
    r = parse_typestring(" FixedString(44)")
    @test r.name == :FixedString
    @test r.args[1] == "44"
    @test result_type(r) == Vector{String}

    r = parse_typestring("Tuple(Int64, String)")
    @test r.name == :Tuple
    @test r.args[1].name == :Int64
    @test r.args[2].name == :String
    @test result_type(r) == Vector{Tuple{Int64, String}}

    r = parse_typestring("Tuple(Enum16('a' = 10), Tuple(Int32, Float32))")
    @test r.name == :Tuple
    @test r.args[1].name == :Enum16
    @test r.args[1].args[1] == "'a' = 10"
    @test r.args[2].name == :Tuple
    @test r.args[2].args[1].name == :Int32
    @test r.args[2].args[2].name == :Float32
    @test result_type(r) == Vector{
        Tuple{
            CategoricalValue{String},
            Tuple{Int32, Float32}
            }
        }

    r = parse_typestring("LowCardinality(String)")
    @test result_type(r) == CategoricalVector{String}

    r = parse_typestring("Array(Array(Nullable(Int32)))")
    @test result_type(r) == Vector{
        Vector{
            Vector{Union{Missing, Int32}}
        }
    }

end

@testset "Int columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Sockets.IPv4.(rand(UInt32, nrows))
    column = Column("test", "IPv4", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Sockets.IPv6.(rand(UInt128, nrows))
    column = Column("test", "IPv6", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end

@testset "IP columns" begin

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

@testset "DateTime64 columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = DateTime.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows),
    rand(0:23, nrows), rand(0:59, nrows), rand(0:59, nrows))
    column = Column("test", "DateTime64(0)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = DateTime.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows),
    rand(0:23, nrows), rand(0:59, nrows), rand(0:59, nrows))
    column = Column("test", "DateTime64(2)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = DateTime.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows),
    rand(0:23, nrows), rand(0:59, nrows), rand(0:59, nrows),
    rand(1:999))
    column = Column("test", "DateTime64(3)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = DateTime.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows),
    rand(0:23, nrows), rand(0:59, nrows), rand(0:59, nrows),
    rand(1:999))
    column = Column("test", "DateTime64(6)", data)
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
    data = CategoricalVector(rand(["a","b","c", missing], nrows))

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

@testset "Nothing column" begin
    sock = ClickHouseSock(PipeBuffer())
    data = [missing, missing, missing, missing]
    column = Column("test", "Nullable(Nothing)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(4))
    @test all(ismissing.(data))

    sock = ClickHouseSock(PipeBuffer())
    data = [[], [], [], []]
    column = Column("test", "Array(Nothing)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(4))
    @test all(data .== Ref(Missing[]))
end

@testset "Int columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = rand(Int64, nrows)
    column = Column("test", "SimpleAggregateFunction(sum, Int64)", data)
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


@testset "Decimal columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Dec32.(rand(1000:9999, nrows), -3)
    column = Column("test", "Decimal32(3)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    column = Column("test", "Decimal(4,3)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Dec64.(rand(1000000000:9999999999, nrows), -4)
    column = Column("test", "Decimal64(4)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    column = Column("test", "Decimal(10,4)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column
    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Dec128.(rand(Int128(10)^20:(Int128(10)^21 - 1), nrows), -14)
    column = Column("test", "Decimal128(14)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

    sock = ClickHouseSock(PipeBuffer())
    column = Column("test", "Decimal(20,14)", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end