using ClickHouse: Column, chwrite, chread, read_col, VarUInt
using Dates
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
#= FIXME not working now becouse of wrong type work in origin package
@testset "Date columns" begin

    sock = ClickHouseSock(PipeBuffer())
    nrows = 100
    data = Date.(rand(2010:2020, nrows), rand(1:12, nrows), rand(1:20, nrows))
    column = Column("test", "Date", data)
    chwrite(sock, column)
    res = read_col(sock, VarUInt(nrows))
    @test res == column

end=#

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

