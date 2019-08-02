using Test
using ClickHouse

@test begin
    io = IOBuffer([0xC2, 0x0A])
    ctx = ClickHouse.ReadCtx(io, false)
    ClickHouse.chread(ctx, VarUInt) == VarUInt(0x542)
end

@test begin
    io = IOBuffer(UInt8[], read=true, write=true, maxsize=10)
    ctx = ClickHouse.WriteCtx(io, false)
    nb = ClickHouse.chwrite(ctx, VarUInt(100_500))
    seek(ctx.io, 0)
    actual = read(ctx.io, 3)
    actual == [0x94, 0x91, 0x06] && nb == 3
end