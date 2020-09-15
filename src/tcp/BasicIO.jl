function chwrite(sock::ClickHouseSock, x::VarUInt)
    mx::UInt64 = x
    while mx >= 0x80
        write(sock.io, UInt8(mx & 0xFF) | 0x80)
        mx >>= 7
    end
    write(sock.io, UInt8(mx & 0xFF))
end

function chread(sock::ClickHouseSock, ::Type{VarUInt})::VarUInt
    x::UInt64 = 0
    s::UInt32 = 0
    i::UInt64 = 0
    while true
        b = read(sock.io, UInt8)
        if b < 0x80
            if i > 9 || (i == 9 && b > 1)
                throw(OverflowError("varint would overflow"))
            end
            return x | UInt64(b) << s
        end

        x |= UInt64(b & 0x7F) << s
        s += 7
        i += 1
    end
end


chread(sock::ClickHouseSock, ::Type{T}) where T <: Number =
    read(sock.io, T)

chread(sock::ClickHouseSock, x::UInt64)::Vector{UInt8} =
    read(sock.io, x)

function chread(sock::ClickHouseSock, ::Type{String})::String
    len = chread(sock, VarUInt) |> UInt64
    chread(sock, len) |> String
end

# Vector reads
function chread(
    sock::ClickHouseSock,
    ::Type{Vector{T}},
    count::VarUInt,
)::Vector{T} where T <: Number
    data = Vector{T}(undef, UInt64(count))
    read!(sock.io, data)
    data
end

chread(
    sock::ClickHouseSock,
    ::Type{Vector{String}},
    count::VarUInt,
)::Vector{String} = [chread(sock, String) for _ ∈ 1:UInt64(count)]



# Scalar writes
chwrite(sock::ClickHouseSock, x::Number) =
    write(sock.io, x)

function chwrite(sock::ClickHouseSock, x::String)
    chwrite(sock, x |> sizeof |> VarUInt)
    chwrite(sock, x |> Array{UInt8})
end

# Vector writes
chwrite(sock::ClickHouseSock, x::AbstractVector{T}) where T <: Number =
    write(sock.io, x)

chwrite(sock::ClickHouseSock, x::AbstractVector{String}) =
    foreach(x -> chwrite(sock, x), x)