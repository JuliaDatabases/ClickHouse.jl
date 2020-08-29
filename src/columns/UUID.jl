using UUIDs
is_ch_typename(::Val{:UUID})  = true

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:UUID})
    tmp = Vector{UUID}(undef, UInt64(num_rows))
    Base.read!(sock.io, tmp)

    return tmp
    #=return UUID.(
        @. (UInt128(getindex(tmp, 1)) << 64) | getindex(tmp, 2)
    )=#
end

function write_col_data(sock::ClickHouseSock, data::AbstractVector{UUID}, ::Val{:UUID})
    tmp = reinterpret(Tuple{UInt64, UInt64}, data)
    Base.write(sock.io,
        data
    )
end