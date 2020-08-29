using UUIDs
is_ch_type(::Val{:UUID})  = true

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:UUID})
    tmp = Vector{UUID}(undef, UInt64(num_rows))
    Base.read!(sock.io, tmp)

    return tmp
end

function write_col_data(sock::ClickHouseSock,
                        data::AbstractVector{UUID}, ::Val{:UUID})
    tmp = reinterpret(Tuple{UInt64, UInt64}, data)
    Base.write(sock.io,
        data
    )
end