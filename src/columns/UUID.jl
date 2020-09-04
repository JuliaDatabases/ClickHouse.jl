using UUIDs
is_ch_type(::Val{:UUID})  = true
result_type(::Val{:UUID})  = Vector{UUID}

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:UUID})
    tmp = Vector{UUID}(undef, UInt64(num_rows))
    Base.read!(sock.io, tmp)
    return tmp
end

function write_col_data(sock::ClickHouseSock,
                        data::AbstractVector{UUID}, ::Val{:UUID})

    Base.write(sock.io,
        data
    )
end