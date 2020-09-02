using Sockets: IPv6
is_ch_type(::Val{:IPv6})  = true
result_type(::Val{:IPv6})  = Vector{IPv6}

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:IPv6})
    tmp = Vector{IPv6}(undef, UInt64(num_rows))
    Base.read!(sock.io, tmp)
    return tmp
end

function write_col_data(sock::ClickHouseSock,
                        data::AbstractVector{IPv6}, ::Val{:IPv6})

    Base.write(sock.io, data)
end