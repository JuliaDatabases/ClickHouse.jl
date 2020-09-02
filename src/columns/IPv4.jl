using Sockets: IPv4
is_ch_type(::Val{:IPv4})  = true
result_type(::Val{:IPv4})  = Vector{IPv4}

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:IPv4})
    tmp = Vector{IPv4}(undef, UInt64(num_rows))
    Base.read!(sock.io, tmp)
    return tmp
end

function write_col_data(sock::ClickHouseSock,
                        data::AbstractVector{IPv4}, ::Val{:IPv4})

    Base.write(sock.io, data)
end