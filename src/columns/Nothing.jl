is_ch_type(::Val{:Nothing})  = true
result_type(::Val{:Nothing}) = Vector{Missing}

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:Nothing})
    chread(sock, Vector{UInt8}, num_rows) #Just seek, we not intresting on this values
    return Vector{Missing}(undef, UInt64(num_rows))
end


function write_col_data(sock::ClickHouseSock,
                                data::T,
                                ::Val{:Nothing}) where {T}

    chwrite(sock, zeros(UInt8, length(data)))
end