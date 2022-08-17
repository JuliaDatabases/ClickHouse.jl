is_ch_type(::Val{:Bool})  = true
result_type(::Val{:Bool})  = Vector{Bool}

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:Bool})
    data = chread(sock, Vector{UInt8}, num_rows)
    return Bool.(data)
end

function write_col_data(sock::ClickHouseSock,
            data::AbstractVector{Bool}, ::Val{:Bool})
    d = Vector{UInt8}(undef, length(data))
    d .= data
    chwrite(sock, d)
end
