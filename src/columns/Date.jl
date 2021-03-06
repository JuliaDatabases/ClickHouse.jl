is_ch_type(::Val{:Date})  = true
result_type(::Val{:Date})  = Vector{Date}

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:Date})
    data = chread(sock, Vector{UInt16}, num_rows)
    return Date(1970) + Day.(data)

end

function write_col_data(sock::ClickHouseSock,
            data::AbstractVector{Date}, ::Val{:Date})
    d = Vector{UInt16}(undef, length(data))
    d .= Dates.value.(data .- Date(1970))
    chwrite(sock, d)
end