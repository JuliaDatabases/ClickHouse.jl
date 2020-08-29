is_ch_typename(::Val{:DateTime})  = true

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:DateTime})
    data = chread(sock, Vector{Int32}, num_rows)
    return unix2datetime.(data)
end

function write_col_data(sock::ClickHouseSock, data::AbstractVector{DateTime}, ::Val{:DateTime}) where {N}
    d = round.(Int32,
        datetime2unix.(data)
    )
    chwrite(sock, d)
end