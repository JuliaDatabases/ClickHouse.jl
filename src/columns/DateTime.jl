is_ch_type(::Val{:DateTime}) = true

result_type(
    ::Val{:DateTime},
    timezone::Union{String, Nothing} = nothing,
) = Vector{DateTime}

function read_col_data(
    sock::ClickHouseSock,
    num_rows::VarUInt,
    ::Val{:DateTime},
    timezone::Union{String, Nothing} = nothing,
)
    data = chread(sock, Vector{Int32}, num_rows)
    return unix2datetime.(data)
end

function write_col_data(
    sock::ClickHouseSock,
    data::AbstractVector{DateTime},
    ::Val{:DateTime},
    timezone::Union{String, Nothing} = nothing,
)
    d = round.(Int32, datetime2unix.(data))
    chwrite(sock, d)
end