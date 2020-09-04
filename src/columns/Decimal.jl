using DecFP
is_ch_type(::Val{:Decimal32})  = true
is_ch_type(::Val{:Decimal64})  = true
is_ch_type(::Val{:Decimal128})  = true
is_ch_type(::Val{:Decimal})  = true

result_type(::Val{:Decimal32}, scale)  = Vector{Dec32}
result_type(::Val{:Decimal64}, scale)  = Vector{Dec64}
result_type(::Val{:Decimal128}, scale)  = Vector{Dec128}

function dec_type_by_preciosion(precision_str::String)
    precision = parse(Int, precision_str)

    precision in 1:9 && return :Decimal32
    precision in 10:18 && return :Decimal64
    precision in 19:38 && return :Decimal128
    error("Decimal error: unsupported precision $(precision)")
end
function result_type(::Val{:Decimal}, precision_str, scale)
    return result_type(
            Val(dec_type_by_preciosion(precision_str)),
            scale
        )
end

function read_decimal(::Type{DecT}, ::Type{IntT},
                     sock::ClickHouseSock, num_rows, scale_str) where {DecT, IntT}
    scale = parse(Int, scale_str)
    data = chread(sock, Vector{IntT}, num_rows)
    return DecT.(data, -scale)
end

function write_decimal(::Type{DecT}, ::Type{IntT},
    sock::ClickHouseSock, data, scale_str) where {DecT, IntT}
    scale = parse(Int, scale_str)
    tmp = Vector{IntT}(undef, length(data))
    for (i,v) in enumerate(data)
        (sign, value, exp) = sigexp(convert(DecT, v))

        (exp != -scale) &&
            error("Decimal:Wrong exponent in input data, expected $(scale) got $(exp)")

        tmp[i] = IntT(sign * value)
    end
    chwrite(sock, tmp)
end

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
                                            ::Val{:Decimal32}, scale_str)
    return read_decimal(Dec32, Int32, sock, num_rows, scale_str)
end

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
    ::Val{:Decimal64}, scale_str)
    return read_decimal(Dec64, Int64, sock, num_rows, scale_str)
end

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
    ::Val{:Decimal128}, scale_str)
    return read_decimal(Dec128, Int128, sock, num_rows, scale_str)
end

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
    ::Val{:Decimal}, precision_str,  scale_str)
    return read_col_data(sock, num_rows,
    Val(dec_type_by_preciosion(precision_str)) ,scale_str)
end



function write_col_data(sock::ClickHouseSock,
                    data, ::Val{:Decimal32}, scale_str)

    return write_decimal(Dec32, Int32, sock, data, scale_str)
end

function write_col_data(sock::ClickHouseSock,
    data, ::Val{:Decimal64}, scale_str)

    return write_decimal(Dec64, Int64, sock, data, scale_str)
end

function write_col_data(sock::ClickHouseSock,
    data, ::Val{:Decimal128}, scale_str)

    return write_decimal(Dec128, Int128, sock, data, scale_str)
end

function write_col_data(sock::ClickHouseSock, data,
    ::Val{:Decimal}, precision_str,  scale_str)
    return write_col_data(sock, data,
    Val(dec_type_by_preciosion(precision_str)) ,scale_str)
end