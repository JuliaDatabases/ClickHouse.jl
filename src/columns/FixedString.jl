is_ch_type(::Val{:FixedString})  = true
result_type(::Val{:FixedString}, len_str::String)  = Vector{String}

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
                        ::Val{:FixedString}, len_str::String)
    len = parse(Int64, len_str)
    result = Vector{String}(undef, UInt64(num_rows))
    for i in 1:UInt64(num_rows)
        result[i] = String(Base.read(sock.io, len))
    end
    return result
end

function str_to_fix_len(str::String, len)
    length(str) == len && return str
    length(str) > len && error("Too large value \"$str\" for FixedString($len)")
    return str * repeat(" ", len - length(str))
end

function write_col_data(sock::ClickHouseSock, data::AbstractVector{String},
                         ::Val{:FixedString}, len_str::String)
    len = parse(Int64, len_str)
    for str in data
        Base.write(sock.io, str_to_fix_len(str, len))
    end
end