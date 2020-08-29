macro _primitive_columns(args...)
    funcs = Expr[]
    for arg in args
        arg_string = string(arg)
        push!(funcs, quote is_ch_type(::Val{Symbol($arg_string)}) = true end)
        push!(funcs, quote
            function read_col_data(sock::ClickHouseSock,
                     num_rows::VarUInt,
                    ::Val{Symbol($arg_string)})
                return chread(sock, Vector{$arg}, num_rows)
            end
        end )
        push!(funcs, quote
            function write_col_data(sock::ClickHouseSock,
                data::AbstractVector{$arg},
                ::Val{Symbol($arg_string)})
                return chwrite(sock, data)
            end
        end )
        push!(funcs, quote deserialize(::Val{Symbol($arg_string)}) = $arg end )
    end
    return esc(:($(funcs...),))
end


@_primitive_columns(
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String
)