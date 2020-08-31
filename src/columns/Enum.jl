is_ch_type(::Val{:Enum8})  = true
is_ch_type(::Val{:Enum16})  = true
result_type(::Val{:Enum8}, args...)  = CategoricalVector{String}
result_type(::Val{:Enum16}, args...)  = CategoricalVector{String}

const ENUM_RE_ARG = r"""

           '((?:(?:[^'])|(?:\\'))+)'
           \s*=\s*
           (-?\d+)
           \s*$
       """x

function make_enum_map(::Type{BaseT}, args...) where {BaseT}
    map = Dict{String, BaseT}()
    for arg in args
        m = match(ENUM_RE_ARG, arg)
        isnothing(m) && error("Wrong enum argument $arg")
        map[m.captures[1]] = parse(BaseT, m.captures[2])
    end
    return map
end

function read_enum_data(sock::ClickHouseSock, num_rows::VarUInt,
                                        ::Type{BaseT}, args...) where {BaseT}
    map = make_enum_map(BaseT, args...)
    levels = collect(keys(map))
    enum_to_level = Dict(zip(values(map), 1:length(map)))
    data = chread(sock, Vector{BaseT}, num_rows)
    result = CategoricalVector{String, BaseT}(undef, 0, levels = levels)
    data .= getindex.(Ref(enum_to_level), data)
    result.refs = data
    return result
end

function write_enum_data(sock::ClickHouseSock, data::AbstractVector{String},
                                        ::Type{BaseT}, args...) where {BaseT}
    map = make_enum_map(BaseT, args...)
    d = Vector{BaseT}(undef, length(data))
    try
        d .= getindex.(Ref(map), data)
    catch exc
        if exc isa KeyError
            error("Value is not a valid enum variant: $(exc.key)")
        end
        rethrow()
    end
    chwrite(sock, d)
end

function write_enum_data(sock::ClickHouseSock, data::CategoricalVector{String},
                                        ::Type{BaseT}, args...) where {BaseT}
    map = make_enum_map(BaseT, args...)
    try

        level_to_enum = getindex.(Ref(map), levels(data))
        d = convert(Vector{BaseT}, getindex.(Ref(level_to_enum), data.refs))
        chwrite(sock, d)

    catch exc
        if exc isa KeyError
            error("Value is not a valid enum variant: $(exc.key)")
        end
        rethrow()
    end


end

read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:Enum8}, args...) =
                     read_enum_data(sock, num_rows, Int8, args...)
read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:Enum16}, args...) =
                    read_enum_data(sock, num_rows, Int16, args...)


write_col_data(sock::ClickHouseSock,
        data::AbstractVector{String},
         ::Val{:Enum8},
         args...) = write_enum_data(sock, data, Int8, args...)

write_col_data(sock::ClickHouseSock,
                data::AbstractVector{String},
                ::Val{:Enum16},
                 args...) = write_enum_data(sock, data, Int16, args...)

write_col_data(sock::ClickHouseSock,
             data::CategoricalVector{String},
             ::Val{:Enum8},
             args...) = write_enum_data(sock, data, Int8, args...)

write_col_data(sock::ClickHouseSock,
            data::CategoricalVector{String},
            ::Val{:Enum16}, args...) =
            write_enum_data(sock, data, Int16, args...)