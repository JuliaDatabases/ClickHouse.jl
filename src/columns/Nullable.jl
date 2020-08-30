using UUIDs
is_ch_type(::Val{:Nullable})  = true
can_be_nullable(::Val{:Nullable}) = false

convert_to_missings(data::Vector{T}) where {T} =
                                 convert(Vector{Union{T, Missing}}, data)

convert_to_missings(data::CategoricalVector{T}) where {T} =
                            convert(CategoricalVector{Union{T, Missing}}, data)

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
                                            ::Val{:Nullable}, nest::TypeAst)

    missing_map = chread(sock, Vector{UInt8}, num_rows)
    unmissing = read_col_data(sock, num_rows, nest)
    result = convert_to_missings(unmissing)
    for i in 1:length(missing_map)
        (missing_map[i] == 0x1) && (result[i] = missing)
    end
    return result
end

missing_replacement(::Type{T}) where {T <: Number} = T(0)
missing_replacement(::Type{UUID}) = UUID(0)
missing_replacement(::Type{String}) = ""
uint8_ismissing(v)::UInt8 = ismissing(v) ? 1 : 0

function write_col_data(sock::ClickHouseSock,
                                data::AbstractVector{Union{Missing, T}},
                                ::Val{:Nullable}, nest::TypeAst) where {T}
    !can_be_nullable(nest.name) &&
            error("$(nest.name) cannot be inside Nullable")
    missing_map = uint8_ismissing.(data)
    chwrite(sock, missing_map)
    unmissing = if !any(x -> x > 0, missing_map)
        convert(Vector{T}, data)
    else
        replacement = missing_replacement(T)
        [ismissing(v) ? replacement : v for v in data]
    end

    write_col_data(sock, unmissing, nest)
end

function write_col_data(sock::ClickHouseSock,
                                data::AbstractVector{T},
                                ::Val{:Nullable}, nest::TypeAst) where {T}
    !can_be_nullable(nest.name) &&
            error("$(nest.name) cannot be inside Nullable")

    missing_map = fill(Int8(0), 1:length(data))
    chwrite(sock, missing_map)
    write_col_data(sock, data, nest)
end

function write_col_data(sock::ClickHouseSock,
                                data::AbstractCategoricalVector{Union{Missing, T}},
                                ::Val{:Nullable}, nest::TypeAst) where {T}
    !can_be_nullable(nest.name) &&
            error("$(nest.name) cannot be inside Nullable")
    missing_map = uint8_ismissing.(data)
    chwrite(sock, missing_map)
    unmissing = if !any(x -> x > 0, missing_map)
        convert(CategoricalVector{T}, data)
    else
        tmp = deepcopy(data)
        replace!(tmp.refs, 0=>1)
        convert(CategoricalVector{T}, tmp)
    end

    write_col_data(sock, unmissing, nest)
end