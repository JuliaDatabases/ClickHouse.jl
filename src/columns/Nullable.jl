using UUIDs
is_ch_type(::Val{:Nullable})  = true
can_be_nullable(::Val{:Nullable}) = false

missings_vector_type(::Type{Vector{T}}) where {T} =
                                                 Vector{Union{T, Missing}}
missings_vector_type(::Type{CategoricalVector{T}}) where {T} =
                                                CategoricalVector{Union{T, Missing}}

result_type(::Val{:Nullable}, nested)  = missings_vector_type(result_type(nested))

convert_to_missings(data::Vector{T}) where {T} =
                                 convert(Vector{Union{T, Missing}}, data)

convert_to_missings(data::CategoricalVector{T}) where {T} =
                            convert(CategoricalVector{Union{T, Missing}}, data)

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
                                            ::Val{:Nullable}, nested::TypeAst)

    missing_map = chread(sock, Vector{UInt8}, num_rows)
    unmissing = read_col_data(sock, num_rows, nested)
    result = convert_to_missings(unmissing)
    for i in 1:length(missing_map)
        (missing_map[i] == 0x1) && (result[i] = missing)
    end
    return result
end

missing_replacement(::Type{T}) where {T <: Number} = zero(T)
missing_replacement(::Type{UUID}) = UUID(0)
missing_replacement(::Type{Date}) = Date(1970)
missing_replacement(::Type{DateTime}) = unix2datetime(0)
missing_replacement(::Type{String}) = ""



uint8_ismissing(v)::UInt8 = ismissing(v) ? 1 : 0

function write_col_data(sock::ClickHouseSock,
                                data::AbstractVector{Union{Missing, T}},
                                ::Val{:Nullable}, nested::TypeAst) where {T}
    !can_be_nullable(nested.name) &&
            error("$(nested.name) cannot be inside Nullable")
    missing_map = uint8_ismissing.(data)
    chwrite(sock, missing_map)
    unmissing = if !any(x -> x > 0, missing_map)
        convert(Vector{T}, data)
    else
        replacement = missing_replacement(T)
        [ismissing(v) ? replacement : v for v in data]
    end

    write_col_data(sock, unmissing, nested)
end

function write_col_data(sock::ClickHouseSock,
                                data::AbstractVector{T},
                                ::Val{:Nullable}, nested::TypeAst) where {T}
    !can_be_nullable(nested.name) &&
            error("$(nested.name) cannot be inside Nullable")

    missing_map = fill(Int8(0), 1:length(data))
    chwrite(sock, missing_map)
    write_col_data(sock, data, nested)
end

function write_col_data(sock::ClickHouseSock,
                                data::AbstractCategoricalVector{Union{Missing, T}},
                                ::Val{:Nullable}, nested::TypeAst) where {T}
    !can_be_nullable(nested.name) &&
            error("$(nested.name) cannot be inside Nullable")
    missing_map = uint8_ismissing.(data)
    chwrite(sock, missing_map)
    unmissing = if !any(x -> x > 0, missing_map)
        convert(CategoricalVector{T}, data)
    else
        tmp = deepcopy(data)
        #replace missing (it's always 0 in refs of CategorialVector)
        #with something valid
        replace!(tmp.refs, 0=>1)
        convert(CategoricalVector{T}, tmp)
    end

    write_col_data(sock, unmissing, nested)
end

#For Nothing columns,
#(i.e. column of nulls in CHtypes of CH, of missing column in Julia types)
function write_col_data(sock::ClickHouseSock,
                                data::AbstractVector{Missing},
                                ::Val{:Nullable}, nested::TypeAst) where {T}
    nested.name != :Nothing &&
        error("Vector{Missing} can be writen only to Nullable(Nothing) column")
    missing_map = Vector{UInt8}(undef, length(data))
    fill!(missing_map, 0x1)
    chwrite(sock, missing_map)
    write_col_data(sock, data, nested)
end