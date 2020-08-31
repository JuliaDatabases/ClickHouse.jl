using UUIDs
is_ch_type(::Val{:LowCardinality})  = true
can_be_nullable(::Val{:LowCardinality}) = false

# Need to read additional keys.
# Additional keys are stored before indexes as value N and N keys
# after them.
const lc_has_additional_keys_bit = 1 << 9
# Need to update dictionary.
# It means that previous granule has different dictionary.
const lc_need_update_dictionary = 1 << 10

const lc_serialization_type = lc_has_additional_keys_bit | lc_need_update_dictionary

const lc_index_int_types = [:UInt8, :UInt16, :UInt32, :UInt64]

categorical_vector_type(::Type{Vector{T}}) where {T} =
                                                 CategoricalVector{T}
categorical_vector_type(::Type{CategoricalVector{T}}) where {T} =
                                                CategoricalVector{T}

result_type(::Val{:LowCardinality}, nested)  =
                             categorical_vector_type(result_type(nested))

function read_state_prefix(sock::ClickHouseSock, ::Val{:LowCardinality}, nested::TypeAst)
    ver = chread(sock, UInt64) # KeysSerializationVersion
    ver == 1 || error("unsupported LC serialization version: $(ver)")
    return ver
end

function write_state_prefix(sock::ClickHouseSock, ::Val{:LowCardinality}, nested::TypeAst)
    # KeysSerializationVersion. See ClickHouse docs.
    chwrite(sock, Int64(1))
end

function make_result(index::Vector{T}, keys, is_nullable) where {T}

    result = is_nullable ?
            CategoricalVector{Union{T, Missing}}(undef, 0, levels = index)  :
            CategoricalVector{T}(undef, 0, levels = index)
    result.refs = keys
    return result
end

function make_result(index::CategoricalVector{T}, keys, is_nullable) where {T}

    result = is_nullable ?
            CategoricalVector{Union{T, Missing}}(undef, 0, levels = get.(index))  :
            CategoricalVector{T}(undef, 0, levels = get.(index))
    result.refs = keys
    return result
end


function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
                                            ::Val{:LowCardinality}, nested::TypeAst)

    UInt64(num_rows)  == 0 && return read_col_data(sock, num_rows, nested)

    is_nested_nullable = (nested.name == :Nullable)
    notnullable_nested = is_nested_nullable ? nested.args[1] : nested

    serialization_type = chread(sock, UInt64)
    int_type = serialization_type & 0xf

    index_size = chread(sock, UInt64)
    index = read_col_data(sock, VarUInt(index_size), notnullable_nested)
    is_nested_nullable && (index = index[2:end])

    keys_size = chread(sock, UInt64)
    keys = read_col_data(sock, VarUInt(keys_size), Val(lc_index_int_types[int_type + 1]))

    (nested.name != :Nullable) && (keys .= keys .+ 1)


    return make_result(index, keys, nested.name == :Nullable)
end


unmissing_type(::Type{Union{Missing, T}}) where {T} = T
function write_col_data(sock::ClickHouseSock,
                                data::AbstractCategoricalVector{T},
                                ::Val{:LowCardinality}, nested::TypeAst) where {T}

    is_nested_nullable = (nested.name == :Nullable)
    notnullable_nested = is_nested_nullable ? nested.args[1] : nested

    isempty(data) && return

    int_type = floor(Int, log2(length(levels(data))) / 2)

    serialization_type = lc_serialization_type | int_type
    chwrite(sock, serialization_type)

    index = is_nested_nullable ?
                    vcat(missing_replacement(unmissing_type(T)), levels(data)) :
                    levels(data)

    chwrite(sock, length(index))
    write_col_data(sock, index, notnullable_nested)

    chwrite(sock, length(data))

    #In c++ indexes started from 0, in case of nullable nested 0 means null and
    # it's ok, but if nested not nullable we must sub 1 from index
    keys = is_nested_nullable ? data.refs : data.refs .- 1
    write_col_data(sock, keys, Val(lc_index_int_types[int_type + 1]))
end

function write_col_data(sock::ClickHouseSock,
                                data::AbstractVector{T},
                                v::Val{:LowCardinality}, nested::TypeAst) where {T}
    write_col_data(sock, CategoricalVector{T}(data), v, nested)
end
