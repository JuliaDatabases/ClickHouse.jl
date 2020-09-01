is_ch_type(::Val{:Array})  = true
can_be_nullable(::Val{:Array}) = false
result_type(::Val{:Array}, nested::TypeAst) = Vector{result_type(nested)}

"""
    Nested arrays written in flatten form after information about their
    sizes (offsets really).
    One element of array of arrays can be represented as tree:
    (0 depth)          [[3, 4], [5, 6]]
                      |               |
    (1 depth)      [3, 4]           [5, 6]
                   |    |           |    |
    (leaf)        3     4          5     6

    Offsets (sizes) written in breadth-first search order. In example above
    following sequence of offset will be written: 4 -> 2 -> 4
    1) size of whole array: 4
    2) size of array 1 in depth=1: 2
    3) size of array 2 plus size of all array before in depth=1: 2 + 2 = 4

    After sizes info com
"""

read_state_prefix(sock::ClickHouseSock, ::Val{:Array}, nested::TypeAst) =
                    read_state_prefix(sock, nested)

write_state_prefix(sock::ClickHouseSock, ::Val{:Array}, nested::TypeAst) =
                write_state_prefix(sock, nested)

function read_offsets!(dest::Vector{Vector{UInt64}}, sock, nest::TypeAst)
    prev_level = dest[end]
    new_level = Vector{UInt64}()
    last_offset = 0
    for offset in prev_level
        for i in 1:(offset - last_offset)
            push!(new_level, chread(sock, UInt64))
        end
        last_offset = offset
    end
    push!(dest, new_level)

    return nest.name == :Array ?
        read_offsets!(dest, sock, nest.args[1]) :
        nest

end

function split_vector(data::T, offsets) where {T <: AbstractVector}
    result = Vector{T}(undef, length(offsets))
    last_offset = 1
    for (i,offset) in enumerate(offsets)
        result[i] = data[last_offset:offset]
        last_offset = offset + 1
    end
    return result
end

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt,
                                            ::Val{:Array}, nest::TypeAst)

    (UInt64(num_rows) == 0) && return
    prev_offset = 0
    cur_deph = 0

    size = UInt64(num_rows)

    offsets = [UInt64[size]]

    data_type = read_offsets!(offsets, sock, nest)

    data_size = offsets[end][end]

    data = read_col_data(sock, VarUInt(data_size), data_type)

    result = data
    #top offset is the size of column, so we don't take it into account
    for off in Iterators.reverse(offsets[2:end])
        result = split_vector(result, off)
    end
    return result
end

const PossibleVectors{T} =
     Union{<:AbstractVector{T}, <:AbstractCategoricalVector{T}}
function get_base_type(
        ::Type{<:AbstractVector{T}},
        nest ::TypeAst
        ) where {T}
    nest.name == :Array && return get_base_type(T, nest.args[1])
    return (T, nest)
end
function flatten_array!(offsets, itr, nest)
    new_level = UInt64[]
    last_offset = 0
    for part in itr
        push!(new_level, last_offset + length(part))
        last_offset = new_level[end]
    end
    push!(offsets, new_level)
    (nest.name != :Array) && return (Iterators.flatten(itr), nest)
    return flatten_array!(offsets, Iterators.flatten(itr), nest.args[1])

end
function write_col_data(sock::ClickHouseSock,
                                data::AbstractVector{T},
                                ::Val{:Array}, nest::TypeAst) where {T}

    offsets = [[UInt64(length(data))]]
    flatten_itr, base_ast = flatten_array!(offsets, data, nest)
    for i in 2:length(offsets)
        chwrite(sock, offsets[i])
    end
    write_col_data(sock, collect(flatten_itr), base_ast)
end
