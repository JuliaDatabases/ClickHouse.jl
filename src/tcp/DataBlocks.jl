const BLOCK_INFO_FIELD_STOP = UInt64(0)
const BLOCK_INFO_FIELD_OVERFLOWS = UInt64(1)
const BLOCK_INFO_FIELD_BUCKET_NUM = UInt64(2)

struct BlockInfo
    is_overflows::Bool
    bucket_num::Int32

    BlockInfo() = new(false, -1)
    BlockInfo(is_overflows, bucket_num) = new(is_overflows, bucket_num)
end

function chread(sock::ClickHouseSock, ::Type{BlockInfo})::BlockInfo
    is_overflows = false
    bucket_num = -1

    while (field_num = UInt64(chread(sock, VarUInt))) != BLOCK_INFO_FIELD_STOP
        if field_num == BLOCK_INFO_FIELD_OVERFLOWS
            is_overflows = chread(sock, Bool)
        elseif field_num == BLOCK_INFO_FIELD_BUCKET_NUM
            bucket_num = chread(sock, Int32)
        else
            throw("Unknown block info field")
        end
    end

    BlockInfo(is_overflows, bucket_num)
end

function chwrite(sock::ClickHouseSock, x::BlockInfo)
    # This mirrors what the C++ client does.
    chwrite(sock, VarUInt(BLOCK_INFO_FIELD_OVERFLOWS))
    chwrite(sock, x.is_overflows)
    chwrite(sock, VarUInt(BLOCK_INFO_FIELD_BUCKET_NUM))
    chwrite(sock, x.bucket_num)
    chwrite(sock, VarUInt(BLOCK_INFO_FIELD_STOP))
end

struct Column
    name::String
    type::String
    data::Any
end

Base.:(==)(a::Column, b::Column) =
    a.name == b.name && a.type == b.type && a.data == b.data

# We can't just use chread here because we need the size to be passed
# in from the `Block` decoder that holds the row count.
function read_col(sock::ClickHouseSock, num_rows::VarUInt)::Column
    name = chread(sock, String)
    type_name = chread(sock, String)

    type = parse_typestring(type_name)
    data = if UInt64(num_rows) == 0
        result_type(type)(undef, 0)
    else
        try
            read_state_prefix(sock, type)
            read_col_data(sock, num_rows, type)
        catch e
            if e isa ArgumentError
                error("Error while reading col $(name) ($(type)): $(e.msg)")
            else
                rethrow(e)
            end
        end
    end
    Column(name, type_name, data)
end

function chwrite(sock::ClickHouseSock, x::Column)
    chwrite(sock, x.name)
    chwrite(sock, x.type)

    try
        type = parse_typestring(x.type)
        write_state_prefix(sock, type)
        write_col_data(sock, x.data, type)
    catch e
        if e isa ArgumentError
            error("Error while writing col $(x.name) ($(x.type)): $(e.msg)")
        else
            rethrow(e)
        end
    end
end

struct Block
    temp_table::String
    block_info::BlockInfo
    num_columns::VarUInt
    num_rows::VarUInt
    columns::Array{Column}
end

function chread(sock::ClickHouseSock, ::Type{Block})::Block
    temp_table = chread(sock, String)
    block_info = chread(sock, BlockInfo)
    num_columns = chread(sock, VarUInt)
    num_rows = chread(sock, VarUInt)
    columns = [read_col(sock, num_rows) for _ ∈ 1:UInt64(num_columns)]
    Block(temp_table, block_info, num_columns, num_rows, columns)
end

function chwrite(sock::ClickHouseSock, x::Block)
    chwrite(sock, x.temp_table)
    chwrite(sock, x.block_info)
    chwrite(sock, x.num_columns)
    chwrite(sock, x.num_rows)
    for x ∈ x.columns
        chwrite(sock, x)
    end
end