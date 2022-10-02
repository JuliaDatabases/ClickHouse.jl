using CodecLz4

const BLOCK_INFO_FIELD_STOP = UInt64(0)
const BLOCK_INFO_FIELD_OVERFLOWS = UInt64(1)
const BLOCK_INFO_FIELD_BUCKET_NUM = UInt64(2)

# UInt32 || UInt32 || UInt8 = (4 + 4 + 1)
const HEADER_SIZE_W_COMPRESSION = UInt32(9)


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
    main_io = sock.io
    try
        if compression_enabled(sock.settings)
            hash = chread(sock, UInt128)
            method = chread(sock, Compression)
            compressed = chread(sock, UInt32)
            original = chread(sock, UInt32)  # TODO, not needed?
            comp_data = chread(sock, Vector{UInt8}, VarUInt(compressed - 9))
            decomp_data = decompress(method, comp_data, original)
            sock.io = IOBuffer(decomp_data)
        end

        block_info = chread(sock, BlockInfo)
        num_columns = chread(sock, VarUInt)
        num_rows = chread(sock, VarUInt)
        columns = [read_col(sock, num_rows) for _ ∈ 1:UInt64(num_columns)]
        return Block(temp_table, block_info, num_columns, num_rows, columns)
    finally
        sock.io = main_io
    end
end

function chwrite(sock::ClickHouseSock, x::Block)
    main_io = sock.io
    try
        if compression_enabled(sock)
            sock.io = IOBuffer(read = true, write = true)
        else
            # tmp table's aren't written in the compression block, so they are
            # only written here if we aren't compressing what's about to be on
            # sock.io
            chwrite(sock, x.temp_table)
        end

        chwrite(sock, x.block_info)
        chwrite(sock, x.num_columns)
        chwrite(sock, x.num_rows)
        for x ∈ x.columns
            chwrite(sock, x)
        end

        if compression_enabled(sock)
            # packet:
            #   checksum(packet-inner)               :: UInt128  (1)
            #   packet-inner:
            #       compression method ∈ Compression :: UInt8    (2)
            #       |C(D)| + |header|                :: UInt32   (3)
            #       |D|                              :: UInt32   (4)
            #       C(D)                             :: UInt8[]  (5)

            data = take!(sock.io)
            compressed = compress(sock.settings.compression, data)
            if length(data) > typemax(UInt32) ||
                    length(compressed) > typemax(UInt32)
                throw(DomainError("Block too big"))
            end

            sock.io = IOBuffer(read = true, write = true)
            chwrite(sock, sock.settings.compression)  # (2)
            chwrite(sock, UInt32(length(compressed) + HEADER_SIZE_W_COMPRESSION))  # (3)
            chwrite(sock, UInt32(length(data)))  # (4)
            chwrite(sock, compressed)  # (5)

            block_data = take!(sock.io)  # unroll (2:5) for (1)
            hash = city_hash_128(block_data)  # checksum(packet-inner)
            sock.io = main_io
            chwrite(sock, x.temp_table)
            chwrite(sock, hash)  # (1)
            chwrite(sock, block_data) # (2:5)
        end

    finally
        sock.io = main_io
    end
end