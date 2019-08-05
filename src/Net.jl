import Base: UInt64, convert

# ============================================================================ #
# [Context structs]                                                            #
# ============================================================================ #

struct ReadCtx
    io::IO
    compress::Bool
end

struct WriteCtx
    io::IO
    compress::Bool
end

# ============================================================================ #
# [ch{read,write} impls for primitive types]                                   #
# ============================================================================ #

chread(ctx::ReadCtx, ::Type{T}) where T <: Number = read(ctx.io, T)
chread(ctx::ReadCtx, ::Type{Bool})::Bool = read(ctx.io, Bool)
chread(ctx::ReadCtx, x::UInt64)::Array{UInt8, 1} = read(ctx.io, x)

function chread(ctx::ReadCtx, ::Type{String})::String
    len = chread(ctx, VarUInt) |> UInt64
    chread(ctx, len) |> String
end

chwrite(ctx::WriteCtx, x::Number) = write(ctx.io, x)
chwrite(ctx::WriteCtx, x::Bool) = write(ctx.io, x)
chwrite(ctx::WriteCtx, x::Array{UInt8, 1}) = write(ctx.io, x)

function chwrite(ctx::WriteCtx, x::String)
    chwrite(ctx, x |> length |> VarUInt)
    chwrite(ctx, x |> Array{UInt8})
end

# ============================================================================ #
# [Variable length integer]                                                    #
# ============================================================================ #

primitive type VarUInt <: Unsigned 64 end

VarUInt(x::Number) = reinterpret(VarUInt, UInt64(x))
UInt64(x::VarUInt) = reinterpret(UInt64, x)
Base.show(io::IO, x::VarUInt) = print(io, UInt64(x))

function chwrite(ctx::WriteCtx, x::VarUInt)
    mx::UInt64 = x
    while mx >= 0x80
        write(ctx.io, UInt8(mx & 0xFF) | 0x80)
        mx >>= 7
    end
    write(ctx.io, UInt8(mx & 0xFF))
end

function chread(ctx::ReadCtx, ::Type{VarUInt})::VarUInt
    x::UInt64 = 0
    s::UInt32 = 0
    i::UInt64 = 0
    while true
        b = read(ctx.io, UInt8)
        if b < 0x80
            if i > 9 || (i == 9 && b > 1)
                throw(OverflowError("varint would overflow"))
            end
            return x | UInt64(b) << s
        end

        x |= UInt64(b & 0x7F) << s
        s += 7
        i += 1
    end
end

# ============================================================================ #
# [Parse helpers]                                                              #
# ============================================================================ #

function impl_chread_for_ty(ty::Type)::Function
    arg_exprs = [:(chread(ctx, $ty)) for ty ∈ ty.types]
    sym = split(ty.name |> string, '.')[end] |> Symbol
    reader = quote
        function chread(ctx::ReadCtx, ::Type{$sym})::$sym
            $ty($(arg_exprs...))
        end
    end
    eval(reader)
end

function impl_chwrite_for_ty(ty::Type)::Function
    write_stmts = [:(chwrite(ctx, x.$name)) for name ∈ fieldnames(ty)]
    writer = quote
        function chwrite(ctx::WriteCtx, x::$(ty))
            $(write_stmts...)
        end
    end
    eval(writer)
end

# ============================================================================ #
# [Shared messages (wire format)]                                              #
# ============================================================================ #

const BLOCK_INFO_FIELD_STOP = UInt64(0)
const BLOCK_INFO_FIELD_OVERFLOWS = UInt64(1)
const BLOCK_INFO_FIELD_BUCKET_NUM = UInt64(2)

struct BlockInfo
    is_overflows::Bool
    bucket_num::Int32

    BlockInfo() = new(false, -1)
    BlockInfo(is_overflows, bucket_num) = new(is_overflows, bucket_num)
end

function chread(ctx::ReadCtx, ::Type{BlockInfo})::BlockInfo
    is_overflows = false
    bucket_num = -1

    while (field_num = UInt64(chread(ctx, VarUInt))) != BLOCK_INFO_FIELD_STOP
        if field_num == BLOCK_INFO_FIELD_OVERFLOWS
            is_overflows = chread(ctx, Bool)
        elseif field_num == BLOCK_INFO_FIELD_BUCKET_NUM
            bucket_num = chread(ctx, Int32)
        else
            throw("Unknown block info field")
        end
    end

    BlockInfo(is_overflows, bucket_num)
end

function chwrite(ctx::WriteCtx, x::BlockInfo)
    # This mirrors what the C++ client does.
    chwrite(ctx, VarUInt(BLOCK_INFO_FIELD_OVERFLOWS))
    chwrite(ctx, x.is_overflows)
    chwrite(ctx, VarUInt(BLOCK_INFO_FIELD_BUCKET_NUM))
    chwrite(ctx, x.bucket_num)
    chwrite(ctx, VarUInt(BLOCK_INFO_FIELD_STOP))
end

struct Column
    name::String
    type::String
    data::Any
end

struct Block
    temp_table::String
    block_info::BlockInfo
    num_columns::VarUInt
    num_rows::VarUInt
    columns::Array{Column}
end

const COL_TY_MAP = Dict(
    # Unsigned
    "UInt8" => UInt8,
    "UInt16" => UInt16,
    "UInt32" => UInt32,
    "UInt64" => UInt64,

    # Signed
    "Int8" => Int8,
    "Int16" => Int16,
    "Int32" => Int32,
    "Int64" => Int64,

    "DateTime" => UInt8,
    "String" => String,
)

const COL_TY_REV_MAP = Dict(v => k for (k, v) ∈ COL_TY_MAP)

function read_col(ctx::ReadCtx, num_rows::VarUInt)::Column
    name = chread(ctx, String)
    type = chread(ctx, String)
    ty = COL_TY_MAP[type]
    data = [chread(ctx, ty) for _ ∈ 1:UInt64(num_rows)]
    Column(name, type, data)
end

function chwrite(ctx::WriteCtx, x::Column)
    chwrite(ctx, x.name)
    chwrite(ctx, x.type)
    for x ∈ x.data
        chwrite(ctx, x)
    end
end

function chread(ctx::ReadCtx, ::Type{Block})::Block
    temp_table = chread(ctx, String)
    block_info = chread(ctx, BlockInfo)
    num_columns = chread(ctx, VarUInt)
    num_rows = chread(ctx, VarUInt)
    columns = [read_col(ctx, num_rows) for _ ∈ 1:UInt64(num_columns)]
    Block(temp_table, block_info, num_columns, num_rows, columns)
end

function chwrite(ctx::WriteCtx, x::Block)
    chwrite(ctx, x.temp_table)
    chwrite(ctx, x.block_info)
    chwrite(ctx, x.num_columns)
    chwrite(ctx, x.num_rows)
    for x ∈ x.columns
        chwrite(ctx, x)
    end
end

# ============================================================================ #
# [Server messages (wire format)]                                              #
# ============================================================================ #

struct ServerInfo
    server_name::String
    server_major_ver::VarUInt
    server_minor_ver::VarUInt
    server_rev::VarUInt

    # DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
    server_timezone::String

    # DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
    server_display_name::String

    # DBMS_MIN_REVISION_WITH_VERSION_PATCH
    server_version_patch::VarUInt
end

impl_chread_for_ty(ServerInfo)

struct ServerPong
end

impl_chread_for_ty(ServerPong)

struct ServerProgress
    rows::VarUInt
    bytes::VarUInt
    total_rows::VarUInt

    # DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
    written_rows::VarUInt
    written_bytes::VarUInt
end

impl_chread_for_ty(ServerProgress)

struct ServerProfileInfo
    rows::VarUInt
    blocks::VarUInt
    bytes::VarUInt
    applied_limit::Bool
    rows_before_limit::VarUInt
    calc_rows_before_limit::Bool
end

impl_chread_for_ty(ServerProfileInfo)

struct ServerException
    rows::UInt32
    name::String
    message::String
    strack_trace::String
end

impl_chread_for_ty(ServerException)

struct ServerEndOfStream
end

impl_chread_for_ty(ServerEndOfStream)

# ============================================================================ #
# [Client messages (wire format)]                                              #
# ============================================================================ #

struct ClientHello
    client_name::String
    client_dbms_ver_major::VarUInt
    client_dbms_ver_minor::VarUInt
    client_dbms_ver_rev::VarUInt
    database::String
    username::String
    password::String
end

impl_chwrite_for_ty(ClientHello)
impl_chread_for_ty(ClientHello)

struct ClientPing
end

impl_chwrite_for_ty(ClientPing)
impl_chread_for_ty(ClientPing)

struct ClientInfo
    query_kind::UInt8
    initial_user::String
    initial_query_id::String
    initial_address_string::String
    read_interface::UInt8
    os_user::String
    client_hostname::String
    client_name::String
    client_ver_major::VarUInt
    client_ver_minor::VarUInt
    client_rev::VarUInt
    quota_key::String # DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
    client_ver_patch::VarUInt # DBMS_MIN_REVISION_WITH_VERSION_PATCH
end

impl_chwrite_for_ty(ClientInfo)
impl_chread_for_ty(ClientInfo)

struct ClientQuery
    query_id::String
    client_info::ClientInfo
    settings::String
    query_stage::VarUInt
    compression::VarUInt
    query::String
end

impl_chwrite_for_ty(ClientQuery)
impl_chread_for_ty(ClientQuery)

# ============================================================================ #
# [Opcodes]                                                                    #
# ============================================================================ #

const CLIENT_HELLO = UInt64(0)
const CLIENT_QUERY = UInt64(1)
const CLIENT_DATA = UInt64(2)
const CLIENT_CANCEL = UInt64(3)
const CLIENT_PING = UInt64(4)
const CLIENT_TABLE_STATUS_REQ = UInt64(5)
const CLIENT_KEEP_ALIVE = UInt64(6)

const SERVER_HELLO = UInt64(0)
const SERVER_DATA = UInt64(1)
const SERVER_EXCEPTION = UInt64(2)
const SERVER_PROGRESS = UInt64(3)
const SERVER_PONG = UInt64(4)
const SERVER_END_OF_STREAM = UInt64(5)
const SERVER_PROFILE_INFO = UInt64(6)
const SERVER_TOTALS = UInt64(7)
const SERVER_EXTREMES = UInt64(8)
const SERVER_TABLES_STATUS_REPORT = UInt64(9)
const SERVER_TABLES_LOG = UInt64(10)
const SERVER_TABLE_COLUMNS = UInt64(11)

# ============================================================================ #
# [Message decoding]                                                           #
# ============================================================================ #

const SERVER_OPCODE_TY_MAP = Dict(
    SERVER_HELLO => ServerInfo,
    SERVER_PONG => ServerPong,
    SERVER_PROGRESS => ServerProgress,
    SERVER_PROFILE_INFO => ServerProfileInfo,
    SERVER_EXCEPTION => ServerException,
    SERVER_DATA => Block,
    SERVER_TOTALS => Block,
    SERVER_EXTREMES => Block,
    SERVER_END_OF_STREAM => ServerEndOfStream,
)

const CLIENT_OPCODE_TY_MAP = Dict(
    CLIENT_HELLO => ClientHello,
    CLIENT_QUERY => ClientQuery,
    CLIENT_DATA => Block,
    CLIENT_PING => ClientPing,
)

function read_packet(io::IO, opcode_map::Dict{UInt64, DataType})::Any
    ctx = ReadCtx(io, false)
    opcode = chread(ctx, VarUInt)
    ty = opcode_map[UInt64(opcode)]
    chread(ctx, ty)
end

read_server_packet(io::IO)::Any = read_packet(io, SERVER_OPCODE_TY_MAP)
read_client_packet(io::IO)::Any = read_packet(io, CLIENT_OPCODE_TY_MAP)

# ============================================================================ #
# [Message encoding]                                                           #
# ============================================================================ #

const CLIENT_TY_OPCODE_MAP = Dict(v => k for (k, v) ∈ CLIENT_OPCODE_TY_MAP)

function write_packet(io::IO, packet::Any)
    ctx = WriteCtx(io, false)
    opcode = CLIENT_TY_OPCODE_MAP[typeof(packet)]
    chwrite(ctx, VarUInt(opcode))
    chwrite(ctx, packet)
end

# ============================================================================ #
