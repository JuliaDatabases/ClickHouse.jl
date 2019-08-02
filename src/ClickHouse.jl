module ClickHouse

import Base: UInt64
using HTTP

export VarUInt

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

function chread(ctx::ReadCtx, ::Type{String})::String
    len = read(ctx.io, VarUInt)
    read(ctx.io, len) |> String
end

chwrite(ctx::WriteCtx, x::Number)::Int64 = write(ctx.io, x)
chwrite(ctx::WriteCtx, x::Bool)::Int64 = write(ctx.io, x)

function chwrite(ctx::WriteCtx, x::String)::Int64
    chwrite(ctx.io, length(x))
    chwrite(ctx.io, Array{UInt8}(x))
end

# ============================================================================ #
# [Variable length integer]                                                    #
# ============================================================================ #

primitive type VarUInt <: Unsigned 64 end

VarUInt(x::Number) = reinterpret(VarUInt, UInt64(x))
UInt64(x::VarUInt) = reinterpret(UInt64, x)
Base.show(io::IO, x::VarUInt) = print(io, UInt64(x))

function chwrite(ctx::WriteCtx, x::VarUInt)::Int64
    i::Int64 = 1
    mx::UInt64 = x
    while mx >= 0x80
        write(ctx.io, UInt8(mx & 0xFF) | 0x80)
        mx >>= 7
        i += 1
    end
    write(ctx.io, UInt8(mx & 0xFF))
    i
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

function impl_chread_for_ty(ztruct::Type)::Function
    arg_exprs = [:(chread(ctx, $ty)) for ty ∈ ztruct.types]
    reader = quote
        function chread(ctx::ReadCtx, ::Type{$(ztruct)})::$(ztruct.name)
            $ztruct($(arg_exprs...))
        end
    end
    eval(reader)
end

function impl_chwrite_for_ty(ztruct::Type)::Function
    write_stmts = [:(chwrite(ctx, x)) for name ∈ fieldnames(ztruct)]
    writer = quote
        function chwrite(ctx::WriteCtx, x::$(ztruct))::Int64
            $(write_stmts...)
        end
    end
    eval(writer)
end

# ============================================================================ #
# [Server messages (wire format)]                                              #
# ============================================================================ #

struct ServerInfo
    name::String
    major_ver::VarUInt
    minor_ver::VarUInt
    rev::VarUInt
    timezone::String
end

impl_chread_for_ty(ServerInfo)

struct ServerPong
end

impl_chread_for_ty(ServerPong)

struct ServerProgress
    rows::VarUInt
    bytes::VarUInt
    total_rows::VarUInt
end

impl_chread_for_ty(ServerProgress)

struct ServerProfileInfo
    rows::VarUInt
    blocks::VarUInt
    bytes::VarUInt
    applied_limit::VarUInt
    rows_before_limit::VarUInt
    calc_rows_before_limit::VarUInt
end

impl_chread_for_ty(ServerProfileInfo)

struct ServerException
    rows::UInt32
    name::String
    message::String
    strack_trace::String
end

impl_chread_for_ty(ServerException)

struct ServerBlockInfo
    unk1::VarUInt
    is_overflows::Bool
    unk2::VarUInt
    bucket_num::Int32
    unk3::VarUInt
end

impl_chread_for_ty(ServerBlockInfo)

struct ServerEndOfStream
end

impl_chread_for_ty(ServerEndOfStream)

struct ServerColumn
    name::String
    type_name::String
    data::Any
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
)

function chread(ctx::ReadCtx, ::Type{ServerColumn})::ServerColumn
    name = chread(ctx.io, String)
    type_name = chread(ctx.io, String)
    ty = COL_TY_MAP[type_name]
    data = [read(ctx.io, ty) for _ ∈ 1:size]
    ServerColumn(name, type_name, data)
end

struct ServerBlock
    info::ServerBlockInfo
    num_cols::VarUInt
    num_rows::VarUInt
    cols::Array{ServerColumn}
end

function chread(ctx::ReadCtx, ::Type{ServerBlock})::ServerBlock
    something = chread(ctx.io, String)
    @show something
    @assert !ctx.compress "Compression unsupported"
    info = chread(ctx.io, ServerBlockInfo)
    num_cols = chread(ctx.io, VarUInt)
    num_rows = chread(ctx.io, VarUInt)
    cols = [chread(ctx, ServerColumn) for _ ∈ 1:num_cols]
    ServerBlock(info, num_cols, num_rows, cols)
end

# ============================================================================ #
# [Client messages (wire format)]                                              #
# ============================================================================ #

struct ClientHello
    database::String
    username::String
    password::String
end

impl_chwrite_for_ty(ClientHello)

struct ClientPing
end

impl_chwrite_for_ty(ClientPing)

struct ClientInfo
    name::String
    dbms_ver_minor::VarUInt
    dbms_ver_major::VarUInt
    dbms_ver_rev::VarUInt
end

impl_chwrite_for_ty(ClientInfo)

struct ClientData
    temp_table::String
    chunks::Any # TODO
end

impl_chwrite_for_ty(ClientData)

struct ClientQuery
    unk1::String
    unk2::VarUInt
    unk3::String
    initial_query_id::String
    iface_type::VarUInt
    hostname1::String
    hostname2::String
    client_info::ClientInfo
    quota_key::String # new in rev 54060
    settings::String
    compress_enable::Bool
    sql::String
    data::ClientData
end

impl_chwrite_for_ty(ClientQuery)

# ============================================================================ #
# [Opcodes]                                                                    #
# ============================================================================ #

const CLIENT_HELLO = UInt64(0)
const CLIENT_QUERY = UInt64(1)
const CLIENT_DATA = UInt64(2)
const CLIENT_PING = UInt64(4)

const SERVER_HELLO = UInt64(0)
const SERVER_DATA = UInt64(1)
const SERVER_EXCEPTION = UInt64(2)
const SERVER_PROGRESS = UInt64(3)
const SERVER_PONG = UInt64(4)
const SERVER_END_OF_STREAM = UInt64(5)
const SERVER_PROFILE_INFO = UInt64(6)
const SERVER_TOTALS = UInt64(7)
const SERVER_EXTREMES = UInt64(8)

# ============================================================================ #
# [Message decoding]                                                           #
# ============================================================================ #

const OPCODE_TY_MAP = Dict(
    SERVER_HELLO => ServerInfo,
    SERVER_PONG => ServerPong,
    SERVER_PROGRESS => ServerPong,
    SERVER_PROFILE_INFO => ServerProfileInfo,
    SERVER_EXCEPTION => ServerException,
    SERVER_DATA => ServerBlockInfo,
    SERVER_TOTALS => ServerBlockInfo,
    SERVER_EXTREMES => ServerBlockInfo,
    SERVER_END_OF_STREAM => ServerEndOfStream,
)

function read_packet(io::IO)::Any
    ctx = ReadCtx(io, false)
    opcode = chread(ctx, VarUInt)
    ty = OPCODE_TY_MAP[opcode]
    chread(ctx, ty)
end

# ============================================================================ #
# [Message encoding]                                                           #
# ============================================================================ #

const TY_OPCODE_MAP = Dict(
    ClientHello => CLIENT_HELLO,
    ClientData => CLIENT_DATA,
    ClientPing => CLIENT_PING,
    ClientQuery => CLIENT_QUERY,
)

function write_packet(io::IO, packet::Any)
    ctx = WriteCtx(io, false)
    opcode = TY_OPCODE_MAP[typeof(packet)]
    chwrite(ctx, VarUInt(opcode))
    chwrite(ctx, packet)
end

# ============================================================================ #

end # module