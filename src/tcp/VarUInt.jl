# ============================================================================ #
# [Variable length integer]                                                    #
# ============================================================================ #

primitive type VarUInt <: Unsigned 64 end

VarUInt(x::Number) = reinterpret(VarUInt, UInt64(x))
Base.UInt64(x::VarUInt) = reinterpret(UInt64, x)
Base.show(io::IO, x::VarUInt) = print(io, UInt64(x))