#TODO Perhaps we also need stack_trace and nested
struct ClickHouseServerException <: Exception
    code::Int
    name::String
    message::String
end

"""checksum (compressed block hash values) don't match"""
struct ChecksumError <: Exception end