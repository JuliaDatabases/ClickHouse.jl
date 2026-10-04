#TODO Perhaps we also need stack_trace and nested
"""
An error reported by the ClickHouse server.
Its fields are an integer `code` and strings `name` and `message`.
"""
struct ClickHouseServerException <: Exception
    code::Int
    name::String
    message::String
end

"""checksum (compressed block hash values) don't match"""
struct ChecksumError <: Exception end