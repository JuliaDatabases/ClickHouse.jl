#TODO Perhaps we also need stack_trace and nested
struct ClickHouseServerException <: Exception
    code::Int
    name::String
    message::String
end