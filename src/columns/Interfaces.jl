is_ch_typename(::Val{N})  where {N} = false
is_ch_typename(str::String)  = is_ch_typename(Val(Symbol(str)))
is_ch_typename(s::Symbol)  = is_ch_typename(Val(s))

function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{N}, args...) where {N}
    throw(ArgumentError(string("Unsupported type ", N, " with arguments: ", args...)))
end

function write_col_data(sock::ClickHouseSock, data::T, ::Val{N}, args...) where {T, N}
    throw(ArgumentError(string("Unsupported write jl type $T into ch type ", N, " with arguments: ", args...)))
end

read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ast::TypeAst) = read_col_data(sock, num_rows, Val(ast.name), ast.args...)

write_col_data(sock::ClickHouseSock, data, ast::TypeAst) = write_col_data(sock, data, Val(ast.name), ast.args...)




function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, typestring::AbstractString)
    read_col_data(sock, num_rows, parse_typestring(typestring))
end

function write_col_data(sock::ClickHouseSock, data,  typestring::AbstractString)
    write_col_data(sock, data, parse_typestring(typestring))
end