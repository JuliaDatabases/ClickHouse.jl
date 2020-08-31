is_ch_type(::Val{N})  where {N} = false
is_ch_type(str::String)  = is_ch_type(Val(Symbol(str)))
is_ch_type(s::Symbol)  = is_ch_type(Val(s))

can_be_nullable(::Val{N}) where {N} = true
can_be_nullable(s::Symbol) = can_be_nullable(Val(s))

result_type(::Val{N}, args...)  where {N} =
    string("Unsupported type ", N, " with arguments: ", args...)

result_type(ast::TypeAst) =
                result_type(Val(ast.name), ast.args...)

#prefixes writes/reades before any column data.
#For now prefix exists only for LowCardinality columns
write_state_prefix(sock::ClickHouseSock, ::Val{N}, args...) where {N} = nothing
read_state_prefix(sock::ClickHouseSock, ::Val{N}, args...) where {N} = nothing

write_state_prefix(sock::ClickHouseSock, ast::TypeAst) =
                write_state_prefix(sock, Val(ast.name), ast.args...)
read_state_prefix(sock::ClickHouseSock, ast::TypeAst) =
                read_state_prefix(sock, Val(ast.name), ast.args...)

function read_col_data(sock::ClickHouseSock,
                        num_rows::VarUInt, ::Val{N}, args...) where {N}
    throw(
        ArgumentError(
            string("Unsupported type ", N, " with arguments: ", args...)
            )
        )
end

function write_col_data(sock::ClickHouseSock,
                        data::T, ::Val{N}, args...) where {T, N}
    throw(
        ArgumentError(
            string(
                "Unsupported write jl type $T into ch type ",
                N,
                " with arguments: ",
                args...
            )
            )
        )
end

read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ast::TypeAst) =
                read_col_data(sock, num_rows, Val(ast.name), ast.args...)

write_col_data(sock::ClickHouseSock, data, ast::TypeAst) =
                write_col_data(sock, data, Val(ast.name), ast.args...)