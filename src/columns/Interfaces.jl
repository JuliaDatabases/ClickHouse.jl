is_ch_type(::Val{N})  where {N} = false
is_ch_type(str::String)  = is_ch_type(Val(Symbol(str)))
is_ch_type(s::Symbol)  = is_ch_type(Val(s))

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