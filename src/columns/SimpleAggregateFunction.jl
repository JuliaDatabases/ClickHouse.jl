is_ch_type(::Val{:SimpleAggregateFunction})  = true
result_type(::Val{:SimpleAggregateFunction},
            aggr_name,
            base_type::TypeAst) = result_type(base_type)

function read_col_data(sock::ClickHouseSock,
                num_rows::VarUInt, ::Val{:SimpleAggregateFunction},
                aggr_name, base_type::TypeAst)

    return read_col_data(sock, num_rows, base_type)
end

function write_col_data(sock::ClickHouseSock,
                data, ::Val{:SimpleAggregateFunction},
                aggr_name, base_type::TypeAst)

    write_col_data(sock, data, base_type)
end