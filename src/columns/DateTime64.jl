is_ch_type(::Val{:DateTime64})  = true
result_type(::Val{:DateTime64}, precission_str)  = Vector{DateTime}


function read_col_data(sock::ClickHouseSock, num_rows::VarUInt, ::Val{:DateTime64},
    precission_str::String)
    precission = parse(Int, precission_str)
    data = chread(sock, Vector{Int64}, num_rows)

    #Maximum precission of DateTime is milliseconds
    datetime_muliplier =10^3 / 10^precission

    return DateTime(1970) .+ Millisecond.(round.(Int64, data .* datetime_muliplier))
end

function write_col_data(sock::ClickHouseSock,
                            data::AbstractVector{DateTime}, ::Val{:DateTime64},
                            precission_str::String
                            )

    precission = parse(Int, precission_str)

    #Maximum precission of DateTime is milliseconds
    datetime_muliplier =10^3 / 10^precission


    d = round.(Int64,
        getproperty.(
            Millisecond.(data .- DateTime(1970)), :value
        ) ./ datetime_muliplier
    )
    chwrite(sock, d)
end