""" @ch_struct

Macro for defining CH read/writable structs with the support for 
restricting fields by server revision.
"""
macro ch_struct(expr)
    expr isa Expr && expr.head === :struct || error("Invalid usage of @ch_struct")

    struct_name = expr.args[2]
    cleared_block = Expr(:block)
    args = parse_struct_args!(expr.args[3], cleared_block)
    args_names = []

    expr.args[3] = cleared_block
    reader = _make_reader(struct_name, args)
    writer = _make_writer(struct_name, args)
    return Expr(:block, expr, esc(reader), esc(writer))
end

function _make_reader(struct_name, args)

    :(function chread(sock::ClickHouseSock, ::Type{$struct_name})
        $(_field_reader.(args)...)
        return $struct_name($(
                _arg_name.(
                    getindex.(args, 1)
                )...
            )
            )
    end)
end
function _make_writer(struct_name, args)

    :(function chwrite(sock::ClickHouseSock, packet::$struct_name)
        $(_field_writer.(args)...)
    end)

end

function _field_reader(arg_tuple)
    (arg, check) = arg_tuple
    if isnothing(check)
        :($(_arg_name(arg)) = chread(sock, $(_arg_type(arg))))
    else
        :(
            $(_arg_name(arg)) = $(check)(sock.server_rev) ?
                chread(sock, $(_arg_type(arg))) :
                $(_arg_def(arg))
        )
    end
end
function _field_writer(arg_tuple)
    (arg, check) = arg_tuple
    if isnothing(check)
        :(chwrite(sock, packet.$(_arg_name(arg))))
    else
        :(
            if $(check)(sock.server_rev)
                chwrite(sock, packet.$(_arg_name(arg)))
            end
        )
    end
end


_arg_name(arg) = arg.head == :(::) ?
        arg.args[1] :
        arg.args[1].args[1]

_arg_type(arg) = arg.head == :(::) ?
        arg.args[2] :
        arg.args[1].args[2]

_arg_def(arg) = arg.head == :(::) ?
        nothing :
        arg.args[2]


function parse_struct_args!(expr, cleared_expr)
    result = []
    last_rev::Int = 0

    for field in expr.args
        !isa(field, Expr) && continue
        arg::Union{Expr, Nothing} = nothing
        check::Union{Symbol, Nothing} = nothing

        if field.head == :(::)
           arg = field
           push!(cleared_expr.args, arg)
        elseif field.head == :macrocall
            check = Symbol(string(field.args[1])[2:end])
            arg = field.args[3]
            arg.head != :(=) &&
                error("Version restricted fields must have default value")
            arg.head = :kw
            push!(cleared_expr.args, arg.args[1]) #remove macrocall and default value
        else
            error("Invalid usage of @ch_struct")
        end
        push!(result, (
            arg = arg,
            check = check
        ))
    end
    return result
end


macro reg_packet(code, packet_struct)
    result = Expr(:block)
    push!(
        result.args,
        esc(
            :(
                packet_struct(::Val{$code}) = $packet_struct
            )
        )
    )
    push!(
        result.args,
        esc(
            :(
                packet_code(::Type{$packet_struct}) = $code
            )
        )
    )
    return result
end
