function _parse_typestring(s::AbstractString)
    s = strip(s)

    (isempty(s)||s[1]=='(') && error("typename parse error in $S")
    brace_pos = findfirst("(", s)
    if isnothing(brace_pos)
        type_name = Symbol(s)
        return is_ch_typename(type_name) ? TypeAst(type_name) : s

    end
    brace_pos = brace_pos[1]
    s[end] != ')' && error("typename parse error in $s")
    type_name = Symbol(strip(s[1:brace_pos-1]))
    !is_ch_typename(type_name) && error("typename parse error in $S")

    ast = TypeAst(type_name)
    inner = SubString(s, brace_pos + 1, length(s) - 1)

    cursor = 1
    elem_pos = 1
    opened_braces = 0
    while true
        range = findnext(r"\(|\)|,", inner, cursor)

        if isnothing(range) || isempty(range)

            if elem_pos <= length(inner)
                push!(ast, _parse_typestring(inner[elem_pos:end]))
            end
            break
        end
        pos = first(range)
        inner[pos] == '(' && (opened_braces += 1)
        inner[pos] == ')' && (opened_braces -= 1)
        if inner[pos] == ',' && opened_braces == 0
            push!(ast, _parse_typestring(inner[elem_pos:pos-1]))
            elem_pos = last(range) + 1
        end
        cursor = last(range) + 1
    end
    return ast
end

function parse_typestring(s::AbstractString)
    result = _parse_typestring(s)
    !(result isa TypeAst) && error("typename parse error in $s")
    return result
end