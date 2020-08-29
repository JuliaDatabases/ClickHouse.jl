mutable struct TypeAst
    name ::Symbol
    args ::Vector{Union{TypeAst, String}}
    TypeAst(name::Symbol) = new(name, TypeAst[])
end

Base.push!(a::TypeAst, arg::TypeAst) = Base.push!(a.args, deepcopy(arg))
Base.push!(a::TypeAst, arg::AbstractString) = Base.push!(a.args, String(arg))