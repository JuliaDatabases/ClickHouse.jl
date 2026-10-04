using Documenter
using ClickHouse

makedocs(
    modules = [ClickHouse],
    sitename = "ClickHouse.jl",
    checkdocs = :exports,
    pagesonly = true,
    format = Documenter.HTML(
        prettyurls = true,
        canonical = "https://juliadatabases.org/ClickHouse.jl/",
        edit_link = "master",
        footer = "Powered by [Documenter.jl](https://documenter.juliadocs.org/). AI disclosure: This work was prepared with assistance from OpenAI Codex.",
    ),
    pages = [
        "index.md",
        "usage.md",
        "api.md",
    ]
)

deploydocs(
    repo = "github.com/JuliaDatabases/ClickHouse.jl.git",
    devbranch = "master",
    versions = nothing,
)