using Documenter
using ClickHouse

makedocs(
    modules = [ClickHouse],
    sitename = "ClickHouse.jl",
    pages = [
        "index.md",
        "usage.md",
        "api.md",
    ]
)