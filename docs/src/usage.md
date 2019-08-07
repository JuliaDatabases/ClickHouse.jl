# Usage

## Executing DDL

### Creating a table
```julia
using ClickHouse
con = connect("localhost", 9000)

execute(con, """
    CREATE TABLE MyTable
        (u UInt64, f Float32, s String)
    ENGINE = Memory
""")
```

## Inserting data
```julia
insert(con, "MyTable", [Dict(
    :u => UInt64[42, 1337, 123],
    :f => Float32[0., ℯ, π],
    :s => String["aa", "bb", "cc"],
)])
```

## Selecting data

### ... into a dict of `(column, data)` pairs

```julia
select(con, "SELECT * FROM MyTable")
```

```
Dict{Symbol,Any} with 3 entries:
  :f => Float32[0.0, 2.71828, 3.14159]
  :s => ["aa", "bb", "cc"]
  :u => UInt64[0x000000000000002a, 0x0000000000000539, 0x000000000000007b]
```

### ... into a DataFrame
```julia
select_df(con, "SELECT * FROM MyTable")
```

```
3×3 DataFrames.DataFrame
│ Row │ f       │ s      │ u                  │
│     │ Float32 │ String │ UInt64             │
├─────┼─────────┼────────┼────────────────────┤
│ 1   │ 0.0     │ aa     │ 0x000000000000002a │
│ 2   │ 2.71828 │ bb     │ 0x0000000000000539 │
│ 3   │ 3.14159 │ cc     │ 0x000000000000007b │
```

### ... streaming through a channel
```julia
ch = select_channel(con, "SELECT * FROM MyTable LIMIT 1")
for block in ch
    @show block
end
```

```
block = Dict{Symbol,Any}(
    :f => Float32[0.0],
    :s => ["aa"],
    :u => UInt64[0x000000000000002a],
)
```