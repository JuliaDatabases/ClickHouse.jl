# Usage

```@meta
DocTestSetup = quote
    using ClickHouse
end
```

## Executing DDL

### Creating a table
```jldoctest
execute(connect(), """
    CREATE TABLE IF NOT EXISTS MyTable
        (u UInt64, f Float32, s String)
    ENGINE = Memory
""")

# output

```

## Inserting data
```jldoctest
insert(connect(), "MyTable", [Dict(
    :u => UInt64[42, 1337, 123],
    :f => Float32[0., ℯ, π],
    :s => String["aa", "bb", "cc"],
)])

# output

```

## Selecting data

### ... into a dict of `(column, data)` pairs

```jldoctest
select(connect(), "SELECT * FROM MyTable LIMIT 3")

# output

Dict{Symbol,Any} with 3 entries:
  :f => Float32[0.0, 2.71828, 3.14159]
  :s => ["aa", "bb", "cc"]
  :u => UInt64[0x000000000000002a, 0x0000000000000539, 0x000000000000007b]
```

### ... into a DataFrame
```jldoctest
select_df(connect(), "SELECT * FROM MyTable LIMIT 3")

# output

3×3 DataFrame
│ Row │ f       │ s      │ u                  │
│     │ Float32 │ String │ UInt64             │
├─────┼─────────┼────────┼────────────────────┤
│ 1   │ 0.0     │ aa     │ 0x000000000000002a │
│ 2   │ 2.71828 │ bb     │ 0x0000000000000539 │
│ 3   │ 3.14159 │ cc     │ 0x000000000000007b │
```

### ... streaming through a channel
```jldoctest
ch = select_channel(connect(), "SELECT * FROM MyTable LIMIT 1")
for block in ch
    @show block
end

# output

block = Dict{Symbol,Any}(:f => Float32[0.0],:s => ["aa"],:u => UInt64[0x000000000000002a])
```

### ... streaming each block into a callback

This is the fastest way to stream blocks and is used under the hood
to implement all other `select_xyz` implementations.

```jldoctest
select_callback(connect(), "SELECT * FROM MyTable LIMIT 1") do block
    @show block
end

# output

block = Dict{Symbol,Any}(:f => Float32[0.0],:s => ["aa"],:u => UInt64[0x000000000000002a])
```