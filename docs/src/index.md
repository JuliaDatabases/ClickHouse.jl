# ClickHouse.jl

## Installation

In order to enter `pkg>` mode, enter a REPL and press `]`, then:
```julia-repl
pkg> add ClickHouse
```

## Usage examples

Usage examples can be found on the [usage page](@ref Usage).

## Supported data types

- String, FixedString(N)
- Float32, Float64
- Int8, Int16, Int32, Int64
- UInt8, UInt16, UInt32, UInt64
- Date, DateTime, DateTime64
- Enum
- UUID
- Tuple
- LowCardinality(T)
- Nullable(T)
- Array(T)
- Nothing
- SimpleAggregateFunction
- IPv4, IPv6
- Decimals

## Limitations

- Timezone conversion of `DateTime` / `DateTime64` for columns that have a
  timezone assigned in ClickHouse doesn't happen automatically. All DateTime
  objects are naive, meaning they aren't timezone aware. For reasoning, see
  [this post](https://github.com/JuliaDatabases/ClickHouse.jl/pull/21) and
  [this post](https://github.com/JuliaDatabases/ClickHouse.jl/issues/7#issuecomment-683311706).

## Index

```@index
```

## Contents

```@contents
```

## Credits

- [@xzkostyan](https://github.com/xzkostyan)
  (Konstantin Lebedev, maintainer of the
  [Python ClickHouse driver](https://github.com/mymarilyn/clickhouse-driver)
  that served as reference for many column type implementations in this lib)