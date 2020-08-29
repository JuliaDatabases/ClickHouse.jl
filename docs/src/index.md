# ClickHouse.jl

## Installation

In order to enter `pkg>` mode, enter a REPL and press `]`, then:
```julia-repl
pkg> add ClickHouse
```

## Usage examples

Usage examples can be found on the [usage page](@ref Usage).

## Supported data types

- String
- Float32, Float64
- Int8, Int16, Int32, Int64
- UInt8, UInt16, UInt32, UInt64
- Date, DateTime
- Enum
- UUID
- FixedString(N)
- Tuple

All other types are currently not implemented. PRs welcome.

## Limitations

Transfer compression is currently not implemented.

## Index

```@index
```

## Contents

```@contents
```
