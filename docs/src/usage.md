# Usage

## Connecting

Connect to the server's native TCP port, normally 9000. This client uses
unencrypted native TCP. It cannot connect to an HTTPS endpoint or an encrypted
native TCP endpoint, such as the [ClickHouse playground](https://clickhouse.com/docs/get-started/sample-datasets/playground).

```@repl usage
using ClickHouse
sock = ClickHouse.connect("localhost", 9000; compression = ClickHouse.COMPRESSION_LZ4);
```

## Executing DDL

### Creating a table

This temporary table lasts until the connection closes. Reuse the same socket
for the following queries.

```@repl usage
ClickHouse.execute(sock, """
    CREATE TEMPORARY TABLE MyTable
        (u UInt64, f Float32, s String)
    ENGINE = Memory
""")
```

## Inserting data

```@repl usage
data = Dict(
    :u => UInt64[42, 1337, 123],
    :f => Float32[0., ℯ, π],
    :s => String["aa", "bb", "cc"],
);
ClickHouse.insert(sock, "MyTable", [data])
```

```@setup usage
@assert ClickHouse.select(sock, "SELECT * FROM MyTable LIMIT 3") == data
```

## Selecting data

### ... into a dict of `(column, data)` pairs

```@repl usage
ClickHouse.select(sock, "SELECT * FROM MyTable LIMIT 3")
```

### ... into a DataFrame

```@repl usage
ClickHouse.select_df(sock, "SELECT * FROM MyTable LIMIT 3")
```

### ... streaming through a channel

```@repl usage
for block in ClickHouse.select_channel(sock, "SELECT * FROM MyTable LIMIT 1")
    @show block
end
```

### ... streaming each block into a callback

This is the fastest way to stream blocks and is used under the hood
to implement all other `select_xyz` implementations.

```@repl usage
ClickHouse.select_callback(sock, "SELECT * FROM MyTable LIMIT 1") do block
    @show block
end
```

```@setup usage
close(sock)
```
