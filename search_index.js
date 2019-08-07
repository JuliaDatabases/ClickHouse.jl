var documenterSearchIndex = {"docs":
[{"location":"api/#API-1","page":"API","title":"API","text":"","category":"section"},{"location":"api/#Types-1","page":"API","title":"Types","text":"","category":"section"},{"location":"api/#","page":"API","title":"API","text":"ClickHouseServerException\nClickHouseSock","category":"page"},{"location":"api/#ClickHouse.ClickHouseServerException","page":"API","title":"ClickHouse.ClickHouseServerException","text":"ClickHouse server-side exception.\n\n\n\n\n\n","category":"type"},{"location":"api/#ClickHouse.ClickHouseSock","page":"API","title":"ClickHouse.ClickHouseSock","text":"ClickHouse client socket. Created using connect.\n\n\n\n\n\n","category":"type"},{"location":"api/#Functions-1","page":"API","title":"Functions","text":"","category":"section"},{"location":"api/#","page":"API","title":"API","text":"connect\nexecute\ninsert\nping\nselect\nselect_channel\nselect_df","category":"page"},{"location":"api/#ClickHouse.connect","page":"API","title":"ClickHouse.connect","text":"Establish a connection to a given ClickHouse instance.\n\n\n\n\n\n","category":"function"},{"location":"api/#ClickHouse.execute","page":"API","title":"ClickHouse.execute","text":"Execute a DDL query.\n\n\n\n\n\n","category":"function"},{"location":"api/#ClickHouse.insert","page":"API","title":"ClickHouse.insert","text":"Insert blocks into a table, reading from an iterable. The iterable is expected to yield values of type Dict{Symbol, Any}.\n\n\n\n\n\n","category":"function"},{"location":"api/#ClickHouse.ping","page":"API","title":"ClickHouse.ping","text":"Send a ping request and wait for the response.\n\n\n\n\n\n","category":"function"},{"location":"api/#ClickHouse.select","page":"API","title":"ClickHouse.select","text":"Execute a query, flattening blocks into a single dict of column arrays.\n\n\n\n\n\n","category":"function"},{"location":"api/#ClickHouse.select_channel","page":"API","title":"ClickHouse.select_channel","text":"Execute a query, streaming the resulting blocks through a channel.\n\n\n\n\n\n","category":"function"},{"location":"api/#ClickHouse.select_df","page":"API","title":"ClickHouse.select_df","text":"Execute a query, flattening blocks into a dataframe.\n\n\n\n\n\n","category":"function"},{"location":"usage/#Usage-1","page":"Usage","title":"Usage","text":"","category":"section"},{"location":"usage/#Executing-DDL-1","page":"Usage","title":"Executing DDL","text":"","category":"section"},{"location":"usage/#Creating-a-table-1","page":"Usage","title":"Creating a table","text":"","category":"section"},{"location":"usage/#","page":"Usage","title":"Usage","text":"using ClickHouse\ncon = connect(\"localhost\", 9000)\n\nexecute(con, \"\"\"\n    CREATE TABLE MyTable\n        (u UInt64, f Float32, s String)\n    ENGINE = Memory\n\"\"\")","category":"page"},{"location":"usage/#Inserting-data-1","page":"Usage","title":"Inserting data","text":"","category":"section"},{"location":"usage/#","page":"Usage","title":"Usage","text":"insert(con, \"MyTable\", [Dict(\n    :u => UInt64[42, 1337, 123],\n    :f => Float32[0., ℯ, π],\n    :s => String[\"aa\", \"bb\", \"cc\"],\n)])","category":"page"},{"location":"usage/#Selecting-data-1","page":"Usage","title":"Selecting data","text":"","category":"section"},{"location":"usage/#...-into-a-dict-of-(column,-data)-pairs-1","page":"Usage","title":"... into a dict of (column, data) pairs","text":"","category":"section"},{"location":"usage/#","page":"Usage","title":"Usage","text":"select(con, \"SELECT * FROM MyTable\")","category":"page"},{"location":"usage/#","page":"Usage","title":"Usage","text":"Dict{Symbol,Any} with 3 entries:\n  :f => Float32[0.0, 2.71828, 3.14159]\n  :s => [\"aa\", \"bb\", \"cc\"]\n  :u => UInt64[0x000000000000002a, 0x0000000000000539, 0x000000000000007b]","category":"page"},{"location":"usage/#...-into-a-DataFrame-1","page":"Usage","title":"... into a DataFrame","text":"","category":"section"},{"location":"usage/#","page":"Usage","title":"Usage","text":"select_df(con, \"SELECT * FROM MyTable\")","category":"page"},{"location":"usage/#","page":"Usage","title":"Usage","text":"3×3 DataFrames.DataFrame\n│ Row │ f       │ s      │ u                  │\n│     │ Float32 │ String │ UInt64             │\n├─────┼─────────┼────────┼────────────────────┤\n│ 1   │ 0.0     │ aa     │ 0x000000000000002a │\n│ 2   │ 2.71828 │ bb     │ 0x0000000000000539 │\n│ 3   │ 3.14159 │ cc     │ 0x000000000000007b │","category":"page"},{"location":"usage/#...-streaming-through-a-channel-1","page":"Usage","title":"... streaming through a channel","text":"","category":"section"},{"location":"usage/#","page":"Usage","title":"Usage","text":"ch = select_channel(con, \"SELECT * FROM MyTable LIMIT 1\")\nfor block in ch\n    @show block\nend","category":"page"},{"location":"usage/#","page":"Usage","title":"Usage","text":"block = Dict{Symbol,Any}(\n    :f => Float32[0.0],\n    :s => [\"aa\"],\n    :u => UInt64[0x000000000000002a],\n)","category":"page"},{"location":"#ClickHouse.jl-1","page":"ClickHouse.jl","title":"ClickHouse.jl","text":"","category":"section"},{"location":"#Installation-1","page":"ClickHouse.jl","title":"Installation","text":"","category":"section"},{"location":"#","page":"ClickHouse.jl","title":"ClickHouse.jl","text":"In order to enter pkg> mode, enter a REPL and press ], then:","category":"page"},{"location":"#","page":"ClickHouse.jl","title":"ClickHouse.jl","text":"pkg> add https://github.com/athre0z/ClickHouse.jl.git","category":"page"},{"location":"#Supported-data-types-1","page":"ClickHouse.jl","title":"Supported data types","text":"","category":"section"},{"location":"#","page":"ClickHouse.jl","title":"ClickHouse.jl","text":"String\nFloat32, Float64\nInt8, Int16, Int32, Int64\nUInt8, UInt16, UInt32, UInt64\nDate, DateTime\ncurrently represented as UInts – this is subject of change","category":"page"},{"location":"#","page":"ClickHouse.jl","title":"ClickHouse.jl","text":"All other types are currently not implemented. PRs welcome.","category":"page"},{"location":"#Limitations-1","page":"ClickHouse.jl","title":"Limitations","text":"","category":"section"},{"location":"#","page":"ClickHouse.jl","title":"ClickHouse.jl","text":"Transfer compression is currently not implemented.","category":"page"},{"location":"#Contents-1","page":"ClickHouse.jl","title":"Contents","text":"","category":"section"},{"location":"#","page":"ClickHouse.jl","title":"ClickHouse.jl","text":"","category":"page"},{"location":"#Index-1","page":"ClickHouse.jl","title":"Index","text":"","category":"section"},{"location":"#","page":"ClickHouse.jl","title":"ClickHouse.jl","text":"","category":"page"}]
}
