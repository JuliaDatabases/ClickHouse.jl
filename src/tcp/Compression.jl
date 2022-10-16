using CodecLz4

@enum Compression::UInt8 begin
    COMPRESSION_NONE = 0
    COMPRESSION_CHECKSUM_ONLY = 0x02
    COMPRESSION_LZ4 = 0x82
end

"""compress data according to the compression mode"""
function compress(mode::Compression, data::Vector{UInt8})::Vector{UInt8}
    return if mode == COMPRESSION_NONE || mode == COMPRESSION_CHECKSUM_ONLY
        data
    elseif mode == COMPRESSION_LZ4
        lz4_compress(data)
    end
end

function lz4_decompress(
    input::AbstractArray{UInt8},
    expected_size::Integer=length(input) * 2
)
    # mark the input variable here because it's not used again later and the
    # call to pointer erases the GC's knowledge of the binding
    GC.@preserve input begin
        out_buffer = Vector{UInt8}(undef, expected_size)
        out_size = CodecLz4.LZ4_decompress_safe(
            pointer(input),
            pointer(out_buffer),
            length(input),
            expected_size
        )
        resize!(out_buffer, out_size)
    end
end

"""decompress data according to the compression mode"""
function decompress(
    mode::Compression,
    data::AbstractArray{UInt8},
    uncompressed_size::Integer=length(data) * 2
)::Vector{UInt8}
    return if mode == COMPRESSION_NONE || mode == COMPRESSION_CHECKSUM_ONLY
        data
    elseif mode == COMPRESSION_LZ4
        GC.@preserve data lz4_decompress(data, uncompressed_size)
    end
end
