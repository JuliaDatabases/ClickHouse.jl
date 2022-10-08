using CodecLz4

@enum Compression::UInt8 begin
    COMPRESSION_NONE = 0
    COMPRESSION_CHECKSUM_ONLY = 0x02
    COMPRESSION_LZ4 = 0x82
end

Compression(flag::Bool)::Compression = flag ? COMPRESSION_LZ4 : COMPRESSION_NONE

function Compression(name::String)::Compression
    if lowercase(name) == "lz4"
        return COMPRESSION_LZ4
    elseif lowercase(name) == "checksum_only"
        return COMPRESSION_CHECKSUM_ONLY
    end
    error("unkown compression mode: $(name)")
end

"""compress data according to the compression mode"""
function compress(mode::Compression, data)::Vector{UInt8}
    return if mode == COMPRESSION_NONE || mode ==COMPRESSION_CHECKSUM_ONLY
        data
    elseif mode == COMPRESSION_LZ4
        lz4_compress(data)
    end
end

function lz4_decompress(
    input::AbstractArray{UInt8},
    expected_size::Integer=length(input) * 2
)
    out_buffer = Vector{UInt8}(undef, expected_size)
    out_size = CodecLz4.LZ4_decompress_safe(
        pointer(input),
        pointer(out_buffer),
        length(input),
        expected_size
    )
    resize!(out_buffer, out_size)
end

"""decompress data according to the compression mode"""
function decompress(
    mode::Compression,
    data::AbstractArray{UInt8},
    uncompressed_size::Integer = length(data) * 2
)::Vector{UInt8}
    return if mode == COMPRESSION_NONE || mode ==COMPRESSION_CHECKSUM_ONLY
        data
    elseif mode == COMPRESSION_LZ4
        lz4_decompress(data, uncompressed_size)
    end
end
