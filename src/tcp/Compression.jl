
@enum Compression::UInt8 begin
    COMPRESSION_NONE = 0
    COMPRESSION_CHECKSUM_ONLY = 0x02
    COMPRESSION_LZ4 = 0x82
end

Compression(flag::Bool)::Compression = flag ? COMPRESSION_LZ4 : COMPRESSION_NONE

function Compression(name::String)::Compression
    if lowercase(name) == "lz4"
        return COMPRESSION_LZ4
    elseif  lowercase(name) == "dry"
        returnCOMPRESSION_CHECKSUM_ONLY
    end
    error("unkown compression mode: $(name)")
end

"""compress data according to the compression mode"""
function compress(mode::Compression, data::Vector{UInt8})::Vector{UInt8}
    return if mode == COMPRESSION_NONE || mode ==COMPRESSION_CHECKSUM_ONLY
        data
    elseif mode == COMPRESSION_LZ4
        lz4_compress(data)
    end
end

"""decompress data according to the compression mode"""
function decompress(
    mode::Compression,
    data::Vector{UInt8},
    uncompressed_size::Integer = length(data) * 2
)::Vector{UInt8}
    return if mode == COMPRESSION_NONE || mode ==COMPRESSION_CHECKSUM_ONLY
        data
    elseif mode == COMPRESSION_LZ4
        lz4_decompress(data, uncompressed_size)
    end
end
