
@enum Compression::UInt8 begin
    COMPRESSION_NONE = 0
    COMPRESSION_DRY = 0x02
    COMPRESSION_LZ4 = 0x82
end

Compression(flag::Bool)::Compression = flag ? COMPRESSION_LZ4 : COMPRESSION_NONE

function Compression(name::String)::Compression
    if lowercase(name) == "lz4"
        return COMPRESSION_LZ4
    elseif  lowercase(name) == "dry"
        return COMPRESSION_DRY
    end
    error("unkown compression mode: $(name)")
end

"""compress data according to the compression mode"""
function compress(mode::Compression, data::Vector{UInt8})::Vector{UInt8}
    return if mode == COMPRESSION_NONE || mode == COMPRESSION_DRY
        data
    elseif mode == COMPRESSION_LZ4
        lz4_compress(data)
    end
end

"""decompress data according to the compression mode"""
function decompress(mode::Compression, data::Vector{UInt8})::Vector{UInt8}
    return if mode == COMPRESSION_NONE || mode == COMPRESSION_DRY
        data
    elseif mode == COMPRESSION_LZ4
        lz4_decompress(data)
    end
end

function chwrite(sock::ClickHouseSock, compression::ClickHouse.Compression)
    chwrite(sock, UInt8(compression))
end
