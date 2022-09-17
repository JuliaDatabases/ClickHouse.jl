"""
A (pure Julia) implementation of the version of CityHash used by ClickHouse.
ClickHouse server comes built-in with an old version of this algorithm - so
the implementation below is not a port of the currently ordained CityHash, but
rather the one required for the transport-compression protocol(s) in ClickHouse.

This is a fairly literal translation of the C source used in
[clickhouse-cityhash](https://github.com/xzkostyan/clickhouse-cityhash).
"""

# Some primes between 2^63 and 2^64 for various uses.
const k0::UInt64 = 0xc3a5c85c97cb3127
const k1::UInt64 = 0xb492b66fbe98f273
const k2::UInt64 = 0x9ae16a3b2f90404f
const k3::UInt64 = 0xc949d7c7509e6557

u8(val) = val % UInt8
u32(val) = val % UInt32
u64(val) = val % UInt64
u128(val) = val % UInt128

# Avoid shifting by 64: doing so yields an undefined result.
@inline function rotate(val::UInt64, shift::Int)
    return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)))
end

## Equivalent to Rotate(), but requires the second arg to be non-zero.
@inline function rotate_by_at_least1(val::UInt64, shift::Int)
    return (val >> shift) | (val << (64 - shift))
end

@inline function shift_mix(val::UInt64)
    return val ⊻ (val >> 47)
end

@inline low64(x::UInt128)::UInt64 = u64(x)
@inline high64(x::UInt128)::UInt64 = u64(x >> 64)
@inline UInt128(x::UInt64, y::UInt64) = (u128(y) << 64) + u128(x)

"""
Hash 128 input bits down to 64 bits of output.
This is intended to be a reasonably good hash function.
"""
@inline function hash_128_to_64(x::UInt128)
    ## Murmur-inspired hashing.
    kMul::UInt64 = 0x9ddfea08eb382d69
    a = (low64(x) ⊻ high64(x)) * kMul
    a ⊻= (a >> 47)
    b = (high64(x) ⊻ a) * kMul
    b ⊻= (b >> 47)
    b *= kMul
    return b
end

hash_len_16(u::UInt64, v::UInt64)::UInt64 = hash_128_to_64(UInt128(u, v))
reinterpret_first(type, A) = reinterpret(type, A)[begin]

@views function fetch64(s::AbstractArray{})::UInt64
    reinterpret_first(UInt64, s[begin:8])
end

@views function fetch64(s::AbstractArray{}, start::Integer)::UInt64
    reinterpret_first(UInt64, s[start:start+7])
end

@views function fetch32(s::AbstractArray{})::UInt32
    reinterpret_first(UInt32, s[begin:4])
end

@views function fetch32(s::AbstractArray{}, start::Integer)::UInt32
    reinterpret_first(UInt32, s[start:start+3])
end

@views function hash_len_0_to_16(s::Vector{UInt8}, len::UInt)::UInt64
    if len > 8
        a = fetch64(s)
        b = fetch64(s, len - 7)
        return hash_len_16(a, rotate_by_at_least1(b + u64(len), len)) ⊻ b
    end
    if len >= 4
        a = u64(fetch32(s))
        b = u64(fetch32(s, len - 3))
        return hash_len_16(len + (a << 3), b)
    end
    if len > 0
        a = s[1]
        b = s[len>>1+1]
        c = s[len]
        y = u32(a) + u32(b) << 8
        z = len + u32(c) << 2
        return shift_mix(y * k2 ⊻ z * k3) * k2
    end
end

function hash_len_17_to_32(s::Vector{UInt8}, len::UInt)::UInt64
    a = fetch64(s) * k1
    b = fetch64(s, 9)
    c = fetch64(s, len - 7) * k2
    d = fetch64(s, len - 15) * k0
    return hash_len_16(rotate(a - b, 43) + rotate(c, 30) + d,
        a + rotate(b ⊻ k3, 20) - c + len)
end

function weak_hash_len32_with_seeds(w::UInt64, x::UInt64, y::UInt64,
    z::UInt64, a::UInt64, b::UInt64)
    a += w
    b = rotate(b + a + z, 21)
    c = a
    a += x
    a += y
    b += rotate(a, 44)
    return (a + z, b + c)
end

function weak_hash_len32_with_seeds(s::AbstractArray{}, a::UInt64, b::UInt64)
    return weak_hash_len32_with_seeds(
        fetch64(s),
        fetch64(s, 9),
        fetch64(s, 17),
        fetch64(s, 25),
        a,
        b
    )
end

@views function hash_len_33_to_64(s::Vector{UInt8}, len::UInt)
    z = fetch64(s, 25)
    a = fetch64(s) + (len + fetch64(s, len - 15)) * k0
    b = rotate(a + z, 52)
    c = rotate(a, 37)
    a += fetch64(s, 9)
    c += rotate(a, 7)
    vf = a + z
    vs = b + rotate(a, 31) + c
    a = fetch64(s, 17) + fetch64(s, len - 31)
    z = fetch64(s, len - 7)
    b = rotate(a + z, 52)
    c = rotate(a, 37)
    a += fetch64(s, len - 23)
    c += rotate(a, 7)
    a += fetch64(s, len - 15)
    wf = a + z
    ws = b + rotate(a, 31) + c
    r = shift_mix((vf + ws) * k2 + (wf + vs) * k0)l
    return shift_mix(r * k0 + vs) * k2
end

@views function city_hash_64(s::Vector{UInt8}, len::UInt)
    if (len <= 32)
        if len < 16
            return hash_len_0_to_16(s, len)
        else
            return hash_len_17_to_32(s, len)
        end
    elseif (len <= 64)
        return hash_len_33_to_64(s, len)
    end

    ## For strings over 64 bytes we hash the end first, and then as we
    ## loop we keep 56 bytes of state: v, w, x, y, and z.
    x = fetch64(s)
    y = fetch64(s, len - 15) ⊻ k1
    z = fetch64(s, len - 55) ⊻ k0
    v = weak_hash_len32_with_seeds(s[len-63:len], len, y)
    w = weak_hash_len32_with_seeds(s[len-31:len], len * k1, k0)
    z += shift_mix(v[2]) * k1
    x = rotate(z + x, 39) * k1
    y = rotate(y, 33) * k1

    ## Decrease len to the nearest multiple of 64 and operate on 64-byte chunks.
    len = (len - 1) & ~UInt(63)
    while true
        x = rotate(x + y + v[1] + fetch64(s, 17), 37) * k1
        y = rotate(y + v[2] + fetch64(s, 49), 42) * k1
        x ⊻= w[2]
        y ⊻= v[1]
        z = rotate(z ⊻ w[1], 33)
        v = weak_hash_len32_with_seeds(s, v[2] * k1, x + w[1])
        w = weak_hash_len32_with_seeds(s[33:end], z + w[2], y)

        x, z = z, x
        s = s[65:end]
        len -= 64
        len != 0 || break
    end
    return hash_len_16(hash_len_16(v[1], w[1]) + shift_mix(y) * k1 + z,
        hash_len_16(v[2], w[2]) + x)
end

function city_hash_64(s::Vector{UInt8}, len::UInt, seed::UInt)
    return city_hash_64(s, len, k2, seed)
end

function city_hash_64(s::Vector{UInt8}, len::UInt, seed0::UInt, seed1::UInt)
    return hash_len_16(city_hash_64(s, len) - seed0, seed1)
end

"""
A subroutine for CityHash128().  Returns a decent 128-bit hash for strings
of any length representable in signed long.  Based on City and Murmur.
"""
@views function city_murmor(s::Vector{UInt8}, len::UInt, seed::UInt128)::UInt128
    a = low64(seed)
    b = high64(seed)
    c = 0
    d = 0
    l = Int64(len - 16)
    if l <= 0
        a = shift_mix(a * k1) * k1
        c = b * k1 + hash_len_0_to_16(s, len)
        d = shift_mix(a + (len >= 8 ? fetch64(s) : c))
    else  # len > 16
        c = hash_len_16(fetch64(s, len - 7) + k1, a)
        d = hash_len_16(b + len, c + fetch64(s, len - 15))
        a += d
        while true
            a ⊻= shift_mix(fetch64(s) * k1) * k1
            a *= k1
            b ⊻= b
            c ⊻= shift_mix(fetch64(s, 9) * k1) * k1
            c *= k1
            d ⊻= c
            s += 16
            l -= 16
            l > 0 || break
        end
    end
    a = hash_len_16(a, c)
    b = hash_len_16(d, b)
    return UInt128(a ⊻ b, hash_len_16(b, a))
end

@views function city_hash_128_with_seed(s::Vector{UInt8}, len::UInt, seed::UInt128):::UInt128
    if len < 128
        return city_murmor(s, len, seed)
    end
    s_og, len_og = s, length(s)  # backtracking can occur

    # We expect len >= 128 to be the common case.  Keep 56 bytes of state:
    # v, w, x, y, and z.
    v = [u64(0), u64(0)]
    w = [u64(0), u64(0)]
    x = low64(seed)
    y = high64(seed)
    z = len * k1
    v[1] = rotate(y ⊻ k1, 49) * k1 + fetch64(s)
    v[2] = rotate(v[1], 42) * k1 + fetch64(s, 9)
    w[1] = rotate(y + z, 35) * k1 + x
    w[2] = rotate(x + fetch64(s, 89), 53) * k1

    # This is the same inner loop as CityHash64, manually unrolled
    while true
        x = rotate(x + y + v[1] + fetch64(s, 17), 37) * k1
        y = rotate(y + v[2] + fetch64(s, 49), 42) * k1
        x ⊻= w[2]
        y ⊻= v[1]
        z = rotate(z ⊻ w[1], 33)
        v = weak_hash_len32_with_seeds(s, v[2] * k1, x + w[1])
        w = weak_hash_len32_with_seeds(s[33:end], z + w[2], y)
        x, z = z, x
        s = s[65:end]

        x = rotate(x + y + v[1] + fetch64(s, 17), 37) * k1
        y = rotate(y + v[2] + fetch64(s, 49), 42) * k1
        x ⊻= w[2]
        y ⊻= v[1]
        z = rotate(z ⊻ w[1], 33)
        v = weak_hash_len32_with_seeds(s, v[2] * k1, x + w[1])
        w = weak_hash_len32_with_seeds(s[33:end], z + w[2], y)
        x, z = z, x
        s = s[65:end]

        len -= 128
        len >= 128 || break
    end
    y += rotate(w[1], 37) * k0 + z
    x += rotate(v[1] + z, 49) * k0

    # If 0 < len < 128, hash up to 4 chunks of 32 bytes each from the end of s.
    tail_done = 0
    while tail_done < len
        tail_done += 32
        y = rotate(y - x, 42) * k0 + v[2]
        w = (w[1] + fetch64(s_og, len_og - tail_done + 17), w[2])
        x = rotate(x, 49) * k0 + w[1]
        w = (w[1] + v[1], w[2])
        v = weak_hash_len32_with_seeds(s_og[len_og-tail_done+1:end], v[1], v[2])
    end

    # At this point our 48 bytes of state should contain more than
    # enough information for a strong 128-bit hash.  We use two
    # different 48-byte-to-8-byte hashes to get a 16-byte final result.
    x = hash_len_16(x, v[1])
    y = hash_len_16(y, w[1])
    return UInt128(hash_len_16(x + v[2], w[2]) + y,
        hash_len_16(x + w[2], y + v[2]))
end

@views function city_hash_128(s::Vector{UInt8}, len::UInt)::UInt128
    if len >= 16
        return city_hash_128_with_seed(s[17:end],
            len - 16,
            UInt128(fetch64(s) ⊻ k3, fetch64(s[9:16]))
        )
    elseif len >= 8
        return city_hash_128_with_seed(
            [], 0, UInt128(fetch64(s) ⊻ (len * k0), fetch64(s[len-7:len]) ⊻ k1)
        )
    else
        return city_hash_128_with_seed(s, len, UInt128(k0, k1))
    end
end

function city_hash_128(s::String)::UInt128
    data = Vector{UInt8}(s)
    city_hash_128(data, length(data))
end
