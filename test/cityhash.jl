using Test
using ClickHouse: city_hash_128, city_hash_64, low64, high64


text = """riverrun, past Eve and Adam's, from swerve of shore to bend
of bay, brings us by a commodius vicus of recirculation back to
Howth Castle and Environs.
    Sir Tristram, violer d'amores, fr'over the short sea, had passen-
core rearrived from North Armorica on this side the scraggy
isthmus of Europe Minor to wielderfight his penisolate war: nor
had topsawyer's rocks by the stream Oconee exaggerated themselse
to Laurens County's gorgios while they went doublin their mumper
all the time: nor avoice from afire bellowsed mishe mishe to
tauftauf thuartpeatrick: not yet, though venissoon after, had a
kidscad buttended a bland old isaac: not yet, though all's fair in
vanessy, were sosie sesthers wroth with twone nathandjoe. Rot a
peck of pa's malt had Jhem or Shen brewed by arclight and rory
end to the regginbrow was to be seen ringsome on the aquaface.
"""

@testset "CityHash128 known hash value comparisson" begin
    """
    answer key generated from C++ clickhouse CityHash128::

    char* x = "...";
    for (int i = 0; i <= strlen(x); i++) {
        auto y = CityHash128_2(x, i);
        std::cout << Uint128Low64(y) << std::endl;
        std::cout << Uint128High64(y) << std::endl;
    }

    where x is the above `text`` string (with whitespace replaced by " ") and
    stripped as is done below.
    """

    answer_key = parse.(UInt64, readlines("ch/fw_ch128_key.txt"))
    t = strip(replace(text, r"\s+" => " "))

    for i in 0:length(t)
        tt = String(t[begin:i])
        h = city_hash_128(tt)
        x̂, ŷ = low64(h), high64(h)

        x, y = answer_key[2 * i + 1], answer_key[2 * i + 2]
        @test x == x̂
        @test y == ŷ
    end
end

@testset "CityHash64" begin
    # examples given https://clickhouse.com/docs/en/native-protocol/hash/#implementations
    @test city_hash_64("Moscow") == UInt64(12507901496292878638)
    @test city_hash_64("How can you write a big system without C++?  -Paul Glick") == UInt64(6237945311650045625)
end

@testset "CityHash128 Unicode" begin
    # SELECT cityHash64('some unicode ϵ Σ ∱')
    # Query id: 8545fa6f-2a23-479c-8400-f17631e1f6f4
    # ┌─cityHash64('some unicode ϵ Σ ∱')─┐
    # │             15571479198080573106 │
    # └──────────────────────────────────┘
    # 1 rows in set. Elapsed: 0.004 sec.
    @test city_hash_64("some unicode ϵ Σ ∱") == UInt64(15571479198080573106)
end
