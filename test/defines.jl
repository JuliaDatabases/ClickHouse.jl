using ClickHouse


@testset "server capabilites" begin

    @test ClickHouse.has_temporary_tables(50264)
    @test ClickHouse.has_temporary_tables(50274)
    @test !ClickHouse.has_temporary_tables(50263)
end