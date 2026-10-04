module TestInsertProgress
using ClickHouse, Test

const CH = ClickHouse

function insert_fixture(progress_count; terminal=:success)
    sock = CH.ClickHouseSock(PipeBuffer())
    sock.server_rev = CH.DBMS_VER_REV
    sample = CH.make_block([CH.Column("n", "UInt64", UInt64[])])
    CH.write_packet(sock, CH.ServerTableColumns("", "", sample))
    for _ in 1:progress_count
        CH.write_packet(sock, CH.ServerProgress(CH.VarUInt(1), CH.VarUInt(8),
            CH.VarUInt(0), CH.VarUInt(1), CH.VarUInt(8)))
    end
    if terminal == :success
        CH.write_packet(sock, CH.ServerEndOfStream())
        CH.write_packet(sock, CH.ServerPong())
    elseif terminal == :error
        CH.chwrite(sock, CH.VarUInt(UInt64(CH.SERVER_EXCEPTION)))
        CH.chwrite(sock, CH.ServerExceptionBase(UInt32(241), "MEMORY_LIMIT_EXCEEDED",
            "Insert rejected", "", false))
        CH.write_packet(sock, CH.ServerPong())
    else
        CH.write_packet(sock, CH.ServerPong())
    end
    return sock
end

@testset "Insert completion packet stream" begin
    for progress_count in (0, 1, 4), blocks in (NamedTuple[], [(n=UInt64[1, 2],)],
            [(n=UInt64[1],), (n=UInt64[2],)])
        sock = insert_fixture(progress_count)
        try
            result = try CH.insert(sock, "packet_fixture", blocks) catch err; err end
            @test result === nothing
            @test CH.is_connected(sock)
            @test !CH.is_busy(sock)
            if CH.is_connected(sock)
                @test CH.ping(sock) === nothing
            end
        finally
            close(sock)
        end
    end

    for progress_count in (0, 3)
        sock = insert_fixture(progress_count; terminal=:error)
        try
            result = try CH.insert(sock, "packet_fixture", [(n=UInt64[1],)]) catch err; err end
            @test result isa CH.ClickHouseServerException
            if result isa CH.ClickHouseServerException
                @test result.code == 241
                @test result.name == "MEMORY_LIMIT_EXCEEDED"
                @test result.message == "Insert rejected"
            end
            @test CH.is_connected(sock)
            @test !CH.is_busy(sock)
            if CH.is_connected(sock)
                @test CH.ping(sock) === nothing
            end
        finally
            close(sock)
        end
    end

    sock = insert_fixture(2; terminal=:unexpected)
    @test_throws TypeError CH.insert(sock, "packet_fixture", [(n=UInt64[1],)])
    @test !CH.is_connected(sock)
    @test !CH.is_busy(sock)
end

end
