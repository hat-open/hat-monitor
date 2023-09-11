import pytest

from hat import aio
from hat import juggler
from hat import util

from hat.monitor import common
from hat.monitor.observer import server
from hat.monitor.server import ui


host = '127.0.0.1'

infos = [
    common.ComponentInfo(
        cid=123 + i,
        mid=321 + i,
        name=f'name {i}',
        group=f'group {i}',
        data=42 + i,
        rank=321 + i,
        blessing_req=common.BlessingReq(token=1234 + i,
                                        timestamp=123456 + i),
        blessing_res=common.BlessingRes(token=4321 + i,
                                        ready=bool(i % 2)))
    for i in range(10)]


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def addr(port):
    return f'http://{host}:{port}/ws'


def assert_state(conn_state, srv_state):
    assert conn_state['mid'] == srv_state.mid
    assert (len(conn_state['local_components']) ==
            len(srv_state.local_components))
    assert (len(conn_state['global_components']) ==
            len(srv_state.global_components))

    for conn_info, srv_info in zip(conn_state['local_components'],
                                   srv_state.local_components):
        assert conn_info['cid'] == srv_info.cid
        assert conn_info['name'] == srv_info.name
        assert conn_info['group'] == srv_info.group
        assert conn_info['data'] == srv_info.data
        assert conn_info['rank'] == srv_info.rank

    for conn_info, srv_info in zip(conn_state['global_components'],
                                   srv_state.global_components):
        assert conn_info['cid'] == srv_info.cid
        assert conn_info['mid'] == srv_info.mid
        assert conn_info['name'] == srv_info.name
        assert conn_info['group'] == srv_info.group
        assert conn_info['data'] == srv_info.data
        assert conn_info['rank'] == srv_info.rank
        assert conn_info['blessing_req'] == srv_info.blessing_req._asdict()
        assert conn_info['blessing_res'] == srv_info.blessing_res._asdict()


async def test_create(port, addr):
    state = server.State(mid=0,
                         local_components=[],
                         global_components=[])
    srv = await ui.create(host, port, state)

    assert srv.is_open

    await srv.async_close()


async def test_connect(port, addr):
    state = server.State(mid=0,
                         local_components=[],
                         global_components=[])
    srv = await ui.create(host, port, state)
    conn = await juggler.connect(addr)

    assert conn.is_open

    await conn.async_close()
    await srv.async_close()


async def test_state(port, addr):
    srv_state = server.State(mid=0,
                             local_components=[],
                             global_components=[])
    srv = await ui.create(host, port, srv_state,
                          autoflush_delay=0)

    conn_state_queue = aio.Queue()
    conn = await juggler.connect(addr)
    conn.state.register_change_cb(conn_state_queue.put_nowait)

    conn_state = await conn_state_queue.get()
    assert_state(conn_state, srv_state)

    srv_state = server.State(mid=123,
                             local_components=infos[:5],
                             global_components=infos[5:])
    srv.set_state(srv_state)

    conn_state = await conn_state_queue.get()
    assert_state(conn_state, srv_state)

    await conn.async_close()
    await srv.async_close()


async def test_set_rank(port, addr):
    cid_rank_queue = aio.Queue()

    def on_set_rank(s, cid, rank):
        assert s is srv
        cid_rank_queue.put_nowait((cid, rank))

    srv_state = server.State(mid=0,
                             local_components=[],
                             global_components=[])
    srv = await ui.create(host, port, srv_state,
                          set_rank_cb=on_set_rank)

    conn = await juggler.connect(addr)

    assert cid_rank_queue.empty()

    await conn.send('set_rank', {'cid': 123,
                                 'rank': 321})

    cid, rank = await cid_rank_queue.get()
    assert cid == 123
    assert rank == 321

    assert cid_rank_queue.empty()

    await conn.async_close()
    await srv.async_close()


async def test_invalid_request(port, addr):
    srv_state = server.State(mid=0,
                             local_components=[],
                             global_components=[])
    srv = await ui.create(host, port, srv_state)
    conn = await juggler.connect(addr)

    with pytest.raises(Exception):
        await conn.send('invalid', None)

    await conn.async_close()
    await srv.async_close()
