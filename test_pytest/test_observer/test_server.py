import collections

import pytest

from hat import aio
from hat import util
from hat.drivers import chatter
from hat.drivers import tcp

from hat.monitor.observer import common
from hat.monitor.observer import server


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_listen(addr):
    srv = await server.listen(addr)
    assert srv.is_open
    await srv.async_close()


@pytest.mark.parametrize('conn_count', [1, 2, 5])
async def test_connect(addr, conn_count):
    state_queue = aio.Queue()

    def on_state(srv, state):
        state_queue.put_nowait(state)

    srv = await server.listen(addr, state_cb=on_state)

    conns = collections.deque()
    for i in range(conn_count):
        assert state_queue.empty()

        conn = await chatter.connect(addr)
        conns.append(conn)
        assert conn.is_open

        state = await state_queue.get()
        assert len(state.local_components) == len(conns)

    while conns:
        assert state_queue.empty()

        conn = conns.popleft()
        await conn.async_close()

        state = await state_queue.get()
        assert len(state.local_components) == len(conns)

    await srv.async_close()


async def test_msg_client(addr):
    state_queue = aio.Queue()

    def on_state(srv, state):
        state_queue.put_nowait(state)

    srv = await server.listen(addr, default_rank=123, state_cb=on_state)
    conn = await chatter.connect(addr)

    state = await state_queue.get()
    assert state.mid == 0
    assert state.global_components == []
    assert len(state.local_components) == 1

    info = state.local_components[0]
    assert info.mid == 0
    assert info.name is None
    assert info.group is None
    assert info.data is None
    assert info.rank == 123
    assert info.blessing_req.token is None
    assert info.blessing_req.timestamp is None
    assert info.blessing_res.token is None
    assert info.blessing_res.ready is False

    await common.send_msg(conn, 'HatObserver.MsgClient', {
        'name': 'name xyz',
        'group': 'group zyx',
        'data': '{"abc": 42}',
        'blessingRes': {'token': ('value', 123),
                        'ready': True}})

    state = await state_queue.get()
    assert state.mid == 0
    assert state.global_components == []
    assert len(state.local_components) == 1
    assert state.local_components[0] == info._replace(
        name='name xyz',
        group='group zyx',
        data={'abc': 42},
        blessing_res=common.BlessingRes(token=123,
                                        ready=True))

    await conn.async_close()
    await srv.async_close()


async def test_msg_server(addr):
    srv = await server.listen(addr)

    assert srv.state.mid == 0
    assert srv.state.local_components == []
    assert srv.state.global_components == []

    conn = await chatter.connect(addr)

    msg_type, msg_data = await common.receive_msg(conn)

    assert msg_type == 'HatObserver.MsgServer'
    assert msg_data['mid'] == 0
    assert msg_data['components'] == []

    cid = msg_data['cid']

    info = common.ComponentInfo(
        cid=cid + 1,
        mid=123,
        name='name',
        group='group',
        data=42,
        rank=321,
        blessing_req=common.BlessingReq(token=None,
                                        timestamp=1234),
        blessing_res=common.BlessingRes(token=4321,
                                        ready=True))

    await srv.update(42, [info])

    msg_type, msg_data = await common.receive_msg(conn)

    assert msg_type == 'HatObserver.MsgServer'
    assert msg_data['cid'] == cid
    assert msg_data['mid'] == 42
    assert msg_data['components'] == [common.component_info_to_sbs(info)]

    assert srv.state.mid == 42
    assert len(srv.state.local_components) == 1
    assert srv.state.global_components == [info]

    await srv.set_rank(cid, -42)

    assert srv.state.local_components[0].rank == -42

    await conn.async_close()
    await srv.async_close()


async def test_msg_close(addr):
    srv = await server.listen(addr)
    conn = await chatter.connect(addr)

    msg_type, _ = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgServer'

    assert srv.is_open
    assert conn.is_open

    srv.close()

    msg_type, _ = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgClose'

    assert srv.is_closing
    assert not srv.is_closed
    assert conn.is_open

    await conn.async_close()
    await srv.wait_closed()


async def test_rank_cache(addr):
    srv = await server.listen(addr, default_rank=123)
    conn = await chatter.connect(addr)

    msg_type, _ = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgServer'

    assert srv.state.local_components[0].rank == 123

    await common.send_msg(conn, 'HatObserver.MsgClient', {
        'name': 'name',
        'group': 'group',
        'data': 'null',
        'blessingRes': {'token': ('none', None),
                        'ready': False}})

    msg_type, _ = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgServer'

    assert srv.state.local_components[0].rank == 123

    await srv.set_rank(srv.state.local_components[0].cid, 321)

    assert srv.state.local_components[0].rank == 321

    await conn.async_close()
    conn = await chatter.connect(addr)

    msg_type, _ = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgServer'

    assert srv.state.local_components[0].rank == 123

    await common.send_msg(conn, 'HatObserver.MsgClient', {
        'name': 'name',
        'group': 'group',
        'data': 'null',
        'blessingRes': {'token': ('none', None),
                        'ready': False}})

    msg_type, _ = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgServer'

    assert srv.state.local_components[0].rank == 321

    await conn.async_close()
    await srv.async_close()
