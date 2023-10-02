import asyncio

import pytest

from hat import aio
from hat import util
from hat.drivers import tcp

from hat.monitor.observer import common
from hat.monitor.observer import server
from hat.monitor import component


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_connect(addr):

    def create_runner(c):
        return aio.Group()

    with pytest.raises(Exception):
        await component.connect(addr, 'name', 'group', create_runner)

    srv = await server.listen(addr)

    conn = await component.connect(addr, 'name', 'group', create_runner)

    assert conn.is_open

    await conn.async_close()
    await srv.async_close()


async def test_close_req(addr):
    close_req_queue = aio.Queue()

    def create_runner(c):
        return aio.Group()

    def on_close_req(c):
        assert c is conn
        close_req_queue.put_nowait(None)

    srv = await server.listen(addr)

    conn = await component.connect(addr, 'name', 'group', create_runner,
                                   close_req_cb=on_close_req)

    srv.close()

    await close_req_queue.get()

    await conn.wait_closed()
    await srv.wait_closed()


async def test_ready(addr):
    srv_state_queue = aio.Queue()

    def create_runner(c):
        return aio.Group()

    def on_srv_state(s, state):
        srv_state_queue.put_nowait(state)

    srv = await server.listen(addr, state_cb=on_srv_state)

    conn = await component.connect(addr, 'name', 'group', create_runner)
    assert conn.ready is False

    srv_state = await srv_state_queue.get()

    assert len(srv_state.local_components) == 1
    info = srv_state.local_components[0]

    assert info.name is None
    assert info.group is None
    assert info.blessing_res.token is None
    assert info.blessing_res.ready is False

    srv_state = await srv_state_queue.get()

    assert len(srv_state.local_components) == 1
    info = srv_state.local_components[0]

    assert info.name == 'name'
    assert info.group == 'group'
    assert info.blessing_res.token is None
    assert info.blessing_res.ready is False

    await conn.set_ready(True)
    assert conn.ready is True

    srv_state = await srv_state_queue.get()

    assert len(srv_state.local_components) == 1
    info = srv_state.local_components[0]

    assert info.name == 'name'
    assert info.group == 'group'
    assert info.blessing_res.token is None
    assert info.blessing_res.ready is True

    await conn.async_close()
    await srv.async_close()


async def test_state(addr):
    conn_state_queue = aio.Queue()
    srv_state_queue = aio.Queue()

    def create_runner(c):
        return aio.Group()

    def on_conn_state(c, state):
        conn_state_queue.put_nowait(state)

    def on_srv_state(s, state):
        srv_state_queue.put_nowait(state)

    srv = await server.listen(addr, state_cb=on_srv_state)

    conn = await component.connect(addr, 'name', 'group', create_runner,
                                   state_cb=on_conn_state)

    srv_state = await srv_state_queue.get()
    info = srv_state.local_components[0]

    with pytest.raises(TimeoutError):
        await aio.wait_for(conn_state_queue.get(), 0.01)

    await srv.update(mid=info.mid,
                     global_components=[info])

    conn_state = await conn_state_queue.get()
    assert conn.state == conn_state

    assert conn_state.info == info
    assert conn_state.components == [info]

    await conn.async_close()
    await srv.async_close()


async def test_blessing(addr):
    start_queue = aio.Queue()
    stop_queue = aio.Queue()
    conn_state_queue = aio.Queue()
    srv_state_queue = aio.Queue()

    def create_runner(c):
        runner = aio.Group()
        start_queue.put_nowait(None)
        runner.spawn(aio.call_on_cancel, stop_queue.put_nowait, None)
        return runner

    def on_conn_state(c, state):
        conn_state_queue.put_nowait(state)

    def on_srv_state(s, state):
        srv_state_queue.put_nowait(state)

    srv = await server.listen(addr, state_cb=on_srv_state)

    conn = await component.connect(addr, 'name', 'group', create_runner,
                                   state_cb=on_conn_state)

    srv_state = await srv_state_queue.get()
    info = srv_state.local_components[0]
    res = info.blessing_res

    assert res.ready is False
    assert res.token is None

    await conn.set_ready(True)

    srv_state = await srv_state_queue.get()
    info = srv_state.local_components[0]
    res = info.blessing_res

    assert res.ready is True
    assert res.token is None

    req = common.BlessingReq(token=123,
                             timestamp=321)
    await srv.update(mid=info.mid,
                     global_components=[info._replace(blessing_req=req)])

    srv_state = await srv_state_queue.get()
    info = srv_state.local_components[0]

    srv_state = await srv_state_queue.get()
    info = srv_state.local_components[0]
    res = info.blessing_res

    assert res.ready is True
    assert res.token == req.token

    assert start_queue.empty()

    await srv.update(mid=info.mid,
                     global_components=[info])

    await start_queue.get()

    assert start_queue.empty()
    assert stop_queue.empty()

    req = common.BlessingReq(token=None,
                             timestamp=123)
    await srv.update(mid=info.mid,
                     global_components=[info._replace(blessing_req=req)])

    await stop_queue.get()

    srv_state = await srv_state_queue.get()
    info = srv_state.local_components[0]

    srv_state = await srv_state_queue.get()
    info = srv_state.local_components[0]

    srv_state = await srv_state_queue.get()
    info = srv_state.local_components[0]
    res = info.blessing_res

    assert res.ready is True
    assert res.token is None

    assert start_queue.empty()
    assert stop_queue.empty()

    await conn.async_close()
    await srv.async_close()
