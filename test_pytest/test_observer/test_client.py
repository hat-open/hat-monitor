import pytest

from hat import aio
from hat import util
from hat.drivers import chatter
from hat.drivers import tcp

from hat.monitor.observer import client
from hat.monitor.observer import common


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_connect(addr):
    with pytest.raises(Exception):
        await client.connect(addr)

    srv_conn_queue = aio.Queue()
    srv = await chatter.listen(srv_conn_queue.put_nowait, addr)

    conn = await client.connect(addr,
                                name='name',
                                group='group')
    srv_conn = await srv_conn_queue.get()

    assert conn.is_open
    assert srv_conn.is_open

    await conn.async_close()
    await srv.async_close()


async def test_msg_client(addr):
    srv_conn_queue = aio.Queue()
    srv = await chatter.listen(srv_conn_queue.put_nowait, addr)

    conn = await client.connect(addr,
                                name='name',
                                group='group',
                                data='data')
    srv_conn = await srv_conn_queue.get()

    msg_type, msg_data = await common.receive_msg(srv_conn)

    assert msg_type == 'HatObserver.MsgClient'
    assert msg_data == {'name': 'name',
                        'group': 'group',
                        'data': '"data"',
                        'blessingRes': {'token': ('none', None),
                                        'ready': False}}

    await conn.set_blessing_res(common.BlessingRes(token=123,
                                                   ready=True))

    msg_type, msg_data = await common.receive_msg(srv_conn)

    assert msg_type == 'HatObserver.MsgClient'
    assert msg_data == {'name': 'name',
                        'group': 'group',
                        'data': '"data"',
                        'blessingRes': {'token': ('value', 123),
                                        'ready': True}}

    await conn.async_close()
    await srv.async_close()


async def test_msg_server(addr):
    state_queue = aio.Queue()

    def on_state(conn, state):
        state_queue.put_nowait(state)

    srv_conn_queue = aio.Queue()
    srv = await chatter.listen(srv_conn_queue.put_nowait, addr)

    conn = await client.connect(addr,
                                name='name',
                                group='group',
                                state_cb=on_state)
    srv_conn = await srv_conn_queue.get()

    assert state_queue.empty()

    info = common.ComponentInfo(
        cid=123,
        mid=321,
        name='name xyz',
        group='group zyx',
        data=42,
        rank=321,
        blessing_req=common.BlessingReq(token=None,
                                        timestamp=1234),
        blessing_res=common.BlessingRes(token=4321,
                                        ready=True))

    await common.send_msg(srv_conn, 'HatObserver.MsgServer', {
        'cid': info.cid,
        'mid': info.mid,
        'components': [common.component_info_to_sbs(info)]})

    state = await state_queue.get()
    assert state.info == info
    assert state.components == [info]

    await common.send_msg(srv_conn, 'HatObserver.MsgServer', {
        'cid': 123,
        'mid': 321,
        'components': []})

    state = await state_queue.get()
    assert state.info is None
    assert state.components == []

    await conn.async_close()
    await srv.async_close()


async def test_msg_close(addr):
    srv_conn_queue = aio.Queue()
    srv = await chatter.listen(srv_conn_queue.put_nowait, addr)

    conn = await client.connect(addr,
                                name='name',
                                group='group')
    srv_conn = await srv_conn_queue.get()

    await common.send_msg(srv_conn, 'HatObserver.MsgClose', None)

    await conn.wait_closed()
    await srv.async_close()
