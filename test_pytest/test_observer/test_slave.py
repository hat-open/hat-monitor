import asyncio

import pytest

from hat import aio
from hat import util
from hat.drivers import chatter
from hat.drivers import tcp

from hat.monitor.observer import common
import hat.monitor.observer.slave


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
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_connect(addr):
    with pytest.raises(Exception):
        await hat.monitor.observer.slave.connect(addr)

    conn_queue = aio.Queue()
    srv = await chatter.listen(conn_queue.put_nowait, addr)

    slave = await hat.monitor.observer.slave.connect(addr)
    conn = await conn_queue.get()

    assert slave.is_open
    assert conn.is_open

    await slave.async_close()
    await srv.async_close()


async def test_msg_slave(addr):
    conn_queue = aio.Queue()
    srv = await chatter.listen(conn_queue.put_nowait, addr)

    slave = await hat.monitor.observer.slave.connect(addr)
    conn = await conn_queue.get()

    msg_type, msg_data = await common.receive_msg(conn)

    assert msg_type == 'HatObserver.MsgSlave'
    assert msg_data == {'components': []}

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(common.receive_msg(conn), 0.01)

    await slave.update(infos)

    msg_type, msg_data = await common.receive_msg(conn)

    assert msg_type == 'HatObserver.MsgSlave'
    assert msg_data == {'components': [common.component_info_to_sbs(info)
                                       for info in infos]}

    await slave.async_close()
    await srv.async_close()


async def test_msg_master(addr):
    state_queue = aio.Queue()

    def on_state(slave, state):
        state_queue.put_nowait(state)

    conn_queue = aio.Queue()
    srv = await chatter.listen(conn_queue.put_nowait, addr)

    slave = await hat.monitor.observer.slave.connect(addr, state_cb=on_state)
    conn = await conn_queue.get()

    assert state_queue.empty()

    await common.send_msg(conn, 'HatObserver.MsgMaster', {
        'mid': 42,
        'components': []})

    state = await state_queue.get()

    assert state == slave.state
    assert state.mid == 42
    assert state.global_components == []

    assert state_queue.empty()

    await common.send_msg(conn, 'HatObserver.MsgMaster', {
        'mid': 24,
        'components': [common.component_info_to_sbs(info)
                       for info in infos]})

    state = await state_queue.get()

    assert state == slave.state
    assert state.mid == 24
    assert state.global_components == infos

    await slave.async_close()
    await srv.async_close()
