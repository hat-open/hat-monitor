import asyncio

import pytest

from hat import aio
from hat import util
from hat.drivers import chatter
from hat.drivers import tcp

from hat.monitor.observer import common
import hat.monitor.observer.master


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


async def test_listen(addr):
    master = await hat.monitor.observer.master.listen(addr)
    assert master.is_open
    await master.async_close()


async def test_active(addr):
    master = await hat.monitor.observer.master.listen(addr)
    assert master.is_active is False

    conn = await chatter.connect(addr)
    await conn.wait_closed()

    master.set_active(True)

    conn = await chatter.connect(addr)

    await asyncio.sleep(0.01)
    assert conn.is_open

    master.set_active(False)
    await conn.wait_closed()

    await master.async_close()


async def test_msg_slave(addr):
    global_components_queue = aio.Queue()

    def on_global_components(master, components):
        global_components_queue.put_nowait(components)

    master = await hat.monitor.observer.master.listen(
        addr, global_components_cb=on_global_components)
    master.set_active(True)

    conn = await chatter.connect(addr)

    await asyncio.sleep(0.01)
    assert global_components_queue.empty()

    await common.send_msg(conn, 'HatObserver.MsgSlave', {
        'components': [common.component_info_to_sbs(info)
                       for info in infos]})

    msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgMaster'
    mid = msg_data['mid']

    global_components = await global_components_queue.get()
    assert global_components == [info._replace(mid=mid)
                                 for info in infos]
    assert global_components == master.global_components

    await common.send_msg(conn, 'HatObserver.MsgSlave', {
        'components': []})

    global_components = await global_components_queue.get()
    assert global_components == []

    await conn.async_close()
    await master.async_close()


async def test_msg_master(addr):
    master = await hat.monitor.observer.master.listen(addr)
    master.set_active(True)

    conn = await chatter.connect(addr)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(common.receive_msg(conn), 0.01)

    await common.send_msg(conn, 'HatObserver.MsgSlave', {
        'components': []})

    msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgMaster'
    assert msg_data['mid'] > 0
    assert msg_data['components'] == []
    mid = msg_data['mid']

    await master.set_local_components(infos)

    msg_type, msg_data = await common.receive_msg(conn)
    assert msg_type == 'HatObserver.MsgMaster'
    assert msg_data['mid'] == mid
    assert msg_data['components'] == [
        common.component_info_to_sbs(info._replace(mid=0))
        for info in infos]

    await conn.async_close()
    await master.async_close()


async def test_blessing_cb(addr):

    def blessing(m, components):
        assert m is master
        return [component._replace(rank=component.rank + 1)
                for component in components]

    master = await hat.monitor.observer.master.listen(addr,
                                                      blessing_cb=blessing)
    master.set_active(True)

    await master.set_local_components(infos)

    assert master.global_components == [info._replace(mid=0,
                                                      rank=info.rank + 1)
                                        for info in infos]

    await master.async_close()


async def test_update_components_on_different_mids(addr):

    c1 = common.ComponentInfo(
            cid=1,
            mid=0,
            name='c1',
            group='g1',
            data=None,
            rank=1,
            blessing_req=common.BlessingReq(token=None,
                                            timestamp=None),
            blessing_res=common.BlessingRes(token=None,
                                            ready=True))
    c2 = common.ComponentInfo(
            cid=1,
            mid=1,
            name='c2',
            group='g1',
            data=None,
            rank=1,
            blessing_req=common.BlessingReq(token=1,
                                            timestamp=None),
            blessing_res=common.BlessingRes(token=None,
                                            ready=True))

    blessing_input_components_queue = aio.Queue()

    def blessing(m, components):
        assert m is master
        blessing_input_components_queue.put_nowait(components)
        return [c._replace(blessing_req=common.BlessingReq(
                    token=c.blessing_req.token + 1,
                    timestamp=None))
                if (c.mid, c.cid) == (c2.mid, c2.cid) else c
                for c in components]

    master = await hat.monitor.observer.master.listen(addr,
                                                      blessing_cb=blessing)
    master.set_active(True)

    await master.set_local_components([c1])
    components = await blessing_input_components_queue.get()
    assert components == [c1]

    conn = await chatter.connect(addr)
    await asyncio.sleep(0.01)

    await common.send_msg(conn, 'HatObserver.MsgSlave', {
        'components': [common.component_info_to_sbs(c2)]})

    blessing_input_components = await blessing_input_components_queue.get()
    c2_after_bless = c2._replace(
        blessing_req=common.BlessingReq(token=c2.blessing_req.token + 1,
                                        timestamp=None))
    assert blessing_input_components == [c1, c2]
    assert master.global_components == [c1, c2_after_bless]

    c1 = c1._replace(rank=c1.rank + 1)
    await master.set_local_components([c1])
    c2 = c2_after_bless
    c2_after_bless = c2._replace(
        blessing_req=common.BlessingReq(token=c2.blessing_req.token + 1,
                                        timestamp=None))
    blessing_input_components = await blessing_input_components_queue.get()
    assert blessing_input_components == [c1, c2]
    assert master.global_components == [c1, c2_after_bless]

    c1 = c1._replace(rank=c1.rank + 1)
    await master.set_local_components([c1])
    c2 = c2_after_bless
    c2_after_bless = c2._replace(
        blessing_req=common.BlessingReq(token=c2.blessing_req.token + 1,
                                        timestamp=None))
    blessing_input_components = await blessing_input_components_queue.get()
    assert blessing_input_components == [c1, c2]
    assert master.global_components == [c1, c2_after_bless]

    await master.async_close()
