import asyncio

import pytest

from hat import aio
from hat import chatter
from hat import util
from hat.monitor import common
import hat.monitor.client


@pytest.fixture
def server_port():
    return util.get_unused_tcp_port()


@pytest.fixture
def server_address(server_port):
    return f'tcp+sbs://127.0.0.1:{server_port}'


async def create_server(address):
    server = Server()
    server._conn_queue = aio.Queue()
    server._srv = await chatter.listen(
        common.sbs_repo, address,
        lambda conn: server._conn_queue.put_nowait(Connection(conn)))
    return server


class Server(aio.Resource):

    @property
    def async_group(self):
        return self._srv.async_group

    async def get_connection(self):
        return await self._conn_queue.get()


class Connection(aio.Resource):

    def __init__(self, conn):
        self._conn = conn

    @property
    def async_group(self):
        return self._conn.async_group

    def send(self, msg_server):
        self._conn.send(chatter.Data(
            module='HatMonitor',
            type='MsgServer',
            data=common.msg_server_to_sbs(msg_server)))

    def send_close(self):
        self._conn.send(chatter.Data(
            module='HatMonitor',
            type='MsgClose',
            data=None))

    async def receive(self):
        msg = await self._conn.receive()
        msg_type = msg.data.module, msg.data.type
        assert msg_type == ('HatMonitor', 'MsgClient')
        return common.msg_client_from_sbs(msg.data.data)


async def test_client_connect_failure(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': None}

    with pytest.raises(ConnectionError):
        await hat.monitor.client.connect(conf, None)


async def test_client_connect(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': None}

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, None)
    conn = await server.get_connection()

    msg = await conn.receive()
    assert msg == common.MsgClient(name=conf['name'],
                                   group=conf['group'],
                                   data=conf['component_address'],
                                   blessing_res=common.BlessingRes(
                                       token=None, ready=False))

    assert server.is_open
    assert client.is_open
    assert conn.is_open

    await server.async_close()
    await client.wait_closed()
    await conn.wait_closed()


async def test_client_set_blessing_res(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': 'address'}

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, 'address')
    conn = await server.get_connection()

    msg = await conn.receive()
    assert msg == common.MsgClient(name=conf['name'],
                                   group=conf['group'],
                                   data=conf['component_address'],
                                   blessing_res=common.BlessingRes(
                                       token=None, ready=False))

    client.set_blessing_res(common.BlessingRes(token=123, ready=True))
    msg = await conn.receive()
    assert msg == common.MsgClient(name=conf['name'],
                                   group=conf['group'],
                                   data=conf['component_address'],
                                   blessing_res=common.BlessingRes(
                                       token=123, ready=True))

    client.set_blessing_res(common.BlessingRes(token=123, ready=True))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)

    client.set_blessing_res(common.BlessingRes(token=None, ready=False))
    msg = await conn.receive()
    assert msg == common.MsgClient(name=conf['name'],
                                   group=conf['group'],
                                   data=conf['component_address'],
                                   blessing_res=common.BlessingRes(
                                       token=None, ready=False))

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)

    await client.async_close()
    await conn.wait_closed()
    await server.async_close()


async def test_client_change(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': 'address'}

    info = common.ComponentInfo(cid=1,
                                mid=2,
                                name='name',
                                group='group',
                                data='data',
                                rank=3,
                                blessing_req=common.BlessingReq(
                                    token=4, timestamp=2.0),
                                blessing_res=common.BlessingRes(
                                    token=5, ready=True))

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, 'address')
    conn = await server.get_connection()

    changes = aio.Queue()
    client.register_change_cb(
        lambda: changes.put_nowait((client.info, client.components)))

    assert changes.empty()
    assert client.info is None
    assert client.components == []

    msg = common.MsgServer(cid=1, mid=2, components=[])
    conn.send(msg)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(changes.get(), 0.001)

    msg = common.MsgServer(cid=1, mid=2, components=[info])
    conn.send(msg)
    change_info, change_components = await changes.get()
    assert change_info == info
    assert change_components == [info]

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(cid=3)])
    conn.send(msg)
    change_info, change_components = await changes.get()
    assert change_info is None
    assert change_components == [info._replace(cid=3)]

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(changes.get(), 0.001)

    await client.async_close()
    await conn.wait_closed()
    await server.async_close()


async def test_client_close(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': None}

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, None)
    conn = await server.get_connection()

    msg = await conn.receive()
    assert msg == common.MsgClient(name=conf['name'],
                                   group=conf['group'],
                                   data=conf['component_address'],
                                   blessing_res=common.BlessingRes(
                                       token=None, ready=False))

    assert server.is_open
    assert client.is_open
    assert conn.is_open

    closing_future = asyncio.Future()
    closed_future = asyncio.Future()

    async def on_close_request():
        closing_future.set_result(None)
        await closed_future

    client.add_close_request_cb(on_close_request)

    assert not closing_future.done()

    conn.send_close()
    await closing_future

    assert client.is_open

    closed_future.set_result(None)
    await client.wait_closed()
    await conn.wait_closed()

    await server.async_close()


async def test_component(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': 'address'}

    info = common.ComponentInfo(cid=1,
                                mid=2,
                                name='name',
                                group='group',
                                data='data',
                                rank=3,
                                blessing_req=common.BlessingReq(
                                    token=None, timestamp=None),
                                blessing_res=common.BlessingRes(
                                    token=None, ready=False))

    running_queue = aio.Queue()

    async def async_run(component):
        running_queue.put_nowait(True)
        try:
            await asyncio.Future()
        finally:
            running_queue.put_nowait(False)

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, 'address')
    component = hat.monitor.client.Component(client, async_run)
    conn = await server.get_connection()

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=False)
    assert component.is_open
    assert running_queue.empty()

    component.set_ready(True)

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=True)
    assert component.is_open
    assert running_queue.empty()

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=None, ready=True))])
    conn.send(msg)
    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=123, ready=True)
    running = await running_queue.get()
    assert running is True
    assert component.is_open
    assert running_queue.empty()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=None, timestamp=None),
                               blessing_res=common.BlessingRes(
                                   token=123, ready=True))])
    conn.send(msg)
    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=True)
    running = await running_queue.get()
    assert running is False
    assert component.is_open
    assert running_queue.empty()

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=321, timestamp=3.0),
                               blessing_res=common.BlessingRes(
                                   token=None, ready=True))])
    conn.send(msg)
    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=321, ready=True)
    assert component.is_open
    running = await running_queue.get()
    assert running is True
    assert component.is_open
    assert running_queue.empty()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)

    await conn.async_close()
    running = await running_queue.get()
    assert running is False
    await component.wait_closed()

    await client.async_close()
    await server.async_close()
    assert running_queue.empty()


async def test_component_return(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': 'address'}

    info = common.ComponentInfo(cid=1,
                                mid=2,
                                name='name',
                                group='group',
                                data='data',
                                rank=3,
                                blessing_req=common.BlessingReq(
                                    token=None, timestamp=None),
                                blessing_res=common.BlessingRes(
                                    token=None, ready=False))

    async def async_run(component):
        return

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, 'address')
    component = hat.monitor.client.Component(client, async_run)
    conn = await server.get_connection()

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=False)

    component.set_ready(True)

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=True)

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=None, ready=True))])
    conn.send(msg)
    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=123, ready=True)

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=123, ready=True))])
    conn.send(msg)
    await component.wait_closed()

    await client.async_close()
    await conn.wait_closed()
    await server.async_close()


async def test_component_exception(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': 'address'}

    info = common.ComponentInfo(cid=1,
                                mid=2,
                                name='name',
                                group='group',
                                data='data',
                                rank=3,
                                blessing_req=common.BlessingReq(
                                    token=None, timestamp=None),
                                blessing_res=common.BlessingRes(
                                    token=None, ready=False))

    async def async_run(component):
        raise Exception()

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, 'address')
    component = hat.monitor.client.Component(client, async_run)
    conn = await server.get_connection()

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=False)

    component.set_ready(True)

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=True)

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=None, ready=True))])
    conn.send(msg)
    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=123, ready=True)

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=123, ready=True))])
    conn.send(msg)
    await component.wait_closed()

    await client.async_close()
    await conn.wait_closed()
    await server.async_close()


async def test_component_close_before_token_confirmed(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': 'address'}

    info = common.ComponentInfo(cid=1,
                                mid=2,
                                name='name',
                                group='group',
                                data='data',
                                rank=3,
                                blessing_req=common.BlessingReq(
                                    token=None, timestamp=None),
                                blessing_res=common.BlessingRes(
                                    token=None, ready=False))

    async def async_run(component):
        await asyncio.Future()

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, 'address')
    component = hat.monitor.client.Component(client, async_run)
    conn = await server.get_connection()

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=False)

    component.set_ready(True)

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=True)

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=None, ready=True))])
    conn.send(msg)
    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=123, ready=True)

    await conn.async_close()
    await client.wait_closed()
    await component.wait_closed()

    await server.async_close()


async def test_component_ready(server_address):
    conf = {'name': 'name',
            'group': 'group',
            'monitor_address': server_address,
            'component_address': 'address'}

    info = common.ComponentInfo(cid=1,
                                mid=2,
                                name='name',
                                group='group',
                                data='data',
                                rank=3,
                                blessing_req=common.BlessingReq(
                                    token=None, timestamp=None),
                                blessing_res=common.BlessingRes(
                                    token=None, ready=False))

    running_queue = aio.Queue()

    async def async_run(component):
        running_queue.put_nowait(True)
        try:
            await asyncio.Future()
        finally:
            running_queue.put_nowait(False)

    server = await create_server(server_address)
    client = await hat.monitor.client.connect(conf, 'address')
    component = hat.monitor.client.Component(client, async_run)
    conn = await server.get_connection()

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=False)
    assert running_queue.empty()

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=None, ready=False))])
    conn.send(msg)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)
    assert running_queue.empty()

    component.set_ready(True)

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=True)

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=123, ready=True)

    running = await running_queue.get()
    assert running is True
    assert running_queue.empty()

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=123, ready=True))])
    conn.send(msg)

    assert running_queue.empty()

    component.set_ready(False)

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=123, ready=False)

    running = await running_queue.get()
    assert running is False
    assert running_queue.empty()

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=False)

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=123, timestamp=2.0),
                               blessing_res=common.BlessingRes(
                                   token=None, ready=False))])
    conn.send(msg)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)
    assert running_queue.empty()

    msg = common.MsgServer(cid=1, mid=2, components=[info._replace(
                               blessing_req=common.BlessingReq(
                                   token=None, timestamp=None),
                               blessing_res=common.BlessingRes(
                                   token=None, ready=False))])
    conn.send(msg)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)
    assert running_queue.empty()

    component.set_ready(True)

    msg = await conn.receive()
    assert msg.blessing_res == common.BlessingRes(token=None, ready=True)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)
    assert running_queue.empty()

    await component.async_close()
    await client.async_close()
    await conn.wait_closed()
    await server.async_close()
