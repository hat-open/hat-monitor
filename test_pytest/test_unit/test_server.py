import asyncio

import pytest

from hat import aio
from hat import chatter
from hat import util
from hat.monitor.server import common
import hat.monitor.server.server


@pytest.fixture
def server_port():
    return util.get_unused_tcp_port()


@pytest.fixture
def server_address(server_port):
    return f'tcp+sbs://127.0.0.1:{server_port}'


async def connect(address):
    conn = Connection()
    conn._conn = await chatter.connect(common.sbs_repo, address)
    return conn


class Connection(aio.Resource):

    @property
    def async_group(self):
        return self._conn.async_group

    def send(self, msg_client):
        self._conn.send(chatter.Data(
            module='HatMonitor',
            type='MsgClient',
            data=common.msg_client_to_sbs(msg_client)))

    async def receive(self):
        msg = await self._conn.receive()
        msg_type = msg.data.module, msg.data.type
        if msg_type == ('HatMonitor', 'MsgServer'):
            return common.msg_server_from_sbs(msg.data.data)
        if msg_type == ('HatMonitor', 'MsgClose'):
            return
        raise Exception('invalid message type')


async def test_create(server_address):
    conf = {'address': server_address,
            'default_rank': 1}

    server = await hat.monitor.server.server.create(conf)
    conn = await connect(server_address)

    msg = await conn.receive()
    assert msg.mid == server.mid
    assert msg.components == server.global_components

    assert conn.is_open
    assert server.is_open

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.receive(), 0.001)

    await conn.async_close()
    assert server.is_open

    server.close()
    await server.wait_closing()
    assert server.is_closing
    await server.wait_closed()
    assert server.is_closed


@pytest.mark.parametrize("conn_count", [1, 2, 5])
async def test_local_components(server_address, conn_count):
    conf = {'address': server_address,
            'default_rank': 123}

    changes = aio.Queue()

    def on_change():
        changes.put_nowait(server.local_components)

    server = await hat.monitor.server.server.create(conf)
    server.register_change_cb(on_change)

    assert server.local_components == []
    assert changes.empty()

    conns = []
    for i in range(conn_count):
        conn = await connect(server_address)
        conns.append(conn)

        local_components = await changes.get()
        assert len(local_components) == len(conns)

        for cid, info in enumerate(local_components):
            assert info == common.ComponentInfo(
                cid=cid,
                mid=0,
                name=None,
                group=None,
                data=None,
                rank=conf['default_rank'],
                blessing_req=common.BlessingReq(
                    token=None,
                    timestamp=None),
                blessing_res=common.BlessingRes(
                    token=None,
                    ready=False))

        msg = await conn.receive()
        info = local_components[-1]
        assert msg.cid == info.cid == i
        assert msg.mid == info.mid == server.mid == 0
        assert msg.components == server.global_components

    for i, conn in enumerate(conns):
        msg = common.MsgClient(name=f'name{i}',
                               group=f'group{i}',
                               data={'data': i},
                               blessing_res=common.BlessingRes(
                                token=i,
                                ready=bool(i % 2)))
        conn.send(msg)

        local_components = await changes.get()
        assert len(local_components) == len(conns)
        info = local_components[i]
        assert info == common.ComponentInfo(
                cid=i,
                mid=0,
                name=msg.name,
                group=msg.group,
                data=msg.data,
                rank=conf['default_rank'],
                blessing_req=common.BlessingReq(
                    token=None,
                    timestamp=None),
                blessing_res=msg.blessing_res)

        conn.send(msg)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(changes.get(), 0.001)

        blessing_req = common.BlessingReq(token=i,
                                          timestamp=123 + i)
        server.update(info.mid, [info._replace(blessing_req=blessing_req)])

        local_components = await changes.get()
        info = local_components[i]
        assert info == common.ComponentInfo(
                cid=i,
                mid=0,
                name=msg.name,
                group=msg.group,
                data=msg.data,
                rank=conf['default_rank'],
                blessing_req=blessing_req,
                blessing_res=msg.blessing_res)

    while conns:
        conn, conns = conns[0], conns[1:]
        await conn.async_close()

        local_components = await changes.get()
        assert len(local_components) == len(conns)

    await server.async_close()


@pytest.mark.parametrize("conn_count", [1, 2, 5])
async def test_global_components(server_address, conn_count):
    conf = {'address': server_address,
            'default_rank': 123}

    components = [common.ComponentInfo(cid=i * 3,
                                       mid=i * 3 + 1,
                                       name=f'name{i}',
                                       group=f'group{i}',
                                       data={'data': i},
                                       rank=i * 3 + 2,
                                       blessing_req=common.BlessingReq(
                                        token=i * 3 + 3,
                                        timestamp=123 + i),
                                       blessing_res=common.BlessingRes(
                                        token=i * 3 + 4,
                                        ready=True))
                  for i in range(10)]

    server = await hat.monitor.server.server.create(conf)
    server.update(0, [])

    conns = []
    for _ in range(conn_count):
        conn = await connect(server_address)
        conns.append(conn)

        msg = await conn.receive()
        assert msg.mid == 0
        assert msg.components == []

    server.update(1, components)
    for conn in conns:
        msg = await conn.receive()
        assert msg.mid == 1
        assert msg.components == components

    server.update(2, components)
    for conn in conns:
        msg = await conn.receive()
        assert msg.mid == 2
        assert msg.components == components

    server.update(2, [])
    for conn in conns:
        msg = await conn.receive()
        assert msg.mid == 2
        assert msg.components == []

    server.update(2, [])
    for conn in conns:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(conn.receive(), 0.001)

    while conns:
        conn, conns = conns[0], conns[1:]
        await conn.async_close()

    await server.async_close()


async def test_set_rank(server_address):
    conf = {'address': server_address,
            'default_rank': 123}

    msg = common.MsgClient(name='name',
                           group='group',
                           data=None,
                           blessing_res=common.BlessingRes(token=None,
                                                           ready=False))

    changes = aio.Queue()

    def on_change():
        changes.put_nowait(server.local_components)

    server = await hat.monitor.server.server.create(conf)
    server.register_change_cb(on_change)

    server.set_rank(123, 321)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(changes.get(), 0.001)

    conn = await connect(server_address)

    components = await changes.get()
    assert len(components) == 1
    info = components[0]
    assert info.name is None
    assert info.rank == conf['default_rank']

    server.set_rank(info.cid, 321)

    components = await changes.get()
    assert len(components) == 1
    info = components[0]
    assert info.rank == 321

    await conn.async_close()

    components = await changes.get()
    assert components == []

    conn = await connect(server_address)

    components = await changes.get()
    assert len(components) == 1
    info = components[0]
    assert info.name is None
    assert info.rank == conf['default_rank']

    conn.send(msg)

    components = await changes.get()
    assert len(components) == 1
    info = components[0]
    assert info.name == msg.name
    assert info.rank == conf['default_rank']

    server.set_rank(info.cid, 321)

    components = await changes.get()
    assert len(components) == 1
    info = components[0]
    assert info.rank == 321

    await conn.async_close()

    components = await changes.get()
    assert components == []

    conn = await connect(server_address)

    components = await changes.get()
    assert len(components) == 1
    info = components[0]
    assert info.name is None
    assert info.rank == conf['default_rank']

    conn.send(msg)

    components = await changes.get()
    assert len(components) == 1
    info = components[0]
    assert info.name == msg.name
    assert info.rank == 321

    await conn.async_close()

    components = await changes.get()
    assert components == []

    await server.async_close()


async def test_close_server(server_address):
    conf = {'address': server_address,
            'default_rank': 1}

    server = await hat.monitor.server.server.create(conf)
    conn = await connect(server_address)

    msg = await conn.receive()
    assert msg.mid == server.mid
    assert msg.components == server.global_components

    assert conn.is_open
    assert server.is_open

    server.close()

    msg = await conn.receive()
    assert msg is None

    assert server.is_closing
    assert not server.is_closed

    await conn.async_close()
    await server.wait_closed()
