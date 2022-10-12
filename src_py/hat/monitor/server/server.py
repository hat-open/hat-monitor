"""Implementation of local monitor server communication"""

import asyncio
import contextlib
import itertools
import logging
import typing

from hat import aio
from hat import chatter
from hat import json
from hat import util
from hat.monitor.server import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

close_timeout: float = 3


async def create(conf: json.Data
                 ) -> 'Server':
    """Create local monitor server

    Args:
        conf: configuration defined by
            ``hat://monitor/main.yaml#/definitions/server``

    """
    server = Server()
    server._default_rank = conf['default_rank']
    server._next_cids = itertools.count(0)
    server._mid = 0
    server._rank_cache = {}
    server._local_components = []
    server._global_components = []
    server._change_cbs = util.CallbackRegistry()

    server._srv = await chatter.listen(
        sbs_repo=common.sbs_repo,
        address=conf['address'],
        connection_cb=server._create_client,
        bind_connections=False)

    mlog.debug('monitor server listens clients on %s', conf['address'])
    return server


class Server(aio.Resource):
    """Local monitor server

    For creating new instance of this class see `create` function.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    @property
    def mid(self) -> int:
        """Server's monitor id"""
        return self._mid

    @property
    def local_components(self) -> typing.List[common.ComponentInfo]:
        """Local components"""
        return self._local_components

    @property
    def global_components(self) -> typing.List[common.ComponentInfo]:
        """Global components"""
        return self._global_components

    def register_change_cb(self,
                           cb: typing.Callable[[], None]
                           ) -> util.RegisterCallbackHandle:
        """Register change callback

        Change callback is called once mid and/or local_components and/or
        global_components property changes.

        """
        return self._change_cbs.register(cb)

    def update(self,
               mid: int,
               global_components: typing.List[common.ComponentInfo]):
        """Update server's monitor id and global components"""
        if self._mid == mid and self._global_components == global_components:
            return

        self._mid = mid
        self._global_components = global_components

        blessing_reqs = {info.cid: info.blessing_req
                         for info in global_components
                         if info.mid == mid}

        self._local_components = [
            info._replace(mid=mid,
                          blessing_req=blessing_reqs.get(info.cid,
                                                         info.blessing_req))
            for info in self._local_components]

        self._change_cbs.notify()

    def set_rank(self,
                 cid: int,
                 rank: int):
        """Set component rank"""
        info = util.first(self._local_components, lambda i: i.cid == cid)
        if not info or info.rank == rank:
            return

        updated_info = info._replace(rank=rank)
        self._local_components = [(updated_info if i is info else i)
                                  for i in self._local_components]
        if info.name is not None:
            self._rank_cache[info.name, info.group] = rank
        self._change_cbs.notify()

    def _create_client(self, conn):
        cid = next(self._next_cids)

        _Client(self, conn, cid)

        info = common.ComponentInfo(
            cid=cid,
            mid=self._mid,
            name=None,
            group=None,
            data=None,
            rank=self._default_rank,
            blessing_req=common.BlessingReq(token=None,
                                            timestamp=None),
            blessing_res=common.BlessingRes(token=None,
                                            ready=False))
        self._local_components = [*self._local_components, info]
        self._change_cbs.notify()

    def _set_client(self, cid, name, group, data, blessing_res):
        info = util.first(self._local_components, lambda i: i.cid == cid)
        updated_info = info._replace(name=name,
                                     group=group,
                                     data=data,
                                     blessing_res=blessing_res)

        if info.name is None:
            rank_cache_key = name, group
            rank = self._rank_cache.get(rank_cache_key, info.rank)
            updated_info = updated_info._replace(rank=rank)

        if info == updated_info:
            return

        self._local_components = [(updated_info if i is info else i)
                                  for i in self._local_components]
        self._change_cbs.notify()

    def _remove_client(self, cid):
        self._local_components = [i for i in self._local_components
                                  if i.cid != cid]
        self._change_cbs.notify()


class _Client(aio.Resource):

    def __init__(self, server, conn, cid):
        self._server = server
        self._conn = conn
        self._cid = cid
        self._mid = server.mid
        self._global_components = server.global_components
        self._async_group = server.async_group.create_subgroup()

        self.async_group.spawn(self._client_loop)

    @property
    def async_group(self):
        return self._async_group

    async def _cleanup(self):
        self.close()

        with contextlib.suppress(Exception):
            self._conn.send(chatter.Data(module='HatMonitor',
                                         type='MsgClose',
                                         data=None))
            await asyncio.wait_for(self._conn.wait_closed(),
                                   timeout=close_timeout)

        self._conn.close()
        self._server._remove_client(self._cid)
        await self._conn.async_close()

    async def _client_loop(self):
        try:
            mlog.debug('connection %s established', self._cid)

            self._mid = self._server.mid
            self._global_components = self._server.global_components
            self._send_msg_server()

            with self._server.register_change_cb(self._on_change):
                while True:
                    msg = await self._conn.receive()
                    msg_type = msg.data.module, msg.data.type

                    if msg_type == ('HatMonitor', 'MsgClient'):
                        msg_client = common.msg_client_from_sbs(msg.data.data)
                        self._process_msg_client(msg_client)

                    else:
                        raise Exception('unsupported message type')

        except ConnectionError:
            pass

        except Exception as e:
            mlog.warning('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing connection %s', self._cid)
            await aio.uncancellable(self._cleanup())

    def _on_change(self):
        if (self._mid == self._server.mid and
                self._global_components == self._server.global_components):
            return

        self._mid = self._server.mid
        self._global_components = self._server.global_components
        self._send_msg_server()

    def _send_msg_server(self):
        msg = common.MsgServer(cid=self._cid,
                               mid=self._mid,
                               components=self._global_components)

        with contextlib.suppress(ConnectionError):
            self._conn.send(chatter.Data(
                module='HatMonitor',
                type='MsgServer',
                data=common.msg_server_to_sbs(msg)))

    def _process_msg_client(self, msg_client):
        self._server._set_client(cid=self._cid,
                                 name=msg_client.name,
                                 group=msg_client.group,
                                 data=msg_client.data,
                                 blessing_res=msg_client.blessing_res)
