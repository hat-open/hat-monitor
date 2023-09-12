"""Observer Server"""

import contextlib
import itertools
import logging
import typing

from hat import aio
from hat import json
from hat import util
from hat.drivers import chatter
from hat.drivers import tcp

from hat.monitor.observer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

StateCb: typing.TypeAlias = aio.AsyncCallable[['Server', 'State'], None]
"""State callback"""


class State(typing.NamedTuple):
    mid: int
    local_components: list[common.ComponentInfo]
    global_components: list[common.ComponentInfo]


async def listen(addr: tcp.Address,
                 *,
                 default_rank: int = 1,
                 close_timeout: float = 3,
                 state_cb: StateCb | None = None,
                 **kwargs
                 ) -> 'Server':
    """Create listening Observer Server

    All client connections are always bound to server lifetime regardles
    of `bind_connections` argument.

    Additional arguments are passed directly to `hat.drivers.chatter.listen`.

    """
    server = Server()
    server._default_rank = default_rank
    server._close_timeout = close_timeout
    server._state_cb = state_cb
    server._state = State(mid=0,
                          local_components=[],
                          global_components=[])
    server._next_cids = itertools.count(1)
    server._cid_conns = {}
    server._rank_cache = {}

    server._srv = await chatter.listen(server._client_loop, addr, **kwargs)

    return server


class Server(aio.Resource):
    """Observer Server

    For creating new instance of this class see `listen` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    @property
    def state(self) -> State:
        """Server's state"""
        return self._state

    async def update(self,
                     mid: int,
                     global_components: list[common.ComponentInfo]):
        """Update server's monitor id and global components"""
        if (mid == self._state.mid and
                global_components == self._state.global_components):
            return

        blessing_reqs = {i.cid: i.blessing_req
                         for i in global_components
                         if i.mid == mid}

        local_components = [
            i._replace(mid=mid,
                       blessing_req=blessing_reqs.get(i.cid, i.blessing_req))
            for i in self._state.local_components]

        await self._change_state(mid=mid,
                                 local_components=local_components,
                                 global_components=global_components)

    async def set_rank(self,
                       cid: int,
                       rank: int):
        """Set component rank"""
        info = util.first(self._state.local_components,
                          lambda i: i.cid == cid)
        if not info or info.rank == rank:
            return

        if info.name is not None:
            self._rank_cache[info.name, info.group] = rank

        updated_info = info._replace(rank=rank)
        local_components = [(updated_info if i is info else i)
                            for i in self._state.local_components]

        await self._change_state(local_components=local_components)

    async def _client_loop(self, conn):
        cid = next(self._next_cids)
        self._cid_conns[cid] = conn

        mlog.debug('starting client loop (cid: %s)', cid)
        try:
            local_components = [*self._state.local_components,
                                self._get_init_info(cid)]
            await self._change_state(local_components=local_components)

            while True:
                msg_type, msg_data = await common.receive_msg(conn)

                if msg_type != 'HatObserver.MsgClient':
                    raise Exception('unsupported message type')

                mlog.debug('received msg client (cid: %s)', cid)
                await self._update_client(
                    cid=cid,
                    name=msg_data['name'],
                    group=msg_data['group'],
                    data=json.decode(msg_data['data']),
                    blessing_res=common.blessing_res_from_sbs(
                        msg_data['blessingRes']))

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('client loop error (cid: %s): %s', cid, e, exc_info=e)

        finally:
            mlog.debug('closing client loop (cid: %s)', cid)
            await aio.uncancellable(self._remove_client(cid))

    async def _change_state(self, **kwargs):
        self._state = self._state._replace(**kwargs)

        for cid, conn in list(self._cid_conns.items()):
            with contextlib.suppress(ConnectionError):
                await common.send_msg(conn, 'HatObserver.MsgServer', {
                    'cid': cid,
                    'mid': self._state.mid,
                    'components': [common.component_info_to_sbs(info)
                                   for info in self._state.global_components]})

        if self._state_cb:
            await aio.call(self._state_cb, self, self._state)

    async def _remove_client(self, cid):
        conn = self._cid_conns.pop(cid)

        try:
            local_components = [i for i in self._state.local_components
                                if i.cid != cid]
            await self._change_state(local_components=local_components)

        except Exception as e:
            mlog.error('change state error: %s', e, exc_info=e)

        with contextlib.suppress(Exception):
            await conn.send(chatter.Data('HatObserver.MsgClose', b''))
            await aio.wait_for(conn.wait_closed(), self._close_timeout)

        await conn.async_close()

    async def _update_client(self, cid, name, group, data, blessing_res):
        info = util.first(self._state.local_components,
                          lambda i: i.cid == cid)
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

        local_components = [(updated_info if i is info else i)
                            for i in self._state.local_components]
        await self._change_state(local_components=local_components)

    def _get_init_info(self, cid):
        return common.ComponentInfo(
            cid=cid,
            mid=self._state.mid,
            name=None,
            group=None,
            data=None,
            rank=self._default_rank,
            blessing_req=common.BlessingReq(token=None,
                                            timestamp=None),
            blessing_res=common.BlessingRes(token=None,
                                            ready=False))
