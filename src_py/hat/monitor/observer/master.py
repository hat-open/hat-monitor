"""Observer Master"""

import collections
import contextlib
import itertools
import logging
import typing

from hat import aio
from hat.drivers import chatter
from hat.drivers import tcp

from hat.monitor.observer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

ComponentsCb: typing.TypeAlias = aio.AsyncCallable[['Master',
                                                    list[common.ComponentInfo]],  # NOQA
                                                   None]
"""Components callback"""

BlessingCb: typing.TypeAlias = typing.Callable[['Master',
                                                list[common.ComponentInfo]],
                                               list[common.ComponentInfo]]
"""Blessing callback"""


async def listen(addr: tcp.Address,
                 *,
                 local_components: list[common.ComponentInfo] = [],
                 global_components_cb: ComponentsCb | None = None,
                 blessing_cb: BlessingCb | None = None,
                 **kwargs
                 ) -> 'Master':
    """Create listening inactive Observer Master

    All slave connections are always bound to server lifetime
    (`bind_connections` should not be set).

    Additional arguments are passed directly to `hat.drivers.chatter.listen`.

    """
    master = Master()
    master._global_components_cb = global_components_cb
    master._blessing_cb = blessing_cb
    master._mid_conns = {}
    master._mid_components = {0: [i._replace(mid=0) for i in local_components]}
    master._global_components = list(
        _get_global_components(master._mid_components))
    master._next_mids = itertools.count(1)
    master._active_subgroup = None

    master._srv = await chatter.listen(master._on_connection, addr,
                                       bind_connections=True,
                                       **kwargs)

    return master


class Master(aio.Resource):
    """Observer Master

    For creating new instance of this class see `listen` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    @property
    def global_components(self) -> list[common.ComponentInfo]:
        return self._global_components

    @property
    def is_active(self) -> bool:
        return self._active_subgroup is not None

    def set_active(self, active: bool):
        if active and not self._active_subgroup:
            self._active_subgroup = self.async_group.create_subgroup()

        elif not active and self._active_subgroup:
            self._active_subgroup.close()
            self._active_subgroup = None

    async def set_local_components(self, local_components):
        await self._update_components(0, local_components)

    def _on_connection(self, conn):
        try:
            self._active_subgroup.spawn(self._slave_loop, conn)

        except Exception:
            conn.close()

    async def _slave_loop(self, conn):
        mid = next(self._next_mids)

        mlog.debug('starting slave loop (mid: %s)', mid)
        try:
            msg_type, msg_data = await common.receive_msg(conn)

            if msg_type != 'HatObserver.MsgSlave':
                raise Exception('unsupported message type')

            self._mid_conns[mid] = conn

            while True:
                mlog.debug('received msg slave (mid: %s)', mid)
                components = [common.component_info_from_sbs(i)
                              for i in msg_data['components']]
                await self._update_components(mid, components)

                msg_type, msg_data = await common.receive_msg(conn)

                if msg_type != 'HatObserver.MsgSlave':
                    raise Exception('unsupported message type')

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('slave loop error (mid: %s): %s', mid, e, exc_info=e)

        finally:
            mlog.debug('stopping slave loop (mid: %s)', mid)
            conn.close()
            await aio.uncancellable(self._remove_slave(mid))

    async def _remove_slave(self, mid):
        conn = self._mid_conns.pop(mid, None)
        if not conn:
            return

        await self._update_components(mid, None)

    async def _update_components(self, mid, components):
        if components is None:
            if mid not in self._mid_components:
                return

            self._mid_components.pop(mid)

        else:
            blessing_reqs = {i.cid: i.blessing_req
                             for i in self._mid_components.get(mid, [])}
            components = [
                i._replace(mid=mid,
                           blessing_req=blessing_reqs.get(i.cid,
                                                          i.blessing_req))
                for i in components]

            if self._mid_components.get(mid) == components:
                return

            self._mid_components[mid] = components

        global_components = list(_get_global_components(self._mid_components))
        self._global_components = (self._blessing_cb(self, global_components)
                                   if self._blessing_cb else global_components)

        self._mid_components = collections.defaultdict(list)
        for i in self._global_components:
            self._mid_components[i.mid].append(i)

        if self._global_components_cb:
            await aio.call(self._global_components_cb, self,
                           self._global_components)

        for mid, conn in list(self._mid_conns.items()):
            with contextlib.suppress(ConnectionError):
                await _send_msg_master(conn, mid, self._global_components)


async def _send_msg_master(conn, mid, global_components):
    components = [common.component_info_to_sbs(i) for i in global_components]
    await common.send_msg(conn, 'HatObserver.MsgMaster', {
        'mid': mid,
        'components': components})


def _get_global_components(mid_components):
    for mid in sorted(mid_components.keys()):
        yield from mid_components[mid]
