"""Observer Master"""

from collections.abc import Iterable
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

ComponentsCb: typing.TypeAlias = aio.AsyncCallable[
    ['Master', list[common.ComponentInfo]],
    None]
"""Components callback"""

BlessingCb: typing.TypeAlias = typing.Callable[
    ['Master', Iterable[common.ComponentInfo]],
    Iterable[tuple[common.Mid, common.Cid, common.BlessingReq]]]
"""Blessing callback"""


async def listen(addr: tcp.Address,
                 *,
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
    master._mid_cid_infos = {0: {}}
    master._global_components = []
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

    async def set_local_components(self, local_components: Iterable[common.ComponentInfo]):  # NOQA
        await self._update_components(0, local_components)

    async def set_local_blessing_reqs(self, blessing_reqs: Iterable[tuple[common.Cid, common.BlessingReq]]):  # NOQA
        change = False

        for cid, blessing_req in blessing_reqs:
            info = self._mid_cid_infos[0].get(cid)
            if not info or info.blessing_req == blessing_req:
                continue

            self._mid_cid_infos[0][cid] = info._replace(
                blessing_req=blessing_req)
            change = True

        if change:
            await self._update_global_components()

    def _on_connection(self, conn):
        try:
            self._active_subgroup.spawn(self._slave_loop, conn)

        except Exception:
            conn.close()

    async def _slave_loop(self, conn):
        mid = next(self._next_mids)

        mlog.debug('starting slave loop (mid: %s)', mid)
        try:
            while True:
                msg_type, msg_data = await common.receive_msg(conn)

                if msg_type != 'HatObserver.MsgSlave':
                    raise Exception('unsupported message type')

                mlog.debug('received msg slave (mid: %s)', mid)
                components = (common.component_info_from_sbs(i)
                              for i in msg_data['components'])
                await self._update_components(mid, components)

                if mid not in self._mid_conns:
                    self._mid_conns[mid] = conn

                    global_components = list(
                        _flatten_mid_cid_infos(self._mid_cid_infos))
                    await _send_msg_master(conn, mid, global_components)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('slave loop error (mid: %s): %s', mid, e, exc_info=e)

        finally:
            mlog.debug('stopping slave loop (mid: %s)', mid)
            conn.close()
            await aio.uncancellable(self._remove_slave(mid))

    async def _remove_slave(self, mid):
        self._mid_conns.pop(mid, None)

        if not self._mid_cid_infos.pop(mid, None):
            return

        await self._update_global_components()

    async def _update_components(self, mid, components):
        cid_infos = self._mid_cid_infos.get(mid, {})

        self._mid_cid_infos[mid] = {}
        for info in components:
            info = info._replace(mid=mid)

            old_info = cid_infos.get(info.cid)
            if old_info:
                info = info._replace(blessing_req=old_info.blessing_req)

            self._mid_cid_infos[mid][info.cid] = info

        if self._mid_cid_infos[mid] == cid_infos:
            return

        await self._update_global_components()

    async def _update_global_components(self):
        if self._blessing_cb:
            infos = _flatten_mid_cid_infos(self._mid_cid_infos)

            for mid, cid, blessing_req in self._blessing_cb(self, infos):
                info = self._mid_cid_infos[mid][cid]
                info = info._replace(blessing_req=blessing_req)
                self._mid_cid_infos[mid][cid] = info

        global_components = list(_flatten_mid_cid_infos(self._mid_cid_infos))
        if global_components == self._global_components:
            return

        self._global_components = global_components

        if self._global_components_cb:
            await aio.call(self._global_components_cb, self, global_components)

        for mid, conn in list(self._mid_conns.items()):
            with contextlib.suppress(ConnectionError):
                await _send_msg_master(conn, mid, global_components)


async def _send_msg_master(conn, mid, global_components):
    components = [common.component_info_to_sbs(i) for i in global_components]
    await common.send_msg(conn, 'HatObserver.MsgMaster', {
        'mid': mid,
        'components': components})


def _flatten_mid_cid_infos(mid_cid_infos):
    for cid_infos in mid_cid_infos.values():
        yield from cid_infos.values()
