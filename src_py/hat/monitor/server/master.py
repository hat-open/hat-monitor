"""Master communication implementation"""

import contextlib
import itertools
import logging
import typing

from hat import aio
from hat import chatter
from hat import json
from hat import util
from hat.monitor.server import blessing
from hat.monitor.server import common
import hat.monitor.server.server


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


ChangeCb = typing.Callable[[typing.List[common.ComponentInfo]], None]


async def create(conf: json.Data
                 ) -> 'Master':
    """Create master

    Args:
        conf: configuration defined by
            ``hat://monitor/main.yaml#/definitions/master``

    """
    master = Master()
    master._next_mids = itertools.count(1)
    master._group_algorithms = {
        group: common.Algorithm[algorithm]
        for group, algorithm in conf['group_algorithms'].items()}
    master._default_algorithm = common.Algorithm[conf['default_algorithm']]
    master._components = []
    master._mid_components = {}
    master._change_cbs = util.CallbackRegistry()
    master._active_subgroup = aio.Group()
    master._active_subgroup.close()

    master._srv = await chatter.listen(
        sbs_repo=common.sbs_repo,
        address=conf['address'],
        connection_cb=master._on_connection)

    mlog.debug('master listens slaves on %s', conf['address'])
    return master


class Master(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    @property
    def components(self) -> typing.List[common.ComponentInfo]:
        """Global components"""
        return self._components

    def register_change_cb(self,
                           cb: ChangeCb
                           ) -> util.RegisterCallbackHandle:
        """Register change callback

        Change callback is called once components property changes.

        """
        return self._change_cbs.register(cb)

    async def set_server(self, server: hat.monitor.server.server.Server):
        """Set server

        If `server` is not ``None``, master is activated. Otherwise master is
        deactivated.

        """
        await self._active_subgroup.async_close()
        if not server:
            return

        subgroup = self.async_group.create_subgroup()
        subgroup.spawn(self._active_loop, server, subgroup)

    def _on_connection(self, conn):
        try:
            self._active_subgroup.spawn(self._slave_loop, conn)

        except Exception:
            conn.close()

    async def _active_loop(self, server, subgroup):

        def on_server_change():
            self._set_mid_components(0, server.local_components)

        def on_master_change(components):
            server.update(0, components)

        try:
            mlog.debug('master activated')

            self._active_subgroup = subgroup
            self._components = []
            self._mid_components[0] = []

            with server.register_change_cb(on_server_change):
                with self.register_change_cb(on_master_change):
                    on_server_change()
                    on_master_change(self._components)
                    await server.wait_closing()

        except Exception as e:
            mlog.warning('active loop error: %s', e, exc_info=e)

        finally:
            subgroup.close()
            self._components = []
            self._mid_components = {}
            self._change_cbs.notify(self._components)
            mlog.debug('master deactivated')

    async def _slave_loop(self, conn):
        mid = next(self._next_mids)
        self._mid_components[mid] = []

        def on_master_change(components):
            msg = common.MsgMaster(mid=mid,
                                   components=components)

            with contextlib.suppress(ConnectionError):
                conn.send(chatter.Data(
                    module='HatMonitor',
                    type='MsgMaster',
                    data=common.msg_master_to_sbs(msg)))

        try:
            mlog.debug('connection %s established', mid)

            msg = await conn.receive()
            msg_type = msg.data.module, msg.data.type

            if msg_type != ('HatMonitor', 'MsgSlave'):
                raise Exception('unsupported message type')

            msg_slave = common.msg_slave_from_sbs(msg.data.data)
            self._set_mid_components(mid, msg_slave.components)

            with self.register_change_cb(on_master_change):
                on_master_change(self._components)

                while True:
                    msg = await conn.receive()
                    msg_type = msg.data.module, msg.data.type

                    if msg_type != ('HatMonitor', 'MsgSlave'):
                        raise Exception('unsupported message type')

                    msg_slave = common.msg_slave_from_sbs(msg.data.data)
                    self._set_mid_components(mid, msg_slave.components)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.warning('connection loop error: %s', e, exc_info=e)

        finally:
            conn.close()
            if self._mid_components.pop(mid, []):
                self._update_components()
            mlog.debug('connection %s closed', mid)

    def _set_mid_components(self, mid, components):
        components = [i._replace(mid=mid) for i in components
                      if i.name is not None and i.group is not None]
        if self._mid_components.get(mid, []) == components:
            return

        self._mid_components[mid] = components
        self._update_components()

    def _update_components(self):
        blessing_reqs = {(i.mid, i.cid): i.blessing_req
                         for i in self._components}
        components = itertools.chain.from_iterable(
            self._mid_components.values())
        components = [
            i._replace(blessing_req=blessing_reqs.get((i.mid, i.cid),
                                                      i.blessing_req))
            for i in components]
        components = blessing.calculate(components, self._group_algorithms,
                                        self._default_algorithm)
        if components == self._components:
            return

        self._components = components
        self._change_cbs.notify(self._components)
