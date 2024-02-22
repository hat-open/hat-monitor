import asyncio
import contextlib
import itertools
import logging

from hat import aio
from hat import json
from hat.drivers import tcp

import hat.monitor.observer.master
import hat.monitor.observer.server
import hat.monitor.observer.slave
import hat.monitor.server.blessing
import hat.monitor.server.ui


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def create(conf: json.Data) -> 'Runner':
    runner = Runner()
    runner._loop = asyncio.get_running_loop()
    runner._async_group = aio.Group()
    runner._server = None
    runner._master = None
    runner._ui = None
    runner._slave = None
    runner._slave_conf = conf['slave']
    runner._slave_parents = [tcp.Address(i['host'], i['port'])
                             for i in conf['slave']['parents']]
    runner._default_algorithm = hat.monitor.server.blessing.Algorithm(
        conf['default_algorithm'])
    runner._group_algorithms = {k: hat.monitor.server.blessing.Algorithm(v)
                                for k, v in conf['group_algorithms'].items()}

    runner.async_group.spawn(aio.call_on_cancel, runner._on_close)

    try:
        mlog.debug('starting server')
        runner._server = await hat.monitor.observer.server.listen(
            tcp.Address(conf['server']['host'], conf['server']['port']),
            default_rank=conf['server']['default_rank'],
            state_cb=runner._on_server_state)
        runner._bind_resource(runner._server)

        mlog.debug('starting master')
        runner._master = await hat.monitor.observer.master.listen(
            tcp.Address(conf['master']['host'], conf['master']['port']),
            local_components=runner._server.state.local_components,
            global_components_cb=runner._on_master_global_components,
            blessing_cb=runner._calculate_blessing)
        runner._bind_resource(runner._master)

        ui_conf = conf.get('ui')
        if ui_conf:
            mlog.debug('starting ui')
            runner._ui = await hat.monitor.server.ui.create(
                ui_conf['host'],
                ui_conf['port'],
                runner._server.state,
                set_rank_cb=runner._on_ui_set_rank)
            runner._bind_resource(runner._ui)

        runner.async_group.spawn(runner._runner_loop)

    except BaseException:
        await aio.uncancellable(runner.async_close())
        raise

    return runner


class Runner(aio.Resource):

    @property
    def async_group(self):
        return self._async_group

    async def _on_close(self):
        if self._ui:
            await self._ui.async_close()

        if self._server:
            await self._server.async_close()

        if self._master:
            await self._master.async_close()

        if self._slave:
            await self._slave.async_close()

    async def _on_server_state(self, server, state):
        if self._ui:
            self._ui.set_state(state)

        if self._master:
            await self._master.set_local_components(state.local_components)

        if self._slave and self._slave.is_open:
            with contextlib.suppress(ConnectionError):
                await self._slave.update(state.local_components)

    async def _on_master_global_components(self, master, global_components):
        if self._server and master.is_active:
            await self._server.update(0, global_components)

    async def _on_ui_set_rank(self, ui, cid, rank):
        if self._server:
            await self._server.set_rank(cid, rank)

    async def _on_slave_state(self, slave, state):
        if (self._server and
                self._master and
                not self._master.is_active and
                state.mid is not None):
            await self._server.update(state.mid, state.global_components)

    async def _runner_loop(self):
        try:
            await self._set_master_active(False)

            if not self._slave_parents:
                self._master.set_active(True)
                await self._loop.create_future()

            while True:
                if not self._slave:
                    await self._server.update(0, [])
                    await self._create_slave_loop(
                        self._slave_conf['connect_retry_count'])

                if self._slave and self._slave.is_open:
                    await self._set_master_active(False)
                    await self._slave.wait_closed()

                elif self._slave:
                    await self._slave.async_close()
                    self._slave = None

                else:
                    mlog.debug('no master detected - activating local master')
                    await self._set_master_active(True)
                    await self._create_slave_loop(None)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('runner loop error: %s', e, exc_info=e)

        finally:
            self.close()

    def _bind_resource(self, resource):
        self.async_group.spawn(aio.call_on_done, resource.wait_closing(),
                               self.close)

    def _calculate_blessing(self, master, components):
        return hat.monitor.server.blessing.calculate(
            components=components,
            default_algorithm=self._default_algorithm,
            group_algorithms=self._group_algorithms)

    async def _set_master_active(self, active):
        self._master.set_active(active)
        await self._on_server_state(self._server, self._server.state)

        if active:
            await self._server.update(0, self._master.global_components)

        elif self._slave and self._slave.state.mid is not None:
            await self._server.update(self._slave.state.mid,
                                      self._slave.state.global_components)

    async def _create_slave_loop(self, retry_count):
        counter = (range(retry_count + 1) if retry_count is not None
                   else itertools.repeat(None))

        for count in counter:
            for addr in self._slave_parents:
                with contextlib.suppress(Exception):
                    self._slave = await self._create_slave(addr)
                    return

            if count is None or count < retry_count:
                await asyncio.sleep(self._slave_conf['connect_retry_delay'])

    async def _create_slave(self, addr):
        try:
            return await aio.wait_for(
                hat.monitor.observer.slave.connect(
                    addr,
                    local_components=self._server.state.local_components,
                    state_cb=self._on_slave_state),
                self._slave_conf['connect_timeout'])

        except aio.CancelledWithResultError as e:
            if e.result:
                await aio.uncancellable(e.result.async_close())
            raise
