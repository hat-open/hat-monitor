"""Slave communication implementation"""

import asyncio
import contextlib
import itertools
import logging
import typing

from hat import aio
from hat import chatter
from hat import json
from hat.monitor.server import common
import hat.monitor.server.master
import hat.monitor.server.server


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class Runner(aio.Resource):
    """Slave/master loop runner

    Args:
        conf: configuration defined by
            ``hat://monitor/main.yaml#/definitions/slave``
        server: local server
        master: local master

    """

    def __init__(self,
                 conf: json.Data,
                 server: hat.monitor.server.server.Server,
                 master: hat.monitor.server.master.Master):
        self._conf = conf
        self._server = server
        self._master = master
        self._async_group = aio.Group()

        self.async_group.spawn(self._run_loop)

    @property
    def async_group(self):
        return self._async_group

    async def _run_loop(self):
        conn = None

        async def cleanup():
            await self._master.set_server(None)
            if conn:
                await conn.async_close()

        try:
            if not self._conf['parents']:
                mlog.debug('no parents - activating local master')
                await self._master.set_server(self._server)
                await self._master.wait_closed()
                return

            while True:
                if not conn or not conn.is_open:
                    conn = await connect(
                        addresses=self._conf['parents'],
                        connect_timeout=self._conf['connect_timeout'],
                        connect_retry_count=self._conf['connect_retry_count'],
                        connect_retry_delay=self._conf['connect_retry_delay'])

                if conn and conn.is_open:
                    mlog.debug('master detected - activating local slave')
                    slave = Slave(self._server, conn)
                    await slave.wait_closed()

                elif conn:
                    await conn.async_close()

                else:
                    mlog.debug('no master detected - activating local master')
                    await self._master.set_server(self._server)
                    conn = await connect(
                        addresses=self._conf['parents'],
                        connect_timeout=self._conf['connect_timeout'],
                        connect_retry_count=None,
                        connect_retry_delay=self._conf['connect_retry_delay'])
                    await self._master.set_server(None)

        except Exception as e:
            mlog.warning('run error: %s', e, exc_info=e)

        finally:
            mlog.debug('stopping run')
            self.close()
            await aio.uncancellable(cleanup())


async def connect(addresses: str,
                  connect_timeout: float,
                  connect_retry_count: typing.Optional[int],
                  connect_retry_delay: float
                  ) -> typing.Optional[chatter.Connection]:
    """Establish connection with remote master"""
    counter = (range(connect_retry_count) if connect_retry_count
               else itertools.repeat(None))
    for count in counter:
        for address in addresses:
            with contextlib.suppress(Exception):
                try:
                    conn = await aio.wait_for(
                        chatter.connect(common.sbs_repo, address),
                        connect_timeout)
                except aio.CancelledWithResultError as e:
                    if e.result:
                        await aio.uncancellable(e.result.async_close())
                    raise
                return conn
        if count is None or count < connect_retry_count - 1:
            await asyncio.sleep(connect_retry_delay)


class Slave(aio.Resource):
    """Slave"""

    def __init__(self,
                 server: hat.monitor.server.server.Server,
                 conn: chatter.Connection):
        self._server = server
        self._conn = conn
        self._components = []

        self.async_group.spawn(self._slave_loop)

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._conn.async_group

    async def _slave_loop(self):
        try:
            mlog.debug('connected to master')

            with self._server.register_change_cb(self._on_server_change):
                self._components = self._server.local_components
                self._send_msg_slave()

                while True:
                    msg = await self._conn.receive()
                    msg_type = msg.data.module, msg.data.type

                    if msg_type != ('HatMonitor', 'MsgMaster'):
                        raise Exception('unsupported message type')

                    msg_master = common.msg_master_from_sbs(msg.data.data)
                    if self._server.is_open:
                        self._server.update(msg_master.mid,
                                            msg_master.components)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.warning('slave loop error: %s', e, exc_info=e)

        finally:
            self.close()
            mlog.debug('connection to master closed')

    def _on_server_change(self):
        if self._components == self._server.local_components:
            return

        self._components = self._server.local_components
        self._send_msg_slave()

    def _send_msg_slave(self):
        msg = common.MsgSlave(components=self._components)
        with contextlib.suppress(ConnectionError):
            self._conn.send(chatter.Data(
                module='HatMonitor',
                type='MsgSlave',
                data=common.msg_slave_to_sbs(msg)))
