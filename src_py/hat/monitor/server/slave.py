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


async def run(conf: json.Data,
              server: hat.monitor.server.server.Server,
              master: hat.monitor.server.master.Master):
    """Run slave/master loop

    Args:
        conf: configuration defined by
            ``hat://monitor/main.yaml#/definitions/slave``
        server: local server
        master: local master

    """
    conn = None

    async def cleanup():
        await master.set_server(None)
        if conn:
            await conn.async_close()

    try:
        if not conf['parents']:
            mlog.debug('no parents - activating local master')
            await master.set_server(server)
            await master.wait_closed()
            return

        while True:
            if not conn or not conn.is_open:
                conn = await connect(
                    addresses=conf['parents'],
                    connect_timeout=conf['connect_timeout'],
                    connect_retry_count=conf['connect_retry_count'],
                    connect_retry_delay=conf['connect_retry_delay'])

            if conn and conn.is_open:
                mlog.debug('master detected - activating local slave')
                slave = Slave(server, conn)
                await slave.wait_closed()

            elif conn:
                await conn.async_close()

            else:
                mlog.debug('no master detected - activating local master')
                await master.set_server(server)
                conn = await connect(
                    addresses=conf['parents'],
                    connect_timeout=conf['connect_timeout'],
                    connect_retry_count=None,
                    connect_retry_delay=conf['connect_retry_delay'])
                await master.set_server(None)

    except Exception as e:
        mlog.warning('run error: %s', e, exc_info=e)

    finally:
        mlog.debug('stopping run')
        await aio.uncancellable(cleanup())


async def connect(addresses: str,
                  connect_timeout: float,
                  connect_retry_count: typing.Optional[int],
                  connect_retry_delay: float
                  ) -> typing.Optional[chatter.Connection]:
    """Establish connection with remote master"""
    counter = (range(connect_retry_count) if connect_retry_count
               else itertools.repeat(None))
    for _ in counter:
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
        self.async_group.spawn(aio.call_on_done, server.wait_closing(),
                               self.close)

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
                    self._server.update(msg_master.mid, msg_master.components)

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
