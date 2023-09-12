"""Observer Slave"""

import logging
import typing

from hat import aio
from hat.drivers import chatter
from hat.drivers import tcp

from hat.monitor.observer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

StateCb: typing.TypeAlias = aio.AsyncCallable[['Slave', 'State'], None]
"""State callback"""


class State(typing.NamedTuple):
    mid: int | None
    global_components: list[common.ComponentInfo]


async def connect(addr: tcp.Address,
                  *,
                  local_components: list[common.ComponentInfo] = [],
                  state_cb: StateCb | None = None,
                  **kwargs
                  ) -> 'Slave':
    """Connect to Observer Master

    Additional arguments are passed directly to `hat.drivers.chatter.connect`.

    """
    conn = await chatter.connect(addr, **kwargs)

    try:
        return Slave(conn=conn,
                     local_components=local_components,
                     state_cb=state_cb)

    except Exception:
        await aio.uncancellable(conn.async_close())
        raise


class Slave(aio.Resource):
    """Observer Slave

    For creating new instance of this class see `connect` coroutine.

    """

    def __init__(self,
                 conn: chatter.Connection,
                 local_components: list[common.ComponentInfo],
                 state_cb: StateCb | None):
        self._conn = conn
        self._local_components = local_components
        self._state_cb = state_cb
        self._state = State(mid=None,
                            global_components=[])

        self.async_group.spawn(self._slave_loop)

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._conn.async_group

    @property
    def state(self) -> State:
        """Slave's state"""
        return self._state

    async def update(self, local_components: list[common.ComponentInfo]):
        """Update slaves's local components"""
        if self._local_components == local_components:
            return

        self._local_components = local_components
        await self._send_msg_slave(local_components)

    async def _slave_loop(self):
        mlog.debug('starting slave loop')
        try:
            await self._send_msg_slave(self._local_components)

            while True:
                msg_type, msg_data = await common.receive_msg(self._conn)

                if msg_type != 'HatObserver.MsgMaster':
                    raise Exception('unsupported message type')

                mlog.debug('received msg master')
                components = [common.component_info_from_sbs(i)
                              for i in msg_data['components']]
                self._state = State(mid=msg_data['mid'],
                                    global_components=components)

                if self._state_cb:
                    await aio.call(self._state_cb, self, self._state)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('slave loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('stopping slave loop')
            self.close()

    async def _send_msg_slave(self, local_components):
        await common.send_msg(self._conn, 'HatObserver.MsgSlave', {
            'components': [common.component_info_to_sbs(i)
                           for i in local_components]})
