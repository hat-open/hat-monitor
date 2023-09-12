"""Observer Client"""

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

StateCb: typing.TypeAlias = aio.AsyncCallable[['Client', 'State'], None]
"""State callback"""

CloseReqCb: typing.TypeAlias = aio.AsyncCallable[['Client'], None]
"""Close request callback"""


class State(typing.NamedTuple):
    """Client state"""
    info: common.ComponentInfo | None
    components: list[common.ComponentInfo]


async def connect(addr: tcp.Address,
                  name: str,
                  group: str,
                  *,
                  data: json.Data = None,
                  state_cb: StateCb | None = None,
                  close_req_cb: CloseReqCb | None = None,
                  **kwargs
                  ) -> 'Client':
    """Connect to Observer Server

    Additional arguments are passed directly to `hat.drivers.chatter.connect`.

    """
    conn = await chatter.connect(addr, **kwargs)

    try:
        return Client(conn=conn,
                      name=name,
                      group=group,
                      data=data,
                      state_cb=state_cb,
                      close_req_cb=close_req_cb)

    except Exception:
        await aio.uncancellable(conn.async_close())
        raise


class Client(aio.Resource):
    """Observer Client

    For creating new client see `connect` coroutine.

    """

    def __init__(self,
                 conn: chatter.Connection,
                 name: str,
                 group: str,
                 data: json.Data,
                 state_cb: StateCb | None,
                 close_req_cb: CloseReqCb | None):
        self._conn = conn
        self._name = name
        self._group = group
        self._data = json.encode(data)
        self._state_cb = state_cb
        self._close_req_cb = close_req_cb
        self._state = State(info=None,
                            components=[])
        self._blessing_res = common.BlessingRes(token=None,
                                                ready=False)

        self.async_group.spawn(self._receive_loop)

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._conn.async_group

    @property
    def state(self) -> State:
        """Client's state"""
        return self._state

    async def set_blessing_res(self, res: common.BlessingRes):
        """Set blessing response"""
        if res == self._blessing_res:
            return

        self._blessing_res = res
        await self._send_msg_client(res)

    async def _receive_loop(self):
        mlog.debug("starting receive loop")
        try:
            await self._send_msg_client(self._blessing_res)

            while True:
                msg_type, msg_data = await common.receive_msg(self._conn)

                if msg_type == 'HatObserver.MsgServer':
                    mlog.debug("received msg server")
                    components = [common.component_info_from_sbs(i)
                                  for i in msg_data['components']]
                    await self._process_msg_server(cid=msg_data['cid'],
                                                   mid=msg_data['mid'],
                                                   components=components)

                elif msg_type == 'HatObserver.MsgClose':
                    mlog.debug("received msg close")
                    if self._close_req_cb:
                        await aio.call(self._close_req_cb, self)
                    break

                else:
                    raise Exception('unsupported message type')

        except ConnectionError:
            mlog.debug("connection closed")

        except Exception as e:
            mlog.warning("monitor client error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping receive loop")
            self.close()

    async def _send_msg_client(self, blessing_res):
        await common.send_msg(self._conn, 'HatObserver.MsgClient', {
            'name': self._name,
            'group': self._group,
            'data': self._data,
            'blessingRes': common.blessing_res_to_sbs(blessing_res)})

    async def _process_msg_server(self, cid, mid, components):
        info = util.first(components, lambda i: (i.cid == cid and
                                                 i.mid == mid))
        state = State(info=info,
                      components=components)
        if self._state == state:
            return

        self._state = state
        if self._state_cb:
            await aio.call(self._state_cb, self, state)
