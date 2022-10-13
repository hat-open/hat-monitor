"""Library used by components for communication with Monitor Server

This module provides low-level interface (`connect`/`Client`) and high-level
interface (`Component`) for communication with Monitor Server.

`connect` is used for establishing single chatter based connection
with Monitor Server which is represented by `Client`. Termination of
connection is signaled with `Client.wait_closed`.

Example of low-level interface usage::

    def on_state_change():
        print(f"current global state: {client.components}")

    conf = {'name': 'client1',
            'group': 'group1',
            'monitor_address': 'tcp+sbs://127.0.0.1:23010'}
    client = await hat.monitor.client.connect(conf)
    try:
        with client.register_change_cb(on_state_change):
            await client.wait_closed()
    finally:
        await client.async_close()

`Component` provide high-level interface for communication with
Monitor Server. Component, listens to client changes and, in regard to blessing
and ready, calls or cancels `run_cb` callback. In case component is
ready and blessing token matches, `run_cb` is called. While `run_cb` is
running, once ready or blessing token changes, `run_cb` is canceled. If
`run_cb` finishes or raises exception or connection to monitor server is
closed, component is closed.

Example of high-level interface usage::

    async def run_component(component):
        print("running component")
        try:
            await asyncio.Future()
        finally:
            print("stopping component")

    conf = {'name': 'client',
            'group': 'test clients',
            'monitor_address': 'tcp+sbs://127.0.0.1:23010'}
    client = await hat.monitor.client.connect(conf)
    component = Component(client, run_component)
    component.set_ready(True)
    try:
        await component.wait_closed()
    finally:
        await component.async_close()

"""

import asyncio
import collections
import contextlib
import logging
import typing

from hat import aio
from hat import chatter
from hat import json
from hat import util
from hat.monitor import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

RunCb = typing.Callable[..., typing.Awaitable]
"""Component run callback coroutine

First argument is component instance and remaining arguments are one provided
during component initialization.

"""


async def connect(conf: json.Data,
                  data: json.Data = None
                  ) -> 'Client':
    """Connect to local monitor server

    Connection is established once chatter communication is established.

    Args:
        conf: configuration as defined by ``hat://monitor/client.yaml#``

    """
    client = Client()
    client._conf = conf
    client._data = data
    client._components = []
    client._info = None
    client._blessing_res = common.BlessingRes(token=None,
                                              ready=False)
    client._change_cbs = util.CallbackRegistry()
    client._close_request_cbs = collections.deque()

    client._conn = await chatter.connect(common.sbs_repo,
                                         conf['monitor_address'])
    client.async_group.spawn(client._receive_loop)

    mlog.debug("connected to local monitor server %s", conf['monitor_address'])
    return client


class Client(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._conn.async_group

    @property
    def info(self) -> typing.Optional[common.ComponentInfo]:
        """Client's component info"""
        return self._info

    @property
    def components(self) -> typing.List[common.ComponentInfo]:
        """Global component state"""
        return self._components

    def register_change_cb(self,
                           cb: typing.Callable[[], None]
                           ) -> util.RegisterCallbackHandle:
        """Register change callback

        Registered callback is called once info and/or components changes.

        """
        return self._change_cbs.register(cb)

    def set_blessing_res(self, res: common.BlessingRes):
        """Set blessing response"""
        if res == self._blessing_res:
            return

        self._blessing_res = res
        self._send_msg_client()

    def add_close_request_cb(self, cb: aio.AsyncCallable[[], None]):
        """Add close request callback

        Close request callbacks are called when client receives `MsgClose`.
        Client closes connection after all callbacks finish execution.

        """
        self._close_request_cbs.append(cb)

    async def _receive_loop(self):
        try:
            self._send_msg_client()

            while True:
                msg = await self._conn.receive()
                msg_type = msg.data.module, msg.data.type

                if msg_type == ('HatMonitor', 'MsgServer'):
                    mlog.debug("received MsgServer")
                    msg_server = common.msg_server_from_sbs(msg.data.data)
                    self._process_msg_server(msg_server)

                elif msg_type == ('HatMonitor', 'MsgClose'):
                    mlog.debug("received MsgClose")
                    for cb in self._close_request_cbs:
                        await aio.call(cb)
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

    def _send_msg_client(self):
        msg_client = common.MsgClient(name=self._conf['name'],
                                      group=self._conf['group'],
                                      data=self._data,
                                      blessing_res=self._blessing_res)
        self._conn.send(chatter.Data(
            module='HatMonitor',
            type='MsgClient',
            data=common.msg_client_to_sbs(msg_client)))

    def _process_msg_server(self, msg_server):
        components = msg_server.components
        info = util.first(components, lambda i: (i.cid == msg_server.cid and
                                                 i.mid == msg_server.mid))

        if (self._components == components and self._info == info):
            return

        self._components = components
        self._info = info
        self._change_cbs.notify()


class Component(aio.Resource):
    """Monitor component

    Implementation of component behaviour according to BLESS_ALL and BLESS_ONE
    algorithms.

    Component runs client's loop which manages blessing/ready states based on
    provided monitor client. Initialy, component's ready is disabled.

    When component's ready is enabled and blessing token matches ready token,
    `run_cb` is called with component instance and additionaly provided `args`
    arguments. While `run_cb` is running, if ready enabled state or
    blessing token changes, `run_cb` is canceled.

    If `run_cb` finishes or raises exception, component is closed.

    """

    def __init__(self,
                 client: Client,
                 run_cb: RunCb,
                 *args, **kwargs):
        self._client = client
        self._run_cb = run_cb
        self._args = args
        self._kwargs = kwargs
        self._blessing_res = common.BlessingRes(token=None,
                                                ready=False)
        self._change_queue = aio.Queue()
        self._async_group = client.async_group.create_subgroup()

        client.add_close_request_cb(self.async_close)
        self.async_group.spawn(self._component_loop)

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group

    @property
    def client(self) -> Client:
        """Client"""
        return self._client

    @property
    def ready(self) -> bool:
        """Ready"""
        return self._blessing_res._ready

    def set_ready(self, ready: bool):
        """Set ready"""
        if self._blessing_res.ready == ready:
            return

        self._change_blessing_res(ready=ready)

    def _change_blessing_res(self, **kwargs):
        self._blessing_res = self._blessing_res._replace(**kwargs)
        self._client.set_blessing_res(self._blessing_res)

        with contextlib.suppress(aio.QueueClosedError):
            self._change_queue.put_nowait(None)

    def _on_client_change(self):
        self._change_queue.put_nowait(None)

    async def _component_loop(self):
        try:
            with self._client.register_change_cb(self._on_client_change):
                self._change_blessing_res()

                while True:
                    mlog.debug("waiting blessing and ready")
                    token = await self._get_blessed_and_ready_token()
                    self._change_blessing_res(token=token)

                    try:
                        async with self.async_group.create_subgroup() as subgroup:  # NOQA
                            mlog.debug("running component's run_cb")

                            run_future = subgroup.spawn(
                                self._run_cb, self, *self._args,
                                **self._kwargs)
                            ready_future = subgroup.spawn(
                                self._wait_while_blessed_and_ready)

                            await asyncio.wait(
                                [run_future, ready_future],
                                return_when=asyncio.FIRST_COMPLETED)

                            if run_future.done():
                                return

                    finally:
                        self._change_blessing_res(token=None)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.warning("component loop error: %s", e, exc_info=e)

        finally:
            self.close()

    async def _get_blessed_and_ready_token(self):
        while True:
            if self._blessing_res.ready:
                info = self._client.info
                token = info.blessing_req.token if info else None

                if token is not None:
                    return token

            await self._change_queue.get_until_empty()

    async def _wait_while_blessed_and_ready(self):
        while True:
            if not self._blessing_res.ready:
                return

            info = self._client.info
            token = info.blessing_req.token if info else None

            if token is None or token != self._blessing_res.token:
                return

            await self._change_queue.get_until_empty()
