"""Monitor Component"""

import asyncio
import contextlib
import logging
import typing

from hat import aio
from hat import json
from hat.drivers import tcp

from hat.monitor import common
from hat.monitor.observer import client


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

State: typing.TypeAlias = client.State
"""Component state"""

RunCb: typing.TypeAlias = typing.Callable[['Component'],
                                          typing.Awaitable[None]]
"""Component run callback coroutine"""

StateCb: typing.TypeAlias = aio.AsyncCallable[['Component', State], None]
"""State callback"""

CloseReqCb: typing.TypeAlias = aio.AsyncCallable[['Component'], None]
"""Close request callback"""


async def connect(addr: tcp.Address,
                  name: str,
                  group: str,
                  run_cb: RunCb,
                  *,
                  data: json.Data = None,
                  state_cb: StateCb | None = None,
                  close_req_cb: CloseReqCb | None = None,
                  **kwargs
                  ) -> 'Component':
    """Connect to local monitor server and create component

    Implementation of component behaviour according to BLESS_ALL and BLESS_ONE
    algorithms.

    Component runs client's loop which manages blessing/ready states based on
    provided monitor client. Initialy, component's ready is disabled.

    When component's ready is enabled and blessing token matches ready token,
    `run_cb` is called with component instance and additionaly provided `args`
    arguments. While `run_cb` is running, if ready enabled state or
    blessing token changes, `run_cb` is canceled.

    If `run_cb` finishes or raises exception, component is closed.

    Additional arguments are passed to `hat.drivers.chatter.connect`.

    """
    component = Component()
    component._run_cb = run_cb
    component._state_cb = state_cb
    component._close_req_cb = close_req_cb
    component._blessing_res = common.BlessingRes(token=None,
                                                 ready=False)
    component._change_queue = aio.Queue()

    component._client = await client.connect(
        addr, name, group,
        data=data,
        state_cb=component._on_client_state,
        close_req_cb=component._on_client_close_req,
        **kwargs)

    try:
        component.async_group.spawn(component._component_loop)

    except Exception:
        await aio.uncancellable(component._client.async_close())
        raise

    return component


class Component(aio.Resource):
    """Monitor component

    For creating new component see `connect` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._client.async_group

    @property
    def state(self) -> State:
        """Component's state"""
        return self._client.state

    @property
    def ready(self) -> bool:
        """Ready"""
        return self._blessing_res.ready

    async def set_ready(self, ready: bool):
        """Set ready"""
        if self._blessing_res.ready == ready:
            return

        await self._change_blessing_res(ready=ready)

    async def _on_client_state(self, c, state):
        with contextlib.suppress(aio.QueueClosedError):
            self._change_queue.put_nowait(None)

        if not self._state_cb:
            return

        await aio.call(self._state_cb, self, state)

    async def _on_client_close_req(self, c):
        if not self._close_req_cb:
            return

        await aio.call(self._close_req_cb, self)

    async def _change_blessing_res(self, **kwargs):
        self._blessing_res = self._blessing_res._replace(**kwargs)
        await self._client.set_blessing_res(self._blessing_res)

        with contextlib.suppress(aio.QueueClosedError):
            self._change_queue.put_nowait(None)

    async def _component_loop(self):
        mlog.debug("starting component loop")
        try:
            await self._change_blessing_res()

            while True:
                mlog.debug("waiting blessing and ready")
                token = await self._get_blessed_and_ready_token()

                if self._blessing_res.token != token:
                    await self._change_blessing_res(token=token)

                ready = await self._wait_blessed_and_ready_token()
                if not ready:
                    continue

                try:
                    async with self.async_group.create_subgroup() as subgroup:
                        mlog.debug("running component's run_cb")

                        run_task = subgroup.spawn(self._run_cb, self)
                        ready_task = subgroup.spawn(
                            self._wait_while_blessed_and_ready)

                        await asyncio.wait([run_task, ready_task],
                                           return_when=asyncio.FIRST_COMPLETED)

                        if run_task.done():
                            return

                finally:
                    await self._change_blessing_res(token=None)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.warning("component loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping component loop")
            self.close()
            self._change_queue.close()

    async def _get_blessed_and_ready_token(self):
        while True:
            if self._blessing_res.ready:
                info = self._client.state.info
                token = info.blessing_req.token if info else None

                if token is not None:
                    return token

            await self._change_queue.get_until_empty()

    async def _wait_blessed_and_ready_token(self):
        while True:
            if not self._blessing_res.ready:
                return False

            if self._blessing_res.token is None:
                return False

            info = self._client.state.info
            token = info.blessing_res.token if info else None

            if token == self._blessing_res.token:
                return token == info.blessing_req.token

            await self._change_queue.get_until_empty()

    async def _wait_while_blessed_and_ready(self):
        while True:
            if not self._blessing_res.ready:
                return

            info = self._client.state.info
            token = info.blessing_req.token if info else None

            if token is None or token != self._blessing_res.token:
                return

            await self._change_queue.get_until_empty()
