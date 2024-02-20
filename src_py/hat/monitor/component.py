"""Monitor Component"""

import asyncio
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

Runner: typing.TypeAlias = aio.Resource
"""Component runner"""

RunnerCb: typing.TypeAlias = aio.AsyncCallable[['Component'], Runner]
"""Runner callback"""

StateCb: typing.TypeAlias = aio.AsyncCallable[['Component', State], None]
"""State callback"""

CloseReqCb: typing.TypeAlias = aio.AsyncCallable[['Component'], None]
"""Close request callback"""


async def connect(addr: tcp.Address,
                  name: str,
                  group: str,
                  runner_cb: RunnerCb,
                  *,
                  data: json.Data = None,
                  state_cb: StateCb | None = None,
                  close_req_cb: CloseReqCb | None = None,
                  **kwargs
                  ) -> 'Component':
    """Connect to local monitor server and create component

    Implementation of component behavior according to BLESS_ALL and BLESS_ONE
    algorithms.

    Component runs client's loop which manages blessing req/res states based on
    provided monitor client. Initially, component's ready is disabled.

    Component is considered active when component's ready is ``True`` and
    blessing req/res tokens are matching.

    When component becomes active, `component_cb` is called. Result of calling
    `component_cb` should be runner representing user defined components
    activity. Once component stops being active, runner is closed. If
    component becomes active again, `component_cb` call is repeated.

    If runner is closed, while component remains active, component is closed.

    If connection to Monitor Server is closed, component is also closed.
    If component is closed while active, runner is closed.

    Additional arguments are passed to `hat.monitor.observer.client.connect`.

    """
    component = Component()
    component._runner_cb = runner_cb
    component._state_cb = state_cb
    component._close_req_cb = close_req_cb
    component._blessing_res = common.BlessingRes(token=None,
                                                 ready=False)
    component._change_event = asyncio.Event()

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
    """Monitor Component

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
        self._change_event.set()

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

        self._change_event.set()

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
                    mlog.debug("creating component runner")
                    runner = await aio.call(self._runner_cb, self)

                    try:
                        async with self.async_group.create_subgroup() as subgroup:  # NOQA
                            blessed_and_ready_task = subgroup.spawn(
                                self._wait_while_blessed_and_ready)
                            runner_closing_task = subgroup.spawn(
                                runner.wait_closing)

                            mlog.debug("wait while blessed and ready")
                            await asyncio.wait(
                                [blessed_and_ready_task, runner_closing_task],
                                return_when=asyncio.FIRST_COMPLETED)

                            if (runner_closing_task.done() and
                                    not blessed_and_ready_task.done()):
                                mlog.debug(
                                    "runner closed while blessed and ready")
                                break

                    finally:
                        mlog.debug("closing component runner")
                        await aio.uncancellable(runner.async_close())

                finally:
                    await self._change_blessing_res(token=None)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.warning("component loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping component loop")
            self.close()

    async def _get_blessed_and_ready_token(self):
        while True:
            if self._blessing_res.ready:
                info = self._client.state.info
                token = info.blessing_req.token if info else None

                if token is not None:
                    return token

            await self._change_event.wait()
            self._change_event.clear()

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

            await self._change_event.wait()
            self._change_event.clear()

    async def _wait_while_blessed_and_ready(self):
        while True:
            if not self._blessing_res.ready:
                return

            info = self._client.state.info
            token = info.blessing_req.token if info else None

            if token is None or token != self._blessing_res.token:
                return

            await self._change_event.wait()
            self._change_event.clear()
