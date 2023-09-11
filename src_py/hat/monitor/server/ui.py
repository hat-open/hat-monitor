"""Implementation of web server UI"""

import contextlib
import importlib.resources
import logging
import typing

from hat import aio
from hat import json
from hat import juggler

import hat.monitor.observer.server


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

SetRankCb: typing.TypeAlias = aio.AsyncCallable[['UiServer', int, int], None]
"""Set rank callback (server, cid, rank)"""


async def create(host: str,
                 port: int,
                 state: hat.monitor.observer.server.State,
                 *,
                 set_rank_cb: SetRankCb | None = None,
                 autoflush_delay: float | None = 0.2,
                 shutdown_timeout: float = 0.1
                 ) -> 'UiServer':
    """Create UI server

    `autoflush_delay` and `shutdown_timeout` are passed directy to
    `hat.juggler.listen`.

    """
    server = UiServer()
    server._set_rank_cb = set_rank_cb
    server._state = json.Storage(_state_to_json(state))

    exit_stack = contextlib.ExitStack()
    try:
        ui_path = exit_stack.enter_context(
            importlib.resources.as_file(
                importlib.resources.files(__package__) / 'ui'))

        server._srv = await juggler.listen(host, port,
                                           request_cb=server._on_request,
                                           static_dir=ui_path,
                                           autoflush_delay=autoflush_delay,
                                           shutdown_timeout=shutdown_timeout,
                                           state=server._state)

        try:
            server.async_group.spawn(aio.call_on_cancel, exit_stack.close)

        except BaseException:
            await aio.uncancellable(server.async_close())
            raise

    except BaseException:
        exit_stack.close()
        raise

    return server


class UiServer(aio.Resource):
    """UiServer

    For creating new instance of this class see `create` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    def set_state(self, state: hat.monitor.observer.server.State):
        self._state.set([], _state_to_json(state))

    async def _on_request(self, conn, name, data):
        if name == 'set_rank':
            mlog.debug("received set_rank request")
            if self._set_rank_cb:
                await aio.call(self._set_rank_cb, self, data['cid'],
                               data['rank'])

        else:
            raise Exception('received invalid message type')


def _state_to_json(state):
    return {'mid': state.mid,
            'local_components': list(_get_local_components(state)),
            'global_components': list(_get_global_components(state))}


def _get_local_components(state):
    for i in state.local_components:
        yield {'cid': i.cid,
               'name': i.name,
               'group': i.group,
               'data': i.data,
               'rank': i.rank}


def _get_global_components(state):
    for i in state.global_components:
        yield {'cid': i.cid,
               'mid': i.mid,
               'name': i.name,
               'group': i.group,
               'data': i.data,
               'rank': i.rank,
               'blessing_req': {'token': i.blessing_req.token,
                                'timestamp': i.blessing_req.timestamp},
               'blessing_res': {'token': i.blessing_res.token,
                                'ready': i.blessing_res.ready}}
