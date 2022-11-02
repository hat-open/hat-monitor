"""Implementation of web server UI"""

import contextlib
import functools
import importlib.resources
import logging
import urllib

from hat import aio
from hat import json
from hat import juggler
import hat.monitor.server.server


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

autoflush_delay: float = 0.2
"""Juggler autoflush delay"""


async def create(conf: json.Data,
                 server: hat.monitor.server.server.Server
                 ) -> 'WebServer':
    """Create user interface

    Args:
        conf: configuration defined by
            ``hat-monitor://main.yaml#/definitions/ui``
        server: local monitor server

    """
    srv = WebServer()
    srv._server = server

    exit_stack = contextlib.ExitStack()
    try:
        ui_path = exit_stack.enter_context(
            importlib.resources.path(__package__, 'ui'))

        state = json.Storage()
        update_state = functools.partial(_update_state, state, server)
        exit_stack.enter_context(server.register_change_cb(update_state))
        update_state()

        addr = urllib.parse.urlparse(conf['address'])
        srv._srv = await juggler.listen(host=addr.hostname,
                                        port=addr.port,
                                        request_cb=srv._on_request,
                                        static_dir=ui_path,
                                        autoflush_delay=autoflush_delay,
                                        state=state)

        try:
            srv.async_group.spawn(aio.call_on_cancel, exit_stack.close)

        except BaseException:
            await aio.uncancellable(srv.async_close())
            raise

    except BaseException:
        exit_stack.close()
        raise

    mlog.debug("web server listening on %s", conf['address'])
    return srv


class WebServer(aio.Resource):
    """WebServer

    For creating new instance of this class see `create` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    async def _on_request(self, conn, name, data):
        if name == 'set_rank':
            mlog.debug("received set_rank request")
            self._server.set_rank(data['cid'], data['rank'])

        else:
            raise Exception('received invalid message type')


def _update_state(state, server):
    local_components = [{'cid': i.cid,
                         'name': i.name,
                         'group': i.group,
                         'data': i.data,
                         'rank': i.rank}
                        for i in server.local_components]
    global_components = [
        {'cid': i.cid,
         'mid': i.mid,
         'name': i.name,
         'group': i.group,
         'data': i.data,
         'rank': i.rank,
         'blessing_req': {'token': i.blessing_req.token,
                          'timestamp': i.blessing_req.timestamp},
         'blessing_res': {'token': i.blessing_res.token,
                          'ready': i.blessing_res.ready}}
        for i in server.global_components]
    state.set([], {'mid': server.mid,
                   'local_components': local_components,
                   'global_components': global_components})
