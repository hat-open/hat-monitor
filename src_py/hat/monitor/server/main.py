"""Monitor server main"""

from pathlib import Path
import argparse
import asyncio
import contextlib
import logging.config
import sys

import appdirs

from hat import aio
from hat import json
from hat.monitor.server import common
import hat.monitor.server.master
import hat.monitor.server.server
import hat.monitor.server.slave
import hat.monitor.server.ui


mlog: logging.Logger = logging.getLogger('hat.monitor.server.main')
"""Module logger"""

user_conf_dir: Path = Path(appdirs.user_config_dir('hat'))
"""User configuration directory path"""


def create_argument_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conf', metavar='PATH', type=Path, default=None,
        help="configuration defined by hat-monitor://main.yaml# "
             "(default $XDG_CONFIG_HOME/hat/monitor.{yaml|yml|json})")
    return parser


def main():
    """Monitor Server"""
    parser = create_argument_parser()
    args = parser.parse_args()

    conf_path = args.conf
    if not conf_path:
        for suffix in ('.yaml', '.yml', '.json'):
            conf_path = (user_conf_dir / 'monitor').with_suffix(suffix)
            if conf_path.exists():
                break

    if conf_path == Path('-'):
        conf = json.decode_stream(sys.stdin)
    else:
        conf = json.decode_file(conf_path)

    sync_main(conf)


def sync_main(conf: json.Data):
    """Sync main entry point"""
    aio.init_asyncio()

    common.json_schema_repo.validate('hat-monitor://main.yaml#', conf)

    logging.config.dictConfig(conf['log'])

    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main(conf))


async def async_main(conf: json.Data):
    """Async main entry point"""
    async_group = aio.Group()
    async_group.spawn(aio.call_on_cancel, asyncio.sleep, 0.1)

    try:
        mlog.debug('starting server')
        server = await hat.monitor.server.server.create(conf['server'])
        async_group.spawn(aio.call_on_cancel, server.async_close)

        mlog.debug('starting master')
        master = await hat.monitor.server.master.create(conf['master'])
        async_group.spawn(aio.call_on_cancel, master.async_close)

        mlog.debug('starting ui')
        ui = await hat.monitor.server.ui.create(conf['ui'], server)
        async_group.spawn(aio.call_on_cancel, ui.async_close)

        mlog.debug('starting slave')
        slave_future = async_group.spawn(hat.monitor.server.slave.run,
                                         conf['slave'], server, master)

        mlog.debug('monitor started')
        for f in [server.wait_closing(),
                  master.wait_closing(),
                  ui.wait_closing(),
                  slave_future]:
            async_group.spawn(aio.call_on_done, f, async_group.close)

        await async_group.wait_closing()

    except Exception as e:
        mlog.warning('async main error: %s', e, exc_info=e)

    finally:
        mlog.debug('stopping monitor')
        await aio.uncancellable(async_group.async_close())


if __name__ == '__main__':
    sys.argv[0] = 'hat-monitor'
    sys.exit(main())
