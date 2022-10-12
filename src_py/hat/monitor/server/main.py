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
    server = None
    master = None
    ui = None
    slave_runner = None

    async def cleanup():
        if ui:
            await ui.async_close()
        if server:
            await server.async_close()
        if slave_runner:
            await slave_runner.async_close()
        if master:
            await master.async_close()
        await async_group.async_close()
        await asyncio.sleep(0.1)

    try:
        mlog.debug('starting server')
        server = await hat.monitor.server.server.create(conf['server'])
        async_group.spawn(aio.call_on_done, server.wait_closing(),
                          async_group.close)

        mlog.debug('starting master')
        master = await hat.monitor.server.master.create(conf['master'])
        async_group.spawn(aio.call_on_done, master.wait_closing(),
                          async_group.close)

        mlog.debug('starting ui')
        ui = await hat.monitor.server.ui.create(conf['ui'], server)
        async_group.spawn(aio.call_on_done, ui.wait_closing(),
                          async_group.close)

        mlog.debug('starting slave runner')
        slave_runner = hat.monitor.server.slave.Runner(conf['slave'], server,
                                                       master)
        async_group.spawn(aio.call_on_done, slave_runner.wait_closing(),
                          async_group.close)

        mlog.debug('monitor started')
        await async_group.wait_closing()

    except Exception as e:
        mlog.warning('async main error: %s', e, exc_info=e)

    finally:
        mlog.debug('stopping monitor')
        await aio.uncancellable(cleanup())


if __name__ == '__main__':
    sys.argv[0] = 'hat-monitor'
    sys.exit(main())
