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

from hat.monitor import common
import hat.monitor.server.runner


mlog: logging.Logger = logging.getLogger('hat.monitor.server.main')
"""Module logger"""

user_conf_dir: Path = Path(appdirs.user_config_dir('hat'))
"""User configuration directory path"""


def create_argument_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conf', metavar='PATH', type=Path, default=None,
        help="configuration defined by hat-monitor://server.yaml "
             "(default $XDG_CONFIG_HOME/hat/monitor.{yaml|yml|toml|json})")
    return parser


def main():
    """Monitor Server"""
    parser = create_argument_parser()
    args = parser.parse_args()
    conf = json.read_conf(args.conf, user_conf_dir / 'monitor')
    sync_main(conf)


def sync_main(conf: json.Data):
    """Sync main entry point"""
    aio.init_asyncio()

    common.json_schema_repo.validate('hat-monitor://server.yaml', conf)

    log_conf = conf.get('log')
    if log_conf:
        logging.config.dictConfig(log_conf)

    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main(conf))


async def async_main(conf: json.Data):
    """Async main entry point"""
    runner = None

    async def cleanup():
        if runner:
            await runner.async_close()

        await asyncio.sleep(0.1)

    try:
        runner = await hat.monitor.server.runner.create(conf)
        await runner.wait_closing()

    except Exception as e:
        mlog.warning('async main error: %s', e, exc_info=e)

    finally:
        await aio.uncancellable(cleanup())


if __name__ == '__main__':
    sys.argv[0] = 'hat-monitor'
    sys.exit(main())
