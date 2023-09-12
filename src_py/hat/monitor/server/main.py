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

conf_suffixes: list[str] = ['.yaml', '.yml', '.toml', '.json']
"""Configuration path suffixes"""


def create_argument_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conf', metavar='PATH', type=Path, default=None,
        help=f"configuration defined by hat-monitor://server.yaml# "
             f"(default $XDG_CONFIG_HOME/hat/monitor"
             f"{{{'|'.join(conf_suffixes)}}})")
    return parser


def main():
    """Monitor Server"""
    parser = create_argument_parser()
    args = parser.parse_args()

    conf_path = args.conf
    if not conf_path:
        for suffix in conf_suffixes:
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

    common.json_schema_repo.validate('hat-monitor://server.yaml#', conf)

    logging.config.dictConfig(conf['log'])

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
