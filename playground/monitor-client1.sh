#!/bin/sh

. $(dirname -- "$0")/env.sh

exec $PYTHON << EOF
import asyncio
import contextlib

from hat import aio
from hat.monitor import common
import hat.monitor.client

def main():
    aio.init_asyncio()
    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main())

async def async_main():
    conf ={'name': 'name1',
           'group': 'group',
           'monitor_address': 'tcp+sbs://127.0.0.1:23010'}
    data = {'abcabcabc1': 'abcabcabc1',
            'abcabcabc2': 'abcabcabc2',
            'abcabcabc3': 'abcabcabc3'}
    conn = await hat.monitor.client.connect(conf, data)
    conn.set_blessing_res(common.BlessingRes(None, True))
    try:
        await conn.wait_closing()
    finally:
        await conn.async_close()

main()
EOF
