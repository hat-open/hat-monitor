import asyncio
import contextlib
import sys

from hat import aio
from hat.drivers import tcp

import hat.monitor.component


def main():
    aio.init_asyncio()
    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main())


async def async_main():
    addr = tcp.Address(sys.argv[1], int(sys.argv[2]))
    name = sys.argv[3]
    group = sys.argv[4]
    data = {'abcabcabc1': 'abcabcabc1',
            'abcabcabc2': 'abcabcabc2',
            'abcabcabc3': 'abcabcabc3'}

    conn = await hat.monitor.component.connect(addr, name, group, run,
                                               data=data)
    await conn.set_ready(True)

    try:
        await conn.wait_closing()

    finally:
        await conn.async_close()


async def run(component):
    print('>> start')

    try:
        await asyncio.Future()

    finally:
        print('>> stop')


if __name__ == '__main__':
    main()
