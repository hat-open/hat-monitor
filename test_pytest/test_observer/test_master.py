import pytest

from hat import util
from hat.drivers import tcp

import hat.monitor.observer.master


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_listen(addr):
    master = await hat.monitor.observer.master.listen(addr)
    assert master.is_open
    await master.async_close()
