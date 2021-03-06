import asyncio
import pytest

import hat.juggler
import hat.chatter
import hat.sbs
from hat import util

from test_sys import common


@pytest.mark.timeout(10)
async def test_ui_malformed_message(monitor_factory):
    monitor = monitor_factory()
    incorrect_ui_client = await hat.juggler.connect(
        f'ws://127.0.0.1:{monitor.ui_port}/ws')
    await asyncio.sleep(0.5)

    await incorrect_ui_client.send('JSON serializable data')

    await incorrect_ui_client.wait_closed()

    assert common.process_is_running(monitor.process)

    connections = monitor.process.connections()
    for port in {monitor.ui_port, monitor.monitor_port,
                 monitor.master_port}:
        assert util.first(connections, lambda c: (c.laddr.ip == '0.0.0.0'
                                                  and c.laddr.port == port))


@pytest.mark.timeout(10)
async def test_ui_incorrect_type(cluster_factory):
    cluster = await cluster_factory({
        'group': {
            'components': ['c1', 'c2']}})
    incorrect_ui_client = await hat.juggler.connect(
        f'ws://127.0.0.1:{cluster.server_info.ui_port}/ws')
    await asyncio.sleep(0.5)

    await incorrect_ui_client.send({
        'type': 'incorrect_type',
        'payload': None})

    await incorrect_ui_client.wait_closed()

    server_info = cluster.server_info
    assert common.process_is_running(server_info.process)

    connections = server_info.process.connections()
    for port in {server_info.ui_port, server_info.monitor_port,
                 server_info.master_port}:
        assert util.first(connections, lambda c: (c.laddr.ip == '0.0.0.0'
                                                  and c.laddr.port == port))


@pytest.mark.timeout(10)
async def test_ui_incorrect_cid_mid(cluster_factory):
    cluster = await cluster_factory({
        'group': {
            'components': ['c1', 'c2']}})
    incorrect_ui_client = await hat.juggler.connect(
        f'ws://127.0.0.1:{cluster.server_info.ui_port}/ws')
    await asyncio.sleep(0.5)

    try:
        await incorrect_ui_client.send({
            'type': 'set_rank',
            'payload': {
                'cid': 150,
                'rank': 2}})

        server_info = cluster.server_info
        assert common.process_is_running(server_info.process)

        connections = server_info.process.connections()
        for port in {server_info.ui_port, server_info.monitor_port,
                     server_info.master_port}:
            assert util.first(connections,
                              lambda c: (c.laddr.ip == '0.0.0.0'
                                         and c.laddr.port == port))
    finally:
        await incorrect_ui_client.async_close()


@pytest.mark.timeout(10)
async def test_component_malformed_message(monitor_factory):
    monitor = monitor_factory()
    sbs_repo = hat.sbs.Repository(
        hat.chatter.sbs_repo,
        hat.sbs.Repository("""
            module Test

            T = String
        """))

    incorrect_component_client = await hat.chatter.connect(
        sbs_repo, f'tcp+sbs://127.0.0.1:{monitor.monitor_port}')
    incorrect_component_client.send(hat.chatter.Data(
        module='Test',
        type='T',
        data='Incorrect message'))
    await incorrect_component_client.wait_closed()

    assert common.process_is_running(monitor.process)

    connections = monitor.process.connections()
    for port in {monitor.ui_port, monitor.monitor_port,
                 monitor.master_port}:
        assert util.first(connections, lambda c: (c.laddr.ip == '0.0.0.0'
                                                  and c.laddr.port == port))
