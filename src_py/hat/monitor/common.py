"""Common functionality shared between clients and monitor server"""

from pathlib import Path
import typing

from hat import chatter
from hat import json
from hat import sbs


package_path = Path(__file__).parent

json_schema_repo: json.SchemaRepository = json.SchemaRepository(
    json.json_schema_repo,
    json.SchemaRepository.from_json(package_path / 'json_schema_repo.json'))

sbs_repo: sbs.Repository = sbs.Repository(
    chatter.sbs_repo,
    sbs.Repository.from_json(package_path / 'sbs_repo.json'))


class Blessing(typing.NamedTuple):
    token: typing.Optional[int]
    timestamp: typing.Optional[float]


class Ready(typing.NamedTuple):
    token: typing.Optional[int]
    enabled: bool


class ComponentInfo(typing.NamedTuple):
    cid: int
    mid: int
    name: typing.Optional[str]
    group: typing.Optional[str]
    data: json.Data
    rank: int
    blessing: Blessing
    ready: Ready


class MsgClient(typing.NamedTuple):
    name: str
    group: str
    data: json.Data
    ready: Ready


class MsgServer(typing.NamedTuple):
    cid: int
    mid: int
    components: typing.List[ComponentInfo]


def blessing_to_sbs(blessing: Blessing) -> sbs.Data:
    """Convert blessing to SBS data"""
    return {'token': _value_to_sbs_maybe(blessing.token),
            'timestamp': _value_to_sbs_maybe(blessing.timestamp)}


def blessing_from_sbs(data: sbs.Data) -> Blessing:
    """Convert SBS data to blessing"""
    return Blessing(token=_value_from_sbs_maybe(data['token']),
                    timestamp=_value_from_sbs_maybe(data['timestamp']))


def ready_to_sbs(ready: Ready) -> sbs.Data:
    """Convert ready to SBS data"""
    return {'token': _value_to_sbs_maybe(ready.token),
            'enabled': ready.enabled}


def ready_from_sbs(data: sbs.Data) -> Blessing:
    """Convert SBS data to ready"""
    return Blessing(token=_value_from_sbs_maybe(data['token']),
                    enabled=data['enabled'])


def component_info_to_sbs(info: ComponentInfo) -> sbs.Data:
    """Convert component info to SBS data"""
    return {'cid': info.cid,
            'mid': info.mid,
            'name': _value_to_sbs_maybe(info.name),
            'group': _value_to_sbs_maybe(info.group),
            'data': json.encode(info.data),
            'rank': info.rank,
            'blessing': blessing_to_sbs(info.blessing),
            'ready': ready_to_sbs(info.ready)}


def component_info_from_sbs(data: sbs.Data) -> ComponentInfo:
    """Convert SBS data to component info"""
    return ComponentInfo(cid=data['cid'],
                         mid=data['mid'],
                         name=_value_from_sbs_maybe(data['name']),
                         group=_value_from_sbs_maybe(data['group']),
                         data=json.decode(data['data']),
                         rank=data['rank'],
                         blessing=blessing_from_sbs(data['blessing']),
                         ready=ready_from_sbs(data['ready']))


def msg_client_to_sbs(msg: MsgClient) -> sbs.Data:
    """Convert MsgClient to SBS data"""
    return {'name': msg.name,
            'group': msg.group,
            'data': json.encode(msg.data),
            'ready': ready_to_sbs(msg.ready)}


def msg_client_from_sbs(data: sbs.Data) -> MsgClient:
    """Convert SBS data to MsgClient"""
    return MsgClient(name=data['name'],
                     group=data['group'],
                     data=json.decode(data['data']),
                     ready=ready_from_sbs(data['ready']))


def msg_server_to_sbs(msg: MsgServer) -> sbs.Data:
    """Convert MsgServer to SBS data"""
    return {'cid': msg.cid,
            'mid': msg.mid,
            'components': [component_info_to_sbs(info)
                           for info in msg.components]}


def msg_server_from_sbs(data: sbs.Data) -> MsgServer:
    """Convert SBS data to MsgServer"""
    return MsgServer(cid=data['cid'],
                     mid=data['mid'],
                     components=[component_info_from_sbs(info)
                                 for info in data['components']])


def _value_to_sbs_maybe(value):
    return ('Just', value) if value is not None else ('Nothing', None)


def _value_from_sbs_maybe(maybe):
    return maybe[1]
