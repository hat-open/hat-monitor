"""Common functionality shared between clients and monitor server"""

import importlib.resources
import typing

from hat import chatter
from hat import json
from hat import sbs


with importlib.resources.path(__package__, 'json_schema_repo.json') as _path:
    json_schema_repo: json.SchemaRepository = json.SchemaRepository(
        json.json_schema_repo,
        json.SchemaRepository.from_json(_path))

with importlib.resources.path(__package__, 'sbs_repo.json') as _path:
    sbs_repo: sbs.Repository = sbs.Repository(
        chatter.sbs_repo,
        sbs.Repository.from_json(_path))


class BlessingReq(typing.NamedTuple):
    token: typing.Optional[int]
    timestamp: typing.Optional[float]


class BlessingRes(typing.NamedTuple):
    token: typing.Optional[int]
    ready: bool


class ComponentInfo(typing.NamedTuple):
    cid: int
    mid: int
    name: typing.Optional[str]
    group: typing.Optional[str]
    data: json.Data
    rank: int
    blessing_req: BlessingReq
    blessing_res: BlessingRes


class MsgClient(typing.NamedTuple):
    name: str
    group: str
    data: json.Data
    blessing_res: BlessingRes


class MsgServer(typing.NamedTuple):
    cid: int
    mid: int
    components: typing.List[ComponentInfo]


def blessing_req_to_sbs(blessing: BlessingReq) -> sbs.Data:
    """Convert blessing request to SBS data"""
    return {'token': _value_to_sbs_optional(blessing.token),
            'timestamp': _value_to_sbs_optional(blessing.timestamp)}


def blessing_req_from_sbs(data: sbs.Data) -> BlessingReq:
    """Convert SBS data to blessing request"""
    return BlessingReq(token=_value_from_sbs_maybe(data['token']),
                       timestamp=_value_from_sbs_maybe(data['timestamp']))


def blessing_res_to_sbs(res: BlessingRes) -> sbs.Data:
    """Convert blessing response to SBS data"""
    return {'token': _value_to_sbs_optional(res.token),
            'ready': res.ready}


def blessing_res_from_sbs(data: sbs.Data) -> BlessingRes:
    """Convert SBS data to blessing response"""
    return BlessingRes(token=_value_from_sbs_maybe(data['token']),
                       ready=data['ready'])


def component_info_to_sbs(info: ComponentInfo) -> sbs.Data:
    """Convert component info to SBS data"""
    return {'cid': info.cid,
            'mid': info.mid,
            'name': _value_to_sbs_optional(info.name),
            'group': _value_to_sbs_optional(info.group),
            'data': json.encode(info.data),
            'rank': info.rank,
            'blessingReq': blessing_req_to_sbs(info.blessing_req),
            'blessingRes': blessing_res_to_sbs(info.blessing_res)}


def component_info_from_sbs(data: sbs.Data) -> ComponentInfo:
    """Convert SBS data to component info"""
    return ComponentInfo(
        cid=data['cid'],
        mid=data['mid'],
        name=_value_from_sbs_maybe(data['name']),
        group=_value_from_sbs_maybe(data['group']),
        data=json.decode(data['data']),
        rank=data['rank'],
        blessing_req=blessing_req_from_sbs(data['blessingReq']),
        blessing_res=blessing_res_from_sbs(data['blessingRes']))


def msg_client_to_sbs(msg: MsgClient) -> sbs.Data:
    """Convert MsgClient to SBS data"""
    return {'name': msg.name,
            'group': msg.group,
            'data': json.encode(msg.data),
            'blessingRes': blessing_res_to_sbs(msg.blessing_res)}


def msg_client_from_sbs(data: sbs.Data) -> MsgClient:
    """Convert SBS data to MsgClient"""
    return MsgClient(name=data['name'],
                     group=data['group'],
                     data=json.decode(data['data']),
                     blessing_res=blessing_res_from_sbs(data['blessingRes']))


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


def _value_to_sbs_optional(value):
    return ('value', value) if value is not None else ('none', None)


def _value_from_sbs_maybe(maybe):
    return maybe[1]
