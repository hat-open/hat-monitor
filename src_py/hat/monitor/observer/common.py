from hat.monitor.common import *  # NOQA

from hat import json
from hat import sbs
from hat.drivers import chatter

from hat.monitor.common import (BlessingReq,
                                BlessingRes,
                                ComponentInfo,
                                sbs_repo)


async def send_msg(conn: chatter.Connection,
                   msg_type: str,
                   msg_data: sbs.Data):
    """Send Observer message"""
    msg = sbs_repo.encode(msg_type, msg_data)
    await conn.send(chatter.Data(msg_type, msg))


async def receive_msg(conn: chatter.Connection) -> tuple[str, sbs.Data]:
    """Receive Observer message"""
    msg = await conn.receive()
    msg_data = sbs_repo.decode(msg.data.type, msg.data.data)
    return msg.data.type, msg_data


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


def _value_to_sbs_optional(value):
    return ('value', value) if value is not None else ('none', None)


def _value_from_sbs_maybe(maybe):
    return maybe[1]
