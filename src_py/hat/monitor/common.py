"""Common functionality shared between clients and monitor server"""

import importlib.resources
import typing

from hat import json
from hat import sbs


with importlib.resources.as_file(importlib.resources.files(__package__) /
                                 'json_schema_repo.json') as _path:
    json_schema_repo: json.SchemaRepository = json.merge_schema_repositories(
        json.json_schema_repo,
        json.decode_file(_path))

with importlib.resources.as_file(importlib.resources.files(__package__) /
                                 'sbs_repo.json') as _path:
    sbs_repo: sbs.Repository = sbs.Repository.from_json(_path)


Cid: typing.TypeAlias = int

Mid: typing.TypeAlias = int


class BlessingReq(typing.NamedTuple):
    token: int | None
    timestamp: float | None


class BlessingRes(typing.NamedTuple):
    token: int | None
    ready: bool


class ComponentInfo(typing.NamedTuple):
    cid: Cid
    mid: Mid
    name: str | None
    group: str | None
    data: json.Data
    rank: int
    blessing_req: BlessingReq
    blessing_res: BlessingRes
