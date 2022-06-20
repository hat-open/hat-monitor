"""Implementation of blessing calculation algorithms"""

import itertools
import time
import typing

from hat.monitor.server import common


_next_tokens = itertools.count(1)


def calculate(components: typing.List[common.ComponentInfo],
              group_algorithms: typing.Dict[str, common.Algorithm],
              default_algorithm: common.Algorithm
              ) -> typing.List[common.ComponentInfo]:
    """Calculate blessing

    Args:
        components: components state with previous blessing tokens
        group_algorithms: association of algorithm to group
        default_algorithm: default algorithm

    Returns:
        components state with updated blessing

    """
    group_components = {}
    for c in components:
        group_components.setdefault(c.group, []).append(c)

    blessings = {}
    for group, components_from_group in group_components.items():
        algorithm = group_algorithms.get(group, default_algorithm)
        for c in _calculate_group(algorithm, components_from_group):
            blessings[c.mid, c.cid] = c.blessing_req

    return [c._replace(blessing_req=blessings[c.mid, c.cid])
            for c in components]


def _calculate_group(algorithm, components):
    if algorithm == common.Algorithm.BLESS_ALL:
        return _bless_all(components)

    if algorithm == common.Algorithm.BLESS_ONE:
        return _bless_one(components)

    raise ValueError('unsupported algorithm')


def _bless_all(components):
    for c in components:
        if not c.blessing_res.ready:
            yield c._replace(blessing_req=common.BlessingReq(token=None,
                                                             timestamp=None))

        elif _has_blessing(c):
            yield c

        else:
            blessing = common.BlessingReq(token=next(_next_tokens),
                                          timestamp=time.time())
            yield c._replace(blessing_req=blessing)


def _bless_one(components):

    def highlander_battle(highlander, c):
        if not highlander:
            return c
        if c.rank < highlander.rank:
            return c
        if c.rank == highlander.rank:
            if _has_blessing(c) and not _has_blessing(highlander):
                return c
            if _has_blessing(c) and _has_blessing(highlander):
                if (c.blessing_req.timestamp <
                        highlander.blessing_req.timestamp):
                    return c
            if _has_blessing(c) and _has_blessing(highlander) or (
                    not _has_blessing(c) and
                    not _has_blessing(highlander)):
                if c.mid < highlander.mid:
                    return c
        return highlander

    highlander = None
    for c in components:
        if not c.blessing_res.ready:
            continue
        highlander = highlander_battle(highlander, c)

    if highlander:
        for c in components:
            if c.blessing_res.token and c != highlander:
                highlander = None
                break

    for c in components:
        if c != highlander:
            blessing = common.BlessingReq(token=None,
                                          timestamp=None)
            yield c._replace(blessing_req=blessing)

        elif not _has_blessing(c):
            blessing = common.BlessingReq(token=next(_next_tokens),
                                          timestamp=time.time())
            yield c._replace(blessing_req=blessing)

        else:
            yield c


def _has_blessing(component):
    return (component.blessing_req.token and
            component.blessing_req.timestamp)
