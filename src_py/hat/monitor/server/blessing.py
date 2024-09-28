"""Implementation of blessing calculation algorithms"""

from collections.abc import Iterable
import collections
import enum
import itertools
import time

from hat.monitor import common


_next_tokens = itertools.count(1)


class Algorithm(enum.Enum):
    BLESS_ALL = 'BLESS_ALL'
    BLESS_ONE = 'BLESS_ONE'


def calculate(components: Iterable[common.ComponentInfo],
              group_algorithms: dict[str, Algorithm],
              default_algorithm: Algorithm
              ) -> Iterable[tuple[common.Mid, common.Cid, common.BlessingReq]]:
    """Calculate blessing request changes

    Args:
        components: components state with previous blessing tokens
        group_algorithms: association of algorithm to group
        default_algorithm: default algorithm

    Returns:
        blessing request changes

    """
    group_components = collections.defaultdict(collections.deque)
    for c in components:
        group_components[c.group].append(c)

    for group, components_from_group in group_components.items():
        algorithm = group_algorithms.get(group, default_algorithm)

        yield from _calculate_group(algorithm, components_from_group)


def _calculate_group(algorithm, components):
    if algorithm == Algorithm.BLESS_ALL:
        yield from _bless_all(components)

    elif algorithm == Algorithm.BLESS_ONE:
        yield from _bless_one(components)

    else:
        raise ValueError('unsupported algorithm')


def _bless_all(components):
    for c in components:
        if not c.blessing_res.ready:
            blessing_req = common.BlessingReq(token=None,
                                              timestamp=None)

        elif _has_blessing(c):
            blessing_req = c.blessing_req

        else:
            blessing_req = common.BlessingReq(token=next(_next_tokens),
                                              timestamp=time.time())

        if c.blessing_req != blessing_req:
            yield c.mid, c.cid, blessing_req


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

    if highlander and not (highlander.blessing_res.token and
                           highlander.blessing_res.token ==
                           highlander.blessing_req.token):
        for c in components:
            if c.blessing_res.token and c != highlander:
                highlander = None
                break

    for c in components:
        if c != highlander:
            blessing_req = common.BlessingReq(token=None,
                                              timestamp=None)

        elif not _has_blessing(c):
            blessing_req = common.BlessingReq(token=next(_next_tokens),
                                              timestamp=time.time())

        else:
            blessing_req = c.blessing_req

        if c.blessing_req != blessing_req:
            yield c.mid, c.cid, blessing_req


def _has_blessing(component):
    return (component.blessing_req.token and
            component.blessing_req.timestamp)
