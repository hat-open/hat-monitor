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
            blessings[c.mid, c.cid] = c.blessing

    return [c._replace(blessing=blessings[c.mid, c.cid])
            for c in components]


def _calculate_group(algorithm, components):
    if algorithm == common.Algorithm.BLESS_ALL:
        return _bless_all(components)

    if algorithm == common.Algorithm.BLESS_ONE:
        return _bless_one(components)

    raise ValueError('unsupported algorithm')


def _bless_all(components):
    for c in components:
        if c.blessing.token is not None:
            yield c

        else:
            blessing = common.Blessing(token=next(_next_tokens),
                                       timestamp=time.now())
            yield c._replace(blessing=blessing)


def _bless_one(components):
    highlander = None
    ready_exist = False
    for c in components:
        if c.ready:
            ready_exist = True
        if c.ready == 0:
            continue
        if highlander and highlander.rank < c.rank:
            continue
        if highlander and highlander.rank == c.rank and (highlander.blessing or
                                                         not c.blessing):
            continue
        highlander = c

    if highlander and not highlander.blessing and ready_exist:
        highlander = None

    for c in components:
        if c != highlander:
            blessing = common.Blessing(token=None,
                                       timestamp=None)
            c = c._replace(blessing=blessing)
        elif c.blessing.token is None:
            blessing = common.Blessing(token=next(_next_tokens),
                                       timestamp=time.now())
            c = c._replace(blessing=blessing)
        yield c
