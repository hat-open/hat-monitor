import pytest

from hat.monitor.common import ComponentInfo, BlessingReq, BlessingRes
from hat.monitor.server.blessing import calculate
from hat.monitor.server.common import Algorithm


generic_blessing = object()
no_blessing = BlessingReq(token=None, timestamp=None)
ready = BlessingRes(token=None, ready=True)
not_ready = BlessingRes(token=None, ready=False)


def component_info(*, cid=0, mid=0, name='', group='', data=None, rank=1,
                   blessing_req=no_blessing, blessing_res=not_ready):
    return ComponentInfo(cid=cid,
                         mid=mid,
                         name=name,
                         group=group,
                         data=data,
                         rank=rank,
                         blessing_req=blessing_req,
                         blessing_res=blessing_res)


def group_component_infos(blessing_reqs, ranks, *, mids=None,
                          blessing_ress=None, starting_cid=0, group=''):
    mids = mids or [0] * len(blessing_reqs)
    blessing_ress = blessing_ress or ([not_ready] * len(blessing_reqs))
    temp_iter = enumerate(zip(mids, blessing_reqs, ranks, blessing_ress))
    return [component_info(cid=starting_cid+i,
                           mid=mid,
                           group=group,
                           rank=rank,
                           blessing_req=blessing_req,
                           blessing_res=blessing_res)
            for i, (mid, blessing_req, rank, blessing_res) in temp_iter]


@pytest.mark.parametrize("algorithm, components, result", [
    (Algorithm.BLESS_ALL, [], []),

    (Algorithm.BLESS_ONE, [], []),

    (Algorithm.BLESS_ALL,
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)],
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=generic_blessing, blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)]),

    (Algorithm.BLESS_ALL,
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=BlessingReq(token=123, timestamp=2.0),
                     blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)],
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=generic_blessing, blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)]),

    (Algorithm.BLESS_ALL,
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}')
      for g_id in range(3)
      for c_id in range(5)],
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=no_blessing)
      for g_id in range(3)
      for c_id in range(5)]),

    (Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[1, 1]),
     group_component_infos(blessing_reqs=[generic_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[1, 1])),

    (Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[not_ready, ready],
                           ranks=[1, 1]),
     group_component_infos(blessing_reqs=[no_blessing, generic_blessing],
                           blessing_ress=[not_ready, ready],
                           ranks=[1, 1])),

    (Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[not_ready, not_ready],
                           ranks=[1, 1]),
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[not_ready, not_ready],
                           ranks=[1, 1])),

    (Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[1, 2]),
     group_component_infos(blessing_reqs=[generic_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[1, 2])),

    (Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[2, 1]),
     group_component_infos(blessing_reqs=[no_blessing, generic_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[2, 1])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0), no_blessing],
         blessing_ress=[ready, ready],
         ranks=[1, 1]),
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0), no_blessing],
         blessing_ress=[ready, ready],
         ranks=[1, 1])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, BlessingReq(token=123, timestamp=2.0)],
         blessing_ress=[ready, ready],
         ranks=[1, 1]),
     group_component_infos(
         blessing_reqs=[no_blessing, BlessingReq(token=123, timestamp=2.0)],
         blessing_ress=[ready, ready],
         ranks=[1, 1])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)]),
     group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0), no_blessing],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)]),
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0), no_blessing],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, BlessingReq(token=456, timestamp=2.0)],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)]),
     group_component_infos(
         blessing_reqs=[no_blessing, BlessingReq(token=456, timestamp=2.0)],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         ranks=[1, 2],
         blessing_ress=[not_ready, ready]),
     group_component_infos(
         blessing_reqs=[no_blessing, generic_blessing],
         ranks=[1, 2],
         blessing_ress=[not_ready, ready])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         ranks=[1, 2],
         blessing_ress=[not_ready, not_ready]),
     group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         ranks=[1, 2],
         blessing_ress=[not_ready, not_ready])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0),
                        BlessingReq(token=456, timestamp=3.0)],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)]),
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0), no_blessing],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=3.0),
                        BlessingReq(token=456, timestamp=2.0)],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)]),
     group_component_infos(
         blessing_reqs=[no_blessing, BlessingReq(token=456, timestamp=2.0)],
         ranks=[1, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0),
                        BlessingReq(token=456, timestamp=2.0)],
         ranks=[1, 1],
         mids=[0, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)]),
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0), no_blessing],
         ranks=[1, 1],
         mids=[0, 1],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)])),

    (Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0),
                        BlessingReq(token=456, timestamp=2.0)],
         ranks=[1, 1],
         mids=[1, 0],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)]),
     group_component_infos(
         blessing_reqs=[no_blessing, BlessingReq(token=456, timestamp=2.0)],
         ranks=[1, 1],
         mids=[1, 0],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)])),
])
def test_calculate_blessing(algorithm, components, result):
    calculated_result = calculate(components, {}, algorithm)
    assert len(calculated_result) == len(result)
    for c1, c2 in zip(calculated_result, result):
        if (c2.blessing_req is generic_blessing
                and c1.blessing_req.token is not None):
            c2 = c2._replace(blessing_req=c1.blessing_req)
        assert c1 == c2
