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


def assert_infos_equal(infos1, infos2):
    assert len(infos1) == len(infos2)
    for info1, info2 in zip(infos1, infos2):
        if info1.blessing_req is generic_blessing:
            if info2.blessing_req.token is not None:
                info1 = info1._replace(blessing_req=info2.blessing_req)

        if info2.blessing_req is generic_blessing:
            if info1.blessing_req.token is not None:
                info2 = info2._replace(blessing_req=info1.blessing_req)

        assert info1 == info2


@pytest.mark.parametrize("components, result", [
    ([], []),

    ([component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)],
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=generic_blessing, blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)]),

    ([component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=BlessingReq(token=123, timestamp=2.0),
                     blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)],
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=generic_blessing, blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)]),

    ([component_info(cid=g_id * 10 + c_id, group=f'g{g_id}')
      for g_id in range(3)
      for c_id in range(5)],
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=no_blessing)
      for g_id in range(3)
      for c_id in range(5)]),
])
def test_bless_all(components, result):
    calculated_result = calculate(components, {}, Algorithm.BLESS_ALL)
    assert_infos_equal(result, calculated_result)


@pytest.mark.parametrize("components, blessing_reqs", [
    ([], []),

    (group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[1, 1]),
     [generic_blessing, no_blessing]),

    (group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[not_ready, ready],
                           ranks=[1, 1]),
     [no_blessing, generic_blessing]),

    (group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[not_ready, not_ready],
                           ranks=[1, 1]),
     [no_blessing, no_blessing]),

    (group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[1, 2]),
     [generic_blessing, no_blessing]),

    (group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[2, 1]),
     [no_blessing, generic_blessing]),

    (group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0), no_blessing],
         blessing_ress=[ready, ready],
         ranks=[1, 1]),
     [BlessingReq(token=123, timestamp=2.0), no_blessing]),

    (group_component_infos(
         blessing_reqs=[no_blessing, BlessingReq(token=123, timestamp=2.0)],
         blessing_ress=[ready, ready],
         ranks=[1, 1]),
     [no_blessing, BlessingReq(token=123, timestamp=2.0)]),

    (group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, no_blessing]),

    (group_component_infos(
         blessing_reqs=[BlessingReq(token=1, timestamp=2.0), no_blessing],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, no_blessing]),

    (group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0), no_blessing],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [BlessingReq(token=123, timestamp=2.0), no_blessing]),

    (group_component_infos(
         blessing_reqs=[no_blessing, BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, BlessingReq(token=456, timestamp=2.0)]),

    (group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         blessing_ress=[not_ready, ready],
         ranks=[1, 2]),
     [no_blessing, generic_blessing]),

    (group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         blessing_ress=[not_ready, not_ready],
         ranks=[1, 2]),
     [no_blessing, no_blessing]),

    (group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0),
                        BlessingReq(token=456, timestamp=3.0)],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [BlessingReq(token=123, timestamp=2.0), no_blessing]),

    (group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0),
                        no_blessing],
         blessing_ress=[ready, BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, no_blessing]),

    # if no timestamp, not considered as blessed
    (group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=None),
                        BlessingReq(token=456, timestamp=3.0)],
         blessing_ress=[ready, ready],
         ranks=[1, 1]),
     [no_blessing, BlessingReq(token=456, timestamp=3.0)]),

    (group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=3.0),
                        BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, BlessingReq(token=456, timestamp=2.0)]),

    (group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0),
                        BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 1],
         mids=[0, 1]),
     [BlessingReq(token=123, timestamp=2.0), no_blessing]),

    (group_component_infos(
         blessing_reqs=[BlessingReq(token=123, timestamp=2.0),
                        BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[BlessingRes(token=123, ready=True),
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 1],
         mids=[1, 0]),
     [no_blessing, BlessingReq(token=456, timestamp=2.0)]),

    (group_component_infos(
         blessing_reqs=[no_blessing,
                        BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[ready,
                        BlessingRes(token=456, ready=True)],
         ranks=[1, 2],
         mids=[0, 1]),
     [no_blessing, no_blessing]),

])
def test_bless_one(components, blessing_reqs):
    calculated_result = calculate(components, {}, Algorithm.BLESS_ONE)
    result = [component_info(cid=component.cid,
                             mid=component.mid,
                             name=component.name,
                             group=component.group,
                             data=component.data,
                             rank=component.rank,
                             blessing_req=blessing_req,
                             blessing_res=component.blessing_res)
              for component, blessing_req in zip(components, blessing_reqs)]
    assert_infos_equal(result, calculated_result)
