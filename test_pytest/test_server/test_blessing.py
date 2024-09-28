import pytest

from hat.monitor import common
from hat.monitor.server import blessing


generic_blessing = object()
no_blessing = common.BlessingReq(token=None, timestamp=None)
ready = common.BlessingRes(token=None, ready=True)
not_ready = common.BlessingRes(token=None, ready=False)


def component_info(*, cid=0, mid=0, name='', group='', data=None, rank=1,
                   blessing_req=no_blessing, blessing_res=not_ready):
    return common.ComponentInfo(cid=cid,
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


def assert_blessing_req_equal(blessing_req1, blessing_req2):
    if blessing_req1 is generic_blessing:
        assert blessing_req2 != no_blessing

    elif blessing_req2 is generic_blessing:
        assert blessing_req1 != no_blessing

    else:
        assert blessing_req1 == blessing_req2


@pytest.mark.parametrize("algorithm, components, blessing_reqs", [
    (blessing.Algorithm.BLESS_ALL,
     [],
     []),

    (blessing.Algorithm.BLESS_ALL,
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)],
     [generic_blessing
      for g_id in range(3)
      for c_id in range(5)]),

    (blessing.Algorithm.BLESS_ALL,
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}',
                     blessing_req=common.BlessingReq(token=123, timestamp=2.0),
                     blessing_res=ready)
      for g_id in range(3)
      for c_id in range(5)],
     [generic_blessing
      for g_id in range(3)
      for c_id in range(5)]),

    (blessing.Algorithm.BLESS_ALL,
     [component_info(cid=g_id * 10 + c_id, group=f'g{g_id}')
      for g_id in range(3)
      for c_id in range(5)],
     [no_blessing
      for g_id in range(3)
      for c_id in range(5)]),

    (blessing.Algorithm.BLESS_ONE,
     [],
     []),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[1, 1]),
     [generic_blessing, no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[not_ready, ready],
                           ranks=[1, 1]),
     [no_blessing, generic_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[not_ready, not_ready],
                           ranks=[1, 1]),
     [no_blessing, no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[1, 2]),
     [generic_blessing, no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(blessing_reqs=[no_blessing, no_blessing],
                           blessing_ress=[ready, ready],
                           ranks=[2, 1]),
     [no_blessing, generic_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=123, timestamp=2.0),
                        no_blessing],
         blessing_ress=[ready, ready],
         ranks=[1, 1]),
     [common.BlessingReq(token=123, timestamp=2.0), no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, common.BlessingReq(token=123,
                                                        timestamp=2.0)],
         blessing_ress=[ready, ready],
         ranks=[1, 1]),
     [no_blessing, common.BlessingReq(token=123, timestamp=2.0)]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         blessing_ress=[common.BlessingRes(token=123, ready=True),
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=1, timestamp=2.0),
                        no_blessing],
         blessing_ress=[common.BlessingRes(token=123, ready=True),
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=123, timestamp=2.0),
                        no_blessing],
         blessing_ress=[common.BlessingRes(token=123, ready=True),
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [common.BlessingReq(token=123, timestamp=2.0), no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing,
                        common.BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[common.BlessingRes(token=123, ready=True),
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, common.BlessingReq(token=456, timestamp=2.0)]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         blessing_ress=[not_ready, ready],
         ranks=[1, 2]),
     [no_blessing, generic_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing, no_blessing],
         blessing_ress=[not_ready, not_ready],
         ranks=[1, 2]),
     [no_blessing, no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=123, timestamp=2.0),
                        common.BlessingReq(token=456, timestamp=3.0)],
         blessing_ress=[common.BlessingRes(token=123, ready=True),
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [common.BlessingReq(token=123, timestamp=2.0), no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=123, timestamp=2.0),
                        no_blessing],
         blessing_ress=[ready, common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, no_blessing]),

    # if no timestamp, not considered as blessed
    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=123, timestamp=None),
                        common.BlessingReq(token=456, timestamp=3.0)],
         blessing_ress=[ready, ready],
         ranks=[1, 1]),
     [no_blessing, common.BlessingReq(token=456, timestamp=3.0)]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=123, timestamp=3.0),
                        common.BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[common.BlessingRes(token=123, ready=True),
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1]),
     [no_blessing, common.BlessingReq(token=456, timestamp=2.0)]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=123, timestamp=2.0),
                        common.BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[common.BlessingRes(token=123, ready=True),
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1],
         mids=[0, 1]),
     [common.BlessingReq(token=123, timestamp=2.0), no_blessing]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[common.BlessingReq(token=123, timestamp=2.0),
                        common.BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[common.BlessingRes(token=123, ready=True),
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 1],
         mids=[1, 0]),
     [no_blessing, common.BlessingReq(token=456, timestamp=2.0)]),

    (blessing.Algorithm.BLESS_ONE,
     group_component_infos(
         blessing_reqs=[no_blessing,
                        common.BlessingReq(token=456, timestamp=2.0)],
         blessing_ress=[ready,
                        common.BlessingRes(token=456, ready=True)],
         ranks=[1, 2],
         mids=[0, 1]),
     [no_blessing, no_blessing]),
])
def test_bless_all(algorithm, components, blessing_reqs):
    changes = {
        (mid, cid): blessing_req
        for mid, cid, blessing_req in blessing.calculate(
            components=components,
            group_algorithms={},
            default_algorithm=algorithm)}

    for info, blessing_req in zip(components, blessing_reqs):
        result = changes.get((info.mid, info.cid), info.blessing_req)
        assert_blessing_req_equal(result, blessing_req)
