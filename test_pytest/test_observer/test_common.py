import pytest

from hat.monitor.observer import common


@pytest.mark.parametrize('token', [None, 0, 1, 42])
@pytest.mark.parametrize('timestamp', [None, 12345, 42.5])
def test_encode_decode_blessing_req(token, timestamp):
    req = common.BlessingReq(token=token,
                             timestamp=timestamp)

    encoded = common.blessing_req_to_sbs(req)
    decoded = common.blessing_req_from_sbs(encoded)

    assert req == decoded


@pytest.mark.parametrize('token', [None, 0, 1, 42])
@pytest.mark.parametrize('ready', [True, False])
def test_encode_decode_blessing_res(token, ready):
    res = common.BlessingRes(token=token,
                             ready=ready)

    encoded = common.blessing_res_to_sbs(res)
    decoded = common.blessing_res_from_sbs(encoded)

    assert res == decoded


@pytest.mark.parametrize('cid', [0, 42])
@pytest.mark.parametrize('mid', [0, 24])
@pytest.mark.parametrize('name', [None, 'name'])
@pytest.mark.parametrize('group', [None, 'group'])
@pytest.mark.parametrize('data', [None, {'abc': [1, 2, True]}])
@pytest.mark.parametrize('rank', [123, 321])
@pytest.mark.parametrize('req_token', [None, 123])
@pytest.mark.parametrize('req_timestamp', [None, 12345])
@pytest.mark.parametrize('res_token', [None, 42])
@pytest.mark.parametrize('res_ready', [True, False])
def test_encode_decode_component_info(cid, mid, name, group, data, rank,
                                      req_token, req_timestamp, res_token,
                                      res_ready):
    req = common.BlessingReq(token=req_token,
                             timestamp=req_timestamp)
    res = common.BlessingRes(token=res_token,
                             ready=res_ready)
    info = common.ComponentInfo(cid=cid,
                                mid=mid,
                                name=name,
                                group=group,
                                data=data,
                                rank=rank,
                                blessing_req=req,
                                blessing_res=res)

    encoded = common.component_info_to_sbs(info)
    decoded = common.component_info_from_sbs(encoded)

    assert info == decoded
