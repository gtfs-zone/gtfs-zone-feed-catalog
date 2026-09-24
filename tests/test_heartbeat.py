"""The Gatus push: its URL and auth, and that it never fails the run."""

import httpx
import respx

from geometry_car.heartbeat import push_heartbeat
from geometry_car.settings import Settings

PUSH = "http://gatus:8080/api/v1/endpoints/data_catalog-publish/external"


def configured() -> Settings:
    return Settings(_env_file=None, gatus_url="http://gatus:8080/", gatus_token="t")


def test_no_url_skips_the_push():
    assert push_heartbeat(Settings(_env_file=None)) is False


@respx.mock
def test_pushes_success_with_the_bearer_token():
    route = respx.post(PUSH).respond(200)

    assert push_heartbeat(configured()) is True
    request = route.calls.last.request
    assert request.url.params["success"] == "true"
    assert request.headers["Authorization"] == "Bearer t"


@respx.mock
def test_a_rejected_push_is_swallowed():
    respx.post(PUSH).respond(401)
    assert push_heartbeat(configured()) is False


@respx.mock
def test_an_unreachable_gatus_is_swallowed():
    respx.post(PUSH).mock(side_effect=httpx.ConnectError("refused"))
    assert push_heartbeat(configured()) is False
