import pytest

import asyncio

from fondat.aws import Client, Config
from fondat.aws.secrets import Secret, secrets_resource
from fondat.error import BadRequestError, NotFoundError
from uuid import uuid4


pytestmark = pytest.mark.asyncio


config = Config(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="id",
    aws_secret_access_key="secret",
    region_name="us-east-1",
)


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def client():
    async with Client(service_name="secretsmanager", config=config) as client:
        yield client


@pytest.fixture(scope="module")
async def resource(client):
    yield secrets_resource(client)


async def test_all(resource):

    name = str(uuid4())
    with pytest.raises(NotFoundError):
        await resource[name].delete()
    with pytest.raises(NotFoundError):
        await resource[name].put(Secret(value="something"))
    await resource.post(name=name, secret=Secret(value="string"))
    assert (await resource[name].get()).value == "string"
    await resource[name].put(Secret(value=b"binary"))
    assert (await resource[name].get()).value == b"binary"
    await resource[name].delete()
    with pytest.raises(BadRequestError):
        await resource[name].get()

    name = str(uuid4())
    await resource.post(name=name, secret=Secret(value=b"binary"))
    assert (await resource[name].get()).value == b"binary"
    await resource[name].put(Secret(value="string"))
    assert (await resource[name].get()).value == "string"
    await resource[name].delete()
