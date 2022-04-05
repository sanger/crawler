import pytest

from crawler.rabbit.async_consumer import AsyncConsumer
from crawler.types import RabbitServerDetails

DEFAULT_SERVER_DETAILS = RabbitServerDetails(
    uses_ssl=False, host="host", port=5672, username="username", password="password", vhost="vhost"
)


@pytest.mark.parametrize("uses_ssl", [True, False])
def test_connect_provides_correct_parameters(uses_ssl):
    server_details = RabbitServerDetails(
        uses_ssl=uses_ssl, host="host", port=5672, username="username", password="password", vhost="vhost"
    )
    subject = AsyncConsumer(server_details, "queue")
    select_connection = subject.connect()

    parameters = select_connection.params
    if server_details.uses_ssl:
        assert parameters.ssl_options is not None
    else:
        assert parameters.ssl_options is None

    assert parameters.host == server_details.host
    assert parameters.port == server_details.port
    assert parameters.credentials.username == server_details.username
    assert parameters.credentials.password == server_details.password
    assert parameters.virtual_host == server_details.vhost
