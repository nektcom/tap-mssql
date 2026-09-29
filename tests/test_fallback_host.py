"""Opt-in fallback host: chosen once at startup, pinned for the run, untouched when not configured."""

from __future__ import annotations

from unittest import mock

import pymssql
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import URL

from tap_mssql import host_fallback
from tap_mssql.tap import TapMSSQL

BASE_CONFIG = {
    "host": "primary.example.com",
    "port": 1433,
    "user": "reader",
    "password": "secret",
    "database": "erp",
}

NETWORK_ERROR = sa.exc.OperationalError(
    "SELECT 1",
    {},
    pymssql.exceptions.OperationalError(
        (
            20009,
            b"DB-Lib error message 20009, severity 9:\nUnable to connect: Adaptive Server is "
            b"unavailable or does not exist (primary.example.com)\nNet-Lib error during Connection refused (61)\n",
        ),
    ),
)
LOGIN_ERROR = sa.exc.OperationalError(
    "SELECT 1",
    {},
    pymssql.exceptions.OperationalError(
        (
            18456,
            b"Login failed for user 'reader'.DB-Lib error message 20018, severity 14:\n"
            b"General SQL Server error: Check messages from the SQL Server\n",
        ),
    ),
)


def make_tap(**overrides) -> TapMSSQL:
    # The tap resolves its catalog in __init__; stub discovery so each test drives `connector` itself.
    with mock.patch.object(TapMSSQL, "catalog_dict", new_callable=mock.PropertyMock, return_value={"streams": []}):
        return TapMSSQL(config={**BASE_CONFIG, **overrides}, validate_config=False)


def probed_hosts(probe: mock.Mock) -> list[str]:
    return [call.args[0].host for call in probe.call_args_list]


# --- no fallback configured: the current code path, byte for byte ---------------------------------


def test_without_fallback_host_nothing_is_probed():
    tap = make_tap()
    with mock.patch.object(host_fallback, "probe") as probe:
        url = sa.engine.make_url(tap.connector.sqlalchemy_url)

    probe.assert_not_called()
    assert url.host == "primary.example.com"
    assert url.port == 1433


def test_without_fallback_host_url_is_unchanged():
    tap = make_tap()
    expected = sa.engine.make_url(tap.get_sqlalchemy_url(config=tap.config)).render_as_string(hide_password=False)
    assert tap.connector.sqlalchemy_url == expected


def test_empty_fallback_host_counts_as_not_configured():
    tap = make_tap(fallback_host="")
    with mock.patch.object(host_fallback, "probe") as probe:
        tap.connector  # noqa: B018
    probe.assert_not_called()


def test_sqlalchemy_url_ignores_fallback_host():
    tap = make_tap(
        fallback_host="fallback.example.com",
        sqlalchemy_url="mssql+pymssql://reader:secret@explicit.example.com:1433/erp",
    )
    with mock.patch.object(host_fallback, "probe") as probe:
        url = sa.engine.make_url(tap.connector.sqlalchemy_url)
    probe.assert_not_called()
    assert url.host == "explicit.example.com"


# --- fallback configured ---------------------------------------------------------------------------


def test_primary_reachable_uses_primary_and_never_probes_fallback():
    tap = make_tap(fallback_host="fallback.example.com")
    with mock.patch.object(host_fallback, "probe") as probe:
        url = sa.engine.make_url(tap.connector.sqlalchemy_url)

    assert probed_hosts(probe) == ["primary.example.com"]
    assert url.host == "primary.example.com"


def test_primary_down_uses_fallback_with_its_own_port():
    tap = make_tap(fallback_host="fallback.example.com", fallback_port=11433)
    with (
        mock.patch.object(host_fallback, "probe", side_effect=[NETWORK_ERROR, None]) as probe,
        mock.patch.object(tap.user_logger, "warning") as user_warning,
    ):
        url = sa.engine.make_url(tap.connector.sqlalchemy_url)

    assert probed_hosts(probe) == ["primary.example.com", "fallback.example.com"]
    assert (url.host, url.port) == ("fallback.example.com", 11433)
    # credentials and database are carried over to the fallback
    assert (url.username, url.password, url.database) == ("reader", "secret", "erp")
    message = user_warning.call_args.args[0]
    assert "primary.example.com:1433" in message
    assert "fallback.example.com:11433" in message


def test_fallback_port_defaults_to_primary_port():
    tap = make_tap(port=2433, fallback_host="fallback.example.com")
    with mock.patch.object(host_fallback, "probe", side_effect=[NETWORK_ERROR, None]):
        url = sa.engine.make_url(tap.connector.sqlalchemy_url)
    assert (url.host, url.port) == ("fallback.example.com", 2433)


def test_host_is_chosen_once_and_pinned_for_the_run():
    tap = make_tap(fallback_host="fallback.example.com")
    with mock.patch.object(host_fallback, "probe", side_effect=[NETWORK_ERROR, None]) as probe:
        first = tap.connector
        # later accesses (discovery, every stream, pool reconnects) reuse the same connector
        assert tap.connector is first
        assert tap.connector is first
    assert probe.call_count == 2
    assert sa.engine.make_url(first.sqlalchemy_url).host == "fallback.example.com"


def test_both_hosts_down_stops_with_clear_error():
    tap = make_tap(fallback_host="fallback.example.com")
    with (
        mock.patch.object(host_fallback, "probe", side_effect=[NETWORK_ERROR, LOGIN_ERROR]),
        mock.patch.object(tap.user_logger, "error") as user_error,
        mock.patch.object(tap.internal_logger, "error") as internal_error,
        pytest.raises(SystemExit) as exit_info,
    ):
        tap.connector  # noqa: B018

    assert exit_info.value.code == 1
    message = user_error.call_args.args[0]
    assert "primary host primary.example.com:1433: the server could not be reached" in message
    assert "fallback host fallback.example.com:1433: the server rejected the user or password" in message
    assert "secret" not in message
    internal = internal_error.call_args.args[0]
    assert "stage=network code=20009" in internal
    assert "stage=login code=18456" in internal


def test_probe_timeout_is_configurable():
    tap = make_tap(fallback_host="fallback.example.com", fallback_probe_timeout=5)
    with mock.patch.object(host_fallback, "probe") as probe:
        tap.connector  # noqa: B018
    assert probe.call_args.args[1] == 5


def test_probe_timeout_default():
    tap = make_tap(fallback_host="fallback.example.com")
    with mock.patch.object(host_fallback, "probe") as probe:
        tap.connector  # noqa: B018
    assert probe.call_args.args[1] == host_fallback.DEFAULT_PROBE_TIMEOUT_SECONDS


def test_ssh_tunnel_is_reopened_for_the_fallback():
    tap = make_tap(
        fallback_host="fallback.example.com",
        ssh_tunnel={"enable": True, "host": "bastion", "port": 22, "username": "u", "password": "p"},
    )
    remote_binds = []

    def fake_tunnel(*, ssh_config, url: URL) -> URL:
        remote_binds.append((url.host, url.port))
        tap.ssh_tunnel = mock.Mock()
        return url.set(host="127.0.0.1", port=40000 + len(remote_binds))

    with (
        mock.patch.object(tap, "ssh_tunnel_connect", side_effect=fake_tunnel),
        mock.patch.object(host_fallback, "probe", side_effect=[NETWORK_ERROR, None]),
    ):
        url = sa.engine.make_url(tap.connector.sqlalchemy_url)

    assert remote_binds == [("primary.example.com", 1433), ("fallback.example.com", 1433)]
    assert (url.host, url.port) == ("127.0.0.1", 40002)


# --- helpers ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "stage", "code"),
    [
        (NETWORK_ERROR, host_fallback.STAGE_NETWORK, 20009),
        (LOGIN_ERROR, host_fallback.STAGE_LOGIN, 18456),
        (
            sa.exc.OperationalError(
                "SELECT 1", {}, pymssql.exceptions.OperationalError((4060, b"Cannot open database \"erp\"."))
            ),
            host_fallback.STAGE_DATABASE,
            4060,
        ),
        (RuntimeError("boom"), host_fallback.STAGE_UNKNOWN, None),
    ],
)
def test_classify_error(exc, stage, code):
    got_stage, got_code, detail = host_fallback.classify_error(exc)
    assert (got_stage, got_code) == (stage, code)
    assert "\n" not in detail


def test_real_pymssql_error_against_closed_port_is_network():
    url = URL.create("mssql+pymssql", username="u", password="p", host="127.0.0.1", port=1, database="d")
    with pytest.raises(sa.exc.OperationalError) as exc_info:
        host_fallback.probe(url, timeout_seconds=3)
    stage, code, _ = host_fallback.classify_error(exc_info.value)
    assert (stage, code) == (host_fallback.STAGE_NETWORK, 20009)
