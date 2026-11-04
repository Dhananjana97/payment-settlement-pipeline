import pytest

from services.common.config import ConfigError, Settings


def test_defaults_match_compose(env):
    s = Settings.from_env("reader")
    assert s.service_name == "reader"
    assert s.topic == "ledger-events"
    assert s.partitions == 12
    assert s.gateway_port == 7100
    assert s.health_port == 9100
    assert s.api_port == 8080
    assert ":" in s.instance_id


def test_overrides(env, monkeypatch):
    monkeypatch.setenv("KAFKA_PARTITIONS", "24")
    monkeypatch.setenv("HEALTH_PORT", "9200")
    s = Settings.from_env("consumer")
    assert s.partitions == 24
    assert s.health_port == 9200


def test_missing_secret_raises(env, monkeypatch):
    monkeypatch.delenv("GATEWAY_SECRET")
    with pytest.raises(ConfigError, match="GATEWAY_SECRET"):
        Settings.from_env("reader")


def test_bad_int_raises(env, monkeypatch):
    monkeypatch.setenv("KAFKA_PARTITIONS", "twelve")
    with pytest.raises(ConfigError, match="KAFKA_PARTITIONS"):
        Settings.from_env("reader")


def test_secrets_not_in_repr(env):
    s = Settings.from_env("reader")
    assert "bo-dev-secret" not in repr(s)
    assert "bo-bank-dev-secret" not in repr(s)
