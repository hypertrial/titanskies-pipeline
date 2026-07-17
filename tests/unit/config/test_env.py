from datetime import date

from titanskies_pipeline.config import _env


def test_env_int_branches(monkeypatch):
    monkeypatch.delenv("X_INT", raising=False)
    assert _env._env_int("X_INT", 3) == 3
    monkeypatch.setenv("X_INT", "7")
    assert _env._env_int("X_INT", 3) == 7
    monkeypatch.setenv("X_INT", "bad")
    assert _env._env_int("X_INT", 3) == 3


def test_env_float_branches(monkeypatch):
    monkeypatch.delenv("X_FLOAT", raising=False)
    assert _env._env_float("X_FLOAT", 1.5) == 1.5
    monkeypatch.setenv("X_FLOAT", "2.5")
    assert _env._env_float("X_FLOAT", 1.5) == 2.5
    monkeypatch.setenv("X_FLOAT", "bad")
    assert _env._env_float("X_FLOAT", 1.5) == 1.5


def test_optional_env_helpers(monkeypatch):
    monkeypatch.delenv("X_OPT", raising=False)
    assert _env._optional_env_str("X_OPT") is None
    monkeypatch.setenv("X_OPT", "  token  ")
    assert _env._optional_env_str("X_OPT") == "token"
    monkeypatch.setenv("X_OPT", "   ")
    assert _env._optional_env_str("X_OPT") is None


def test_env_date_and_bool(monkeypatch):
    monkeypatch.setenv("X_DATE", "bad")
    assert _env._env_date("X_DATE", "2026-07-12") == date(2026, 7, 12)
    monkeypatch.setenv("X_DATE", "2026-08-01")
    assert _env._env_date("X_DATE", "2026-07-12") == date(2026, 8, 1)

    monkeypatch.delenv("X_BOOL", raising=False)
    assert _env._env_bool("X_BOOL", False) is False
    monkeypatch.setenv("X_BOOL", "true")
    assert _env._env_bool("X_BOOL", False) is True
    monkeypatch.setenv("X_BOOL", "off")
    assert _env._env_bool("X_BOOL", True) is False
