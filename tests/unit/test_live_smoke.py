from pathlib import Path

from scripts import run_live_smoke


def test_live_smoke_discovery_writes_sanitized_result(monkeypatch, tmp_path):
    result_path = tmp_path / "result.json"
    monkeypatch.setattr(
        run_live_smoke,
        "_run_discovery",
        lambda: {"status": "ok", "phase": "discovery", "granules_found": 1},
    )
    assert (
        run_live_smoke.main(["--mode", "discovery", "--result-path", str(result_path)])
        == 0
    )
    assert '"granules_found": 1' in result_path.read_text()


def test_live_smoke_requires_credentials(monkeypatch):
    monkeypatch.delenv("EARTHDATA_USERNAME", raising=False)
    monkeypatch.delenv("EARTHDATA_PASSWORD", raising=False)
    try:
        run_live_smoke._require_credentials()
    except RuntimeError as exc:
        assert "EARTHDATA_USERNAME" in str(exc)
        assert "EARTHDATA_PASSWORD" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("missing credentials should fail")


def test_live_smoke_failure_records_phase(monkeypatch, tmp_path):
    result_path = tmp_path / "result.json"

    def fail():
        raise RuntimeError("CMR unavailable")

    monkeypatch.setattr(run_live_smoke, "_run_discovery", fail)
    assert (
        run_live_smoke.main(["--mode", "discovery", "--result-path", str(result_path)])
        == 1
    )
    payload = result_path.read_text()
    assert '"phase": "discovery"' in payload
    assert "CMR unavailable" in payload


def test_live_smoke_sanitizes_failure_results_and_logs(monkeypatch, tmp_path):
    monkeypatch.setenv("EARTHDATA_USERNAME", "operator@example.test")
    monkeypatch.setenv("EARTHDATA_PASSWORD", "secret-value")
    result_path = tmp_path / "result.json"

    def fail():
        raise RuntimeError(
            "https://operator@example.test:secret-value@example.test/file?token=secret"
        )

    monkeypatch.setattr(run_live_smoke, "_run_live_smoke", fail)
    assert (
        run_live_smoke.main(["--mode", "live-smoke", "--result-path", str(result_path)])
        == 1
    )
    payload = result_path.read_text()
    assert "operator@example.test" not in payload
    assert "secret-value" not in payload
    assert "token=secret" not in payload

    raw_log = tmp_path / "raw.log"
    sanitized_log = tmp_path / "sanitized.log"
    raw_log.write_text(
        "user=operator@example.test password=secret-value "
        "https://example.test/file?signature=signed"
    )
    run_live_smoke.sanitize_log_file(raw_log, sanitized_log)
    assert sanitized_log.read_text() == (
        "user=[REDACTED] password=[REDACTED] https://example.test/file?[REDACTED]"
    )


def test_live_smoke_mode_writes_validation_summary(monkeypatch, tmp_path):
    assert run_live_smoke.LIVE_SMOKE_LOOKBACK_HOURS == 24
    result_path = tmp_path / "result.json"
    monkeypatch.setattr(
        run_live_smoke,
        "_run_live_smoke",
        lambda: {
            "status": "ok",
            "phase": "live-smoke",
            "processed_granules": 1,
            "region_hourly_rows": 2,
            "dq_errors": 0,
        },
    )
    assert (
        run_live_smoke.main(["--mode", "live-smoke", "--result-path", str(result_path)])
        == 0
    )
    assert '"processed_granules": 1' in result_path.read_text()


def test_reset_disposable_paths_stays_under_cache(monkeypatch, tmp_path):
    cache = tmp_path / "live-readiness"
    monkeypatch.setattr(run_live_smoke, "CACHE_ROOT", cache)
    for name in ("raw", "dbt-target"):
        path = cache / name
        path.mkdir(parents=True)
        (path / "data").write_text("x")
    (cache / "live.duckdb").write_text("db")
    run_live_smoke._reset_disposable_paths()
    assert not (cache / "live.duckdb").exists()
    assert not (cache / "raw").exists()


def test_configure_live_environment_uses_disposable_paths(monkeypatch, tmp_path):
    cache = tmp_path / "live-readiness"
    monkeypatch.setattr(run_live_smoke, "CACHE_ROOT", cache)
    run_live_smoke._configure_live_environment()
    assert Path(__import__("os").environ["DUCKDB_PATH"]) == cache / "live.duckdb"
    assert Path(__import__("os").environ["TEMPO_NO2_RAW_DATA_DIR"]) == cache / "raw"


def test_live_failure_phase_classifies_operator_diagnostics():
    assert (
        run_live_smoke._failure_phase("discovery", RuntimeError("bad")) == "discovery"
    )
    assert (
        run_live_smoke._failure_phase(
            "live-smoke", RuntimeError("missing Earthdata credentials")
        )
        == "authentication"
    )
    assert (
        run_live_smoke._failure_phase("live-smoke", RuntimeError("download failed"))
        == "download"
    )
    assert (
        run_live_smoke._failure_phase("live-smoke", RuntimeError("bad NetCDF"))
        == "netcdf"
    )
    assert (
        run_live_smoke._failure_phase("live-smoke", RuntimeError("region grid failed"))
        == "aggregation"
    )
    assert (
        run_live_smoke._failure_phase("live-smoke", RuntimeError("dbt failed")) == "dbt"
    )
    assert (
        run_live_smoke._failure_phase("live-smoke", RuntimeError("unknown"))
        == "live-smoke"
    )
