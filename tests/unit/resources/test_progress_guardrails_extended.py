from titanskies_pipeline.resources.progress_guardrails import ProgressGuardrail


class _Logger:
    def __init__(self):
        self.records: list[tuple[str, str, dict]] = []

    def info(self, msg: str, payload: dict):
        self.records.append(("info", msg, payload))

    def warning(self, msg: str, payload: dict):
        self.records.append(("warning", msg, payload))

    def error(self, msg: str, payload: dict):
        self.records.append(("error", msg, payload))

    def debug(self, msg: str, **kwargs):
        del msg, kwargs


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_progress_guardrail_work_log_interval_triggers_log():
    logger = _Logger()
    clock = _Clock()
    guardrail = ProgressGuardrail(
        asset="asset",
        logger=logger,
        progress_log_interval_seconds=10_000,
        no_progress_soft_timeout_seconds=None,
        no_progress_hard_timeout_seconds=None,
        work_log_interval=2,
        clock=clock,
    )
    guardrail.record_progress(work_increment=2, phase="work")
    assert logger.records
    assert logger.records[-1][2]["reason"] == "progress"


def test_progress_guardrail_check_emits_heartbeat_only():
    logger = _Logger()
    clock = _Clock()
    guardrail = ProgressGuardrail(
        asset="asset",
        logger=logger,
        progress_log_interval_seconds=1,
        no_progress_soft_timeout_seconds=None,
        no_progress_hard_timeout_seconds=None,
        clock=clock,
    )
    guardrail.record_progress(work_increment=0, phase="start", force_log=True)
    clock.advance(1.1)
    guardrail.check(phase="heartbeat-only")
    reasons = [payload["reason"] for _, _, payload in logger.records]
    assert "heartbeat" in reasons


def test_progress_guardrail_check_quiet_when_no_signals():
    logger = _Logger()
    clock = _Clock()
    guardrail = ProgressGuardrail(
        asset="asset",
        logger=logger,
        progress_log_interval_seconds=60,
        no_progress_soft_timeout_seconds=None,
        no_progress_hard_timeout_seconds=None,
        clock=clock,
    )
    guardrail.record_progress(work_increment=0, phase="start", force_log=True)
    guardrail.check(phase="quiet")
    reasons = [payload["reason"] for _, _, payload in logger.records]
    assert reasons == ["progress"]


def test_progress_guardrail_check_prefers_soft_warn_over_heartbeat():
    logger = _Logger()
    clock = _Clock()
    guardrail = ProgressGuardrail(
        asset="asset",
        logger=logger,
        progress_log_interval_seconds=1,
        no_progress_soft_timeout_seconds=1,
        no_progress_hard_timeout_seconds=None,
        clock=clock,
    )
    guardrail.record_progress(work_increment=0, phase="start", force_log=True)
    clock.advance(1.1)
    guardrail.check(phase="soft-not-heartbeat")
    reasons = [payload["reason"] for _, _, payload in logger.records]
    assert "no_progress_soft_timeout" in reasons
    assert "heartbeat" not in reasons
