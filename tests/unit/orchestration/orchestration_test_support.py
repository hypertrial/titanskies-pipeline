"""Helpers for orchestration unit tests (import from test modules)."""

from __future__ import annotations

from queue import Empty

from titanskies_pipeline.orchestration import dbt_build as dbt_build_mod
from titanskies_pipeline.resources.progress_guardrails import ProgressGuardrail


class _FakeClock:
    def __init__(self, start: float = 0.0):
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += float(seconds)
        return self.now


def _patch_guardrail_clock(monkeypatch, clock: _FakeClock) -> None:
    class _ClockedProgressGuardrail(ProgressGuardrail):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("clock", clock)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(dbt_build_mod, "ProgressGuardrail", _ClockedProgressGuardrail)


class _ImmediateThread:
    def __init__(self, target=None, *args, **kwargs):
        del args, kwargs
        self._target = target
        self._alive = False

    def start(self):
        self._alive = True
        try:
            if self._target is not None:
                self._target()
        finally:
            self._alive = False

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        del timeout
        return None


class _DormantThread:
    def __init__(self, target=None, *args, **kwargs):
        del target, args, kwargs

    def start(self):
        return None

    def join(self, timeout=None):
        del timeout
        return None


class _FakeQueue:
    def __init__(self, *args, clock=None, empty_cycles=0, empty_advance=None, **kwargs):
        del args, kwargs
        self._clock = clock
        self._empty_cycles = int(empty_cycles)
        self._empty_advance = empty_advance
        self._items: list[object] = []

    def put(self, item):
        self._items.append(item)

    def get(self, timeout=None):
        if self._empty_cycles > 0:
            self._empty_cycles -= 1
            if self._clock is not None:
                advance_seconds = (
                    float(timeout or 0)
                    if self._empty_advance is None
                    else float(self._empty_advance)
                )
                self._clock.advance(advance_seconds)
            raise Empty
        if not self._items:
            raise Empty
        return self._items.pop(0)

    def qsize(self):
        return len(self._items)
