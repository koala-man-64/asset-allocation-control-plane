from __future__ import annotations

from core.results_freshness import _ranking_dirty_window


def test_ranking_dirty_window_returns_none_for_identical_state() -> None:
    state = {
        "strategy": {"name": "alpha", "version": 1},
        "ranking": {"name": "quality", "version": 2},
        "universe": {"name": "default", "version": 3},
        "domains": {
            "market": {
                "fingerprint": "fp-1",
                "affectedAsOfStart": "2026-03-01",
                "affectedAsOfEnd": "2026-03-03",
            }
        },
    }

    assert _ranking_dirty_window(state, state) == (None, None)


def test_ranking_dirty_window_uses_changed_domain_window_for_lineage_delta() -> None:
    previous_state = {
        "strategy": {"name": "alpha", "version": 1},
        "ranking": {"name": "quality", "version": 2},
        "universe": {"name": "default", "version": 3},
        "domains": {
            "market": {
                "fingerprint": "fp-1",
                "affectedAsOfStart": "2026-03-01",
                "affectedAsOfEnd": "2026-03-03",
            },
            "finance": {
                "fingerprint": "fp-2",
                "affectedAsOfStart": "2026-03-02",
                "affectedAsOfEnd": "2026-03-04",
            },
        },
    }
    current_state = {
        **previous_state,
        "domains": {
            **previous_state["domains"],
            "finance": {
                "fingerprint": "fp-3",
                "affectedAsOfStart": "2026-03-10",
                "affectedAsOfEnd": "2026-03-12",
            },
        },
    }

    dirty_start, dirty_end = _ranking_dirty_window(previous_state, current_state)

    assert dirty_start.isoformat() == "2026-03-10"
    assert dirty_end.isoformat() == "2026-03-12"


def test_ranking_dirty_window_forces_full_window_when_structural_inputs_change() -> None:
    previous_state = {
        "strategy": {"name": "alpha", "version": 1},
        "ranking": {"name": "quality", "version": 2},
        "universe": {"name": "default", "version": 3},
        "domains": {
            "market": {
                "fingerprint": "fp-1",
                "affectedAsOfStart": "2026-03-01",
                "affectedAsOfEnd": "2026-03-03",
            },
            "finance": {
                "fingerprint": "fp-2",
                "affectedAsOfStart": "2026-03-05",
                "affectedAsOfEnd": "2026-03-07",
            },
        },
    }
    current_state = {
        **previous_state,
        "ranking": {"name": "quality", "version": 3},
    }

    dirty_start, dirty_end = _ranking_dirty_window(previous_state, current_state)

    assert dirty_start.isoformat() == "2026-03-01"
    assert dirty_end.isoformat() == "2026-03-07"
