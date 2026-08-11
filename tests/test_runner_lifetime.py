"""Tests for runner lifetime (idle autotermination) configuration.

Covers the duration/disable grammar shared by the runner watchdog and the two
subprocess reapers, and the rule that disabling the runner window disables the
reapers with it.
"""

from __future__ import annotations

import pytest

from omnigent.runner_lifetime import (
    DEFAULT_RUNNER_IDLE_TIMEOUT_S,
    HARNESS_IDLE_TIMEOUT_ENV_VAR,
    PANE_IDLE_TIMEOUT_ENV_VAR,
    RUNNER_IDLE_TIMEOUT_ENV_VAR,
    format_idle_timeout,
    parse_idle_timeout,
    reaper_env_for_idle_timeout,
    resolve_runner_idle_timeout_s,
    runner_idle_timeout_from_config,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(3600, 3600.0, id="int-seconds"),
        pytest.param(12.5, 12.5, id="float-seconds"),
        pytest.param("3600", 3600.0, id="str-seconds"),
        pytest.param("90s", 90.0, id="seconds-suffix"),
        pytest.param("30m", 1800.0, id="minutes"),
        pytest.param("2h", 7200.0, id="hours"),
        pytest.param("1d", 86400.0, id="days"),
        pytest.param("  2H  ", 7200.0, id="whitespace-and-case"),
    ],
)
def test_parse_accepts_seconds_and_durations(raw: object, expected: float) -> None:
    """Bare seconds and unit-suffixed durations both parse.

    :param raw: Value under test.
    :param expected: Expected seconds.
    :returns: None.
    """
    assert parse_idle_timeout(raw, label="x") == expected


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(0, id="int-zero"),
        pytest.param("0", id="str-zero"),
        pytest.param("never", id="never"),
        pytest.param("NEVER", id="never-uppercase"),
    ],
)
def test_parse_disable_forms(raw: object) -> None:
    """``0`` and ``never`` both disable autotermination.

    :param raw: Value under test.
    :returns: None.
    """
    assert parse_idle_timeout(raw, label="x") == 0.0


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(-1, id="negative-int"),
        pytest.param("-5m", id="negative-duration"),
        pytest.param("soon", id="word"),
        pytest.param("", id="empty"),
        pytest.param("   ", id="blank"),
        pytest.param("2y", id="unsupported-unit"),
        pytest.param("m", id="bare-unit"),
        pytest.param(True, id="boolean-true"),
        pytest.param(False, id="boolean-false"),
        pytest.param(["2h"], id="wrong-type"),
    ],
)
def test_parse_rejects_invalid(raw: object) -> None:
    """Malformed windows raise rather than silently defaulting.

    ``true`` is rejected specifically because ``bool`` is an ``int`` subclass —
    ``idle_timeout_s: true`` is a config typo, not a one-second window.

    :param raw: Value under test.
    :returns: None.
    """
    with pytest.raises(ValueError, match="x"):
        parse_idle_timeout(raw, label="x")


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        pytest.param(0.0, "never", id="disabled"),
        pytest.param(3600.0, "1h", id="hours"),
        pytest.param(1800.0, "30m", id="minutes"),
        pytest.param(86400.0, "1d", id="days"),
        pytest.param(90.0, "90s", id="seconds"),
        pytest.param(5400.0, "90m", id="not-whole-hours"),
        pytest.param(0.5, "0.5s", id="sub-second"),
    ],
)
def test_format_round_trips_to_largest_whole_unit(seconds: float, expected: str) -> None:
    """Windows render as the largest unit that divides them evenly.

    :param seconds: Window under test.
    :param expected: Expected rendering.
    :returns: None.
    """
    assert format_idle_timeout(seconds) == expected


def test_config_reader_returns_none_when_unset() -> None:
    """An absent ``runner`` block or field reads as "not configured"."""
    assert runner_idle_timeout_from_config({}) is None
    assert runner_idle_timeout_from_config({"runner": {}}) is None
    assert runner_idle_timeout_from_config({"runner": {"threadpool_max_workers": 4}}) is None


def test_config_reader_parses_duration() -> None:
    """``runner.idle_timeout_s`` accepts the duration grammar."""
    assert runner_idle_timeout_from_config({"runner": {"idle_timeout_s": "2h"}}) == 7200.0


def test_config_reader_rejects_non_mapping_runner_block() -> None:
    """A scalar ``runner:`` is a config error, not a silent default."""
    with pytest.raises(ValueError, match="runner"):
        runner_idle_timeout_from_config({"runner": "disabled"})


def test_resolve_defaults_to_one_hour() -> None:
    """No env and no config yields the one-hour default."""
    assert resolve_runner_idle_timeout_s({}, {}) == DEFAULT_RUNNER_IDLE_TIMEOUT_S


def test_resolve_prefers_env_over_config() -> None:
    """The env override wins so one launch can opt out without editing config."""
    config = {"runner": {"idle_timeout_s": "2h"}}
    env = {RUNNER_IDLE_TIMEOUT_ENV_VAR: "never"}
    assert resolve_runner_idle_timeout_s(config, env) == 0.0


def test_resolve_ignores_blank_env() -> None:
    """A blank env var falls through to config rather than parsing as invalid."""
    config = {"runner": {"idle_timeout_s": "2h"}}
    assert resolve_runner_idle_timeout_s(config, {RUNNER_IDLE_TIMEOUT_ENV_VAR: "  "}) == 7200.0


def test_resolve_raises_on_invalid_env() -> None:
    """A typo'd env override fails loud instead of silently defaulting."""
    with pytest.raises(ValueError, match=RUNNER_IDLE_TIMEOUT_ENV_VAR):
        resolve_runner_idle_timeout_s({}, {RUNNER_IDLE_TIMEOUT_ENV_VAR: "soon"})


def test_disabling_runner_window_disables_both_reapers() -> None:
    """``never`` must also stop the harness/pane reapers.

    Otherwise a runner told never to autoterminate would still lose its harness
    subprocess and TUI pane after their own one-hour windows — the exact
    tear-down the user opted out of.
    """
    assert reaper_env_for_idle_timeout(0.0) == {
        HARNESS_IDLE_TIMEOUT_ENV_VAR: "0",
        PANE_IDLE_TIMEOUT_ENV_VAR: "0",
    }


def test_positive_runner_window_leaves_reapers_on_defaults() -> None:
    """A finite window doesn't force the reapers off their own defaults."""
    assert reaper_env_for_idle_timeout(1800.0) == {}
