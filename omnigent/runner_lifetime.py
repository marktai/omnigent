"""Runner lifetime (idle autotermination) configuration.

A runner self-terminates after an idle window, and two reapers reclaim
idle harness subprocesses and idle native TUI panes on the same
principle. Someone who wants the vendor-TUI experience — a session that
stays up until they close it — needs one knob that turns all three off,
so the runner-level window is the umbrella: disabling it disables the
reapers too (their own env knobs still win when set explicitly).

The knob is ``runner.idle_timeout_s`` in ``config.yaml``, overridable
per-process by :envvar:`OMNIGENT_RUNNER_IDLE_TIMEOUT_S`. Values are bare
seconds or a duration (``90s``, ``30m``, ``2h``, ``1d``); ``0`` or
``never`` disables autotermination.

Stdlib-only so both the CLI and the runner entrypoint can import it
without paying for yaml/FastAPI at process start.
"""

from __future__ import annotations

from collections.abc import Mapping

# Umbrella knob: the runner's own inactivity watchdog. ``0`` disables it.
RUNNER_IDLE_TIMEOUT_ENV_VAR = "OMNIGENT_RUNNER_IDLE_TIMEOUT_S"
# Per-subsystem knobs, kept here so the runner can propagate a disable
# into them and the reaper modules resolve the same names.
HARNESS_IDLE_TIMEOUT_ENV_VAR = "OMNIGENT_HARNESS_IDLE_TIMEOUT_S"
PANE_IDLE_TIMEOUT_ENV_VAR = "OMNIGENT_NATIVE_PANE_IDLE_TIMEOUT_S"

# Dotted config key, as spelled in ``omnigent config set``.
RUNNER_IDLE_TIMEOUT_CONFIG_KEY = "runner.idle_timeout_s"
_CONFIG_SECTION = "runner"
_CONFIG_FIELD = "idle_timeout_s"

DEFAULT_RUNNER_IDLE_TIMEOUT_S = 60.0 * 60.0
# Sentinel for "no autotermination"; shared so call sites compare against a
# name rather than a bare 0.
DISABLED = 0.0

# Word form meaning "never autoterminate". ``0`` means the same and parses
# through the numeric path.
_DISABLE_WORD = "never"

_UNIT_SECONDS: dict[str, float] = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}

# Descending units for rendering a window back as a compact duration.
_DISPLAY_UNITS: tuple[tuple[str, float], ...] = (
    ("d", 86400.0),
    ("h", 3600.0),
    ("m", 60.0),
    ("s", 1.0),
)

_ACCEPTED_FORMS = (
    "seconds (3600), a duration (90s, 30m, 2h, 1d), or 0/never to disable autotermination"
)


def parse_idle_timeout(value: object, *, label: str) -> float:
    """Parse an idle window into seconds.

    :param value: Raw value from YAML, an env var, or ``KEY=VALUE``, e.g.
        ``"2h"``, ``3600``, or ``"never"``.
    :param label: Name to quote in error messages, e.g.
        ``"runner.idle_timeout_s"``.
    :returns: The window in seconds; :data:`DISABLED` when autotermination
        is turned off.
    :raises ValueError: If *value* is not a non-negative duration, ``0``, or
        ``never``.
    """
    # bool is an int subclass, and ``idle_timeout_s: true`` is a config typo
    # rather than a window, so reject it before the numeric path.
    if isinstance(value, bool):
        raise ValueError(f"{label} must be {_ACCEPTED_FORMS}; got {value!r}")
    if isinstance(value, (int, float)):
        seconds = float(value)
    elif isinstance(value, str):
        seconds = _parse_duration_text(value, label=label)
    else:
        raise ValueError(f"{label} must be {_ACCEPTED_FORMS}; got {value!r}")
    if seconds < 0:
        raise ValueError(f"{label} must not be negative; got {value!r}")
    return seconds


def _parse_duration_text(raw: str, *, label: str) -> float:
    """Parse the string form of an idle window into seconds.

    :param raw: Text value, e.g. ``"30m"``, ``"3600"``, or ``"never"``.
    :param label: Name to quote in error messages.
    :returns: The window in seconds.
    :raises ValueError: If *raw* is blank or not a recognized duration.
    """
    text = raw.strip().lower()
    if not text:
        raise ValueError(f"{label} must be {_ACCEPTED_FORMS}; got {raw!r}")
    if text == _DISABLE_WORD:
        return DISABLED
    multiplier = 1.0
    if text[-1] in _UNIT_SECONDS:
        multiplier = _UNIT_SECONDS[text[-1]]
        text = text[:-1].strip()
    try:
        return float(text) * multiplier
    except ValueError:
        raise ValueError(f"{label} must be {_ACCEPTED_FORMS}; got {raw!r}") from None


def format_idle_timeout(seconds: float) -> str:
    """Render an idle window for display.

    :param seconds: Window in seconds, e.g. ``3600.0``.
    :returns: ``"never"`` when disabled, else the largest whole unit that
        divides the window (e.g. ``"1h"``), falling back to seconds.
    """
    if seconds <= DISABLED:
        return "never"
    for suffix, size in _DISPLAY_UNITS:
        if seconds >= size and seconds % size == 0:
            return f"{int(seconds / size)}{suffix}"
    return f"{seconds:g}s"


def runner_idle_timeout_from_config(config: Mapping[str, object]) -> float | None:
    """Read ``runner.idle_timeout_s`` out of a loaded config mapping.

    :param config: Parsed config, e.g. ``{"runner": {"idle_timeout_s": "2h"}}``.
    :returns: The configured window in seconds, or ``None`` when unset.
    :raises ValueError: If the ``runner`` block or the window is malformed.
    """
    section = config.get(_CONFIG_SECTION)
    if section is None:
        return None
    if not isinstance(section, Mapping):
        raise ValueError(f"{_CONFIG_SECTION} config must be a mapping")
    value = section.get(_CONFIG_FIELD)
    if value is None:
        return None
    return parse_idle_timeout(value, label=RUNNER_IDLE_TIMEOUT_CONFIG_KEY)


def resolve_runner_idle_timeout_s(
    config: Mapping[str, object],
    env: Mapping[str, str],
) -> float:
    """Resolve the runner's idle window from env then config.

    The env var wins so a single launch can opt out without editing config
    (and so the CLI can hand the project-config value to a spawned runner,
    which only sees the user-level file).

    :param config: Parsed config mapping to fall back to.
    :param env: Process environment to read the override from.
    :returns: The window in seconds; :data:`DISABLED` when turned off.
    :raises ValueError: If either source holds a malformed value.
    """
    raw_env = env.get(RUNNER_IDLE_TIMEOUT_ENV_VAR)
    if raw_env is not None and raw_env.strip():
        return parse_idle_timeout(raw_env, label=RUNNER_IDLE_TIMEOUT_ENV_VAR)
    configured = runner_idle_timeout_from_config(config)
    if configured is not None:
        return configured
    return DEFAULT_RUNNER_IDLE_TIMEOUT_S


def reaper_env_for_idle_timeout(idle_timeout_s: float) -> dict[str, str]:
    """Return the reaper env vars implied by a runner idle window.

    Disabling the runner watchdog is a request to keep the session alive, so
    the harness and native-pane reapers are disabled with it — otherwise a
    ``never`` runner would still lose its harness subprocesses and TUI panes
    after their own one-hour windows. A positive window leaves the reapers on
    their own defaults, since reaping an idle conversation's subprocess is
    resumable and bounds memory on a shared runner.

    :param idle_timeout_s: The resolved runner window in seconds.
    :returns: Env vars to apply without clobbering explicit operator values
        (empty when the watchdog is enabled).
    """
    if idle_timeout_s > DISABLED:
        return {}
    return {
        HARNESS_IDLE_TIMEOUT_ENV_VAR: "0",
        PANE_IDLE_TIMEOUT_ENV_VAR: "0",
    }


__all__ = [
    "DEFAULT_RUNNER_IDLE_TIMEOUT_S",
    "DISABLED",
    "HARNESS_IDLE_TIMEOUT_ENV_VAR",
    "PANE_IDLE_TIMEOUT_ENV_VAR",
    "RUNNER_IDLE_TIMEOUT_CONFIG_KEY",
    "RUNNER_IDLE_TIMEOUT_ENV_VAR",
    "format_idle_timeout",
    "parse_idle_timeout",
    "reaper_env_for_idle_timeout",
    "resolve_runner_idle_timeout_s",
    "runner_idle_timeout_from_config",
]
