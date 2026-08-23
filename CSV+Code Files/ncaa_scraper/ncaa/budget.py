"""Persistent request budget, so we self-throttle instead of being throttled.

**Why this has to persist to disk.** The block we earned on 2026-08-11 came after
~603 requests in 43 minutes -- roughly 840/hour sustained. Whatever the exact
trigger, it is a *volume over a window* effect, not a per-request speed check. A
budget held only in memory would reset on every ``python run.py``, so five
sequential runs would sail past the same threshold that got us blocked. This
records every request timestamp in ``cache/_request_log.json`` and enforces the
window across runs, restarts, and days.

**Pacing rather than bursting.** Given a cap of N requests/hour, the minimum
spacing is 3600/N seconds and we hold to it, instead of firing N requests as fast
as possible and then stalling for the rest of the hour. A smooth low rate is both
less likely to trip a heuristic and kinder to the server than a spike followed by
silence.

**Jitter.** Intervals are multiplied by a random factor. Perfectly regular timing
is itself a bot signature, and it costs nothing to avoid.

The defaults sit about 3x under the rate that got us blocked. That is a guess at a
safety margin, not a known-good number -- the real threshold is not published and
was never established.
"""

import json
import os
import random
import time

WINDOW_SECONDS = 3600.0


class BudgetExhausted(RuntimeError):
    """The per-run request budget is spent. Stop cleanly and resume later."""


class RequestBudget:
    """Rolling-window request accounting that survives process restarts."""

    def __init__(self, path, max_per_hour, session_max, jitter=(0.85, 1.25),
                 min_sleep=0.0, verbose=True):
        self.path = path
        self.max_per_hour = max_per_hour
        self.session_max = session_max
        self.jitter = jitter
        self.min_sleep = min_sleep
        self.verbose = verbose

        self.session_count = 0
        self.waited_seconds = 0.0
        self._timestamps = self._load()

    # -- persistence ------------------------------------------------------

    def _load(self):
        if not os.path.exists(self.path):
            return []
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return [float(t) for t in data.get("requests", [])]
        except (ValueError, OSError):
            # A corrupt log must not licence an unlimited run: treat it as a
            # full window and let the caller wait it out.
            return [time.time()] * self.max_per_hour

    def _save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        payload = {"requests": [round(t, 3) for t in self._timestamps]}
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, self.path)

    def _prune(self, now):
        cutoff = now - WINDOW_SECONDS
        self._timestamps = [t for t in self._timestamps if t >= cutoff]

    # -- accounting -------------------------------------------------------

    @property
    def min_interval(self):
        """Seconds between requests implied by the hourly cap."""
        return WINDOW_SECONDS / self.max_per_hour if self.max_per_hour else 0.0

    def in_window(self):
        self._prune(time.time())
        return len(self._timestamps)

    def acquire(self):
        """Block until a request may be made.

        Raises:
            BudgetExhausted: the per-run budget is spent.
        """
        if self.session_max and self.session_count >= self.session_max:
            raise BudgetExhausted(
                f"per-run budget of {self.session_max} requests is spent "
                f"({self.in_window()} in the last hour). Re-run later to "
                f"continue; the cache means nothing is repeated."
            )

        now = time.time()
        self._prune(now)

        # Hourly cap: wait until the oldest request ages out of the window.
        if len(self._timestamps) >= self.max_per_hour:
            wake = self._timestamps[0] + WINDOW_SECONDS + 1.0
            delay = max(0.0, wake - now)
            if delay > 0:
                if self.verbose:
                    print(f"    [budget] {len(self._timestamps)}/"
                          f"{self.max_per_hour} requests in the last hour; "
                          f"pausing {delay / 60:.1f} min", flush=True)
                time.sleep(delay)
                self.waited_seconds += delay
                now = time.time()
                self._prune(now)

        # Smooth pacing: honour the interval implied by the cap.
        if self._timestamps:
            target = max(self.min_interval, self.min_sleep)
            elapsed = now - self._timestamps[-1]
            delay = target * random.uniform(*self.jitter) - elapsed
            if delay > 0:
                time.sleep(delay)
                self.waited_seconds += delay

    def record(self):
        self._timestamps.append(time.time())
        self.session_count += 1
        self._save()

    def hold(self, seconds, reason=""):
        """Explicitly stand down, e.g. for a Retry-After header."""
        if seconds <= 0:
            return
        if self.verbose:
            print(f"    [budget] holding {seconds:.0f}s{' -- ' + reason if reason else ''}",
                  flush=True)
        time.sleep(seconds)
        self.waited_seconds += seconds

    def summary(self):
        return (f"{self.session_count} requests this run, "
                f"{self.in_window()}/{self.max_per_hour} in the last hour, "
                f"{self.waited_seconds / 60:.1f} min spent waiting")
