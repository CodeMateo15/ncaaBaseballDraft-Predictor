"""HTTP session for stats.ncaa.org.

Two obstacles, in order of discovery:

1. **TLS fingerprinting.** Plain HTTP/1.1 clients (``requests``, ``curl``) get a
   flat 403 regardless of headers. ``curl_cffi`` negotiates HTTP/2 and
   impersonates a real Chrome TLS fingerprint, which passes. This is the same
   approach ``ncaa_bbStats.team_stats`` already uses.

2. **An Akamai interstitial challenge on ``/teams/*`` and ``/players/*``.**
   ``/rankings/*`` is open, but team pages return **HTTP 200 with a ~2.3 KB
   challenge page** rather than an error status -- so naive code sees success,
   finds no table, and reports zero rows. That silent-empty path is the single
   worst failure mode in this scraper, which is why :meth:`NcaaSession.get`
   raises :class:`FetchError` and never returns an empty string.

   The challenge is not a CAPTCHA and not a cryptographic puzzle. The page
   contains a token and a trivial sum, which is POSTed back once:

       GET  /teams/596471                       -> 2.3 KB challenge page
       POST /_sec/verify?provider=interstitial  -> {"reload": true}
       GET  /teams/596471                       -> 1.9 MB real page

   One solve covers the whole session, so this costs about one extra request
   per run rather than per page.

**On doing this at all.** Replaying the handshake is bot-management
circumvention. This project does it deliberately: the NCAA does not sell these
statistics, the underlying facts are public record, and established tools
(``baseballr``, ``ncaa_stats_py``) have scraped these pages for years. The
challenge page's own ``<noscript>`` block points at
``request_quota_reached.html``, which indicates the concern is server load. So
the mitigations are the point, and they are not optional: one request per
second, one worker by default, and an on-disk cache that guarantees each page is
fetched exactly once. See README.md.
"""

import json
import re
import time

from curl_cffi import requests as cffi_requests

import config


class FetchError(RuntimeError):
    """A URL could not be fetched. Raised rather than returning empty content."""

    def __init__(self, url, attempts, last_error):
        self.url = url
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"{url} failed after {attempts} attempt(s): {last_error}")


class ChallengeError(FetchError):
    """The interstitial could not be cleared.

    Most likely Akamai rotated the challenge shape. Check ``_parse_challenge``
    against a freshly saved copy of the challenge page.
    """


class QuotaExhausted(FetchError):
    """The IP has hit the site's per-address request quota.

    **A 403 from this site means quota, not a transient error.** The interstitial
    returns HTTP 200, and TLS fingerprinting is already handled by curl_cffi
    impersonation, so once a session is working a 403 is the site saying stop --
    ``request_quota_reached.html``, referenced from the challenge page itself.

    Measured 2026-08-11: a 1 req/s run completed ~290 team-seasons (~580
    requests over ~32 minutes) and was then 403'd on **every** subsequent URL,
    including ``/rankings/*`` which is otherwise open. So the block is per-IP and
    site-wide, not per-endpoint.

    Retrying is both futile and rude, so this is raised on the first 403 without
    retries, and :func:`run.collect` aborts the whole run rather than burning a
    failed request on every remaining team. The cache makes that free: re-run
    later and it resumes at the first team it never fetched.
    """


# The challenge page, as observed 2026-08-11. Reproduced here because these
# three regexes are the part most likely to break, and a maintainer needs to see
# what they were written against:
#
#   <html><head><script type="text/javascript">
#   var i = 91;
#   var j = i + Number("6208" + "99594");
#   ...
#   fetch("/_sec/verify?provider=interstitial", {
#     method: "POST",
#     body: JSON.stringify({"bm-verify": "AAQAAAAI_____w...", "pow": j})
#   })
#
# The split string literal in Number("6208" + "99594") is obfuscation, i.e. it
# exists specifically to be changed. Treat a parse failure as expected breakage
# with a clear message, not as a mystery.
_RE_TOKEN = re.compile(r'"bm-verify"\s*:\s*"([^"]+)"')
_RE_I = re.compile(r'\bvar\s+i\s*=\s*(\d+)\s*;')
_RE_J = re.compile(r'\bvar\s+j\s*=\s*i\s*\+\s*Number\(\s*"(\d+)"\s*\+\s*"(\d+)"\s*\)')

# A real page is ~1-2 MB; the challenge is ~2.3 KB. Size alone is a weak signal,
# so we test for the token instead.
_CHALLENGE_MARKER = "bm-verify"

MAX_SOLVES_PER_URL = 2


def is_challenge(html: str) -> bool:
    """True if this is an interstitial challenge page rather than real content."""
    return _CHALLENGE_MARKER in html and len(html) < 100_000


def _parse_challenge(html: str):
    """Extract ``(token, pow_value)`` from a challenge page.

    Raises:
        ValueError: if the page does not match the observed shape, which almost
            certainly means Akamai rotated the challenge.
    """
    token = _RE_TOKEN.search(html)
    i_match = _RE_I.search(html)
    j_match = _RE_J.search(html)

    missing = [
        name
        for name, match in (("bm-verify token", token), ("var i", i_match), ("var j", j_match))
        if match is None
    ]
    if missing:
        raise ValueError(
            "challenge page did not match the expected shape; missing: "
            + ", ".join(missing)
            + ". Akamai has probably rotated the challenge -- save the page and "
            "update the regexes in ncaa/session.py."
        )

    i = int(i_match.group(1))
    addend = int(j_match.group(1) + j_match.group(2))
    return token.group(1), i + addend


class NcaaSession:
    """A rate-limited stats.ncaa.org session that clears the interstitial.

    Not thread-safe by design. ``run.py --workers N`` gives each thread its own
    instance, so each has its own cookie jar and its own challenge solve, and no
    lock is needed.
    """

    def __init__(self, sleep_ok=None, sleep_fail=None, max_retries=None, timeout=None,
                 budget=None):
        self._session = cffi_requests.Session(impersonate="chrome")
        self.sleep_ok = config.SLEEP_OK if sleep_ok is None else sleep_ok
        self.sleep_fail = config.SLEEP_FAIL if sleep_fail is None else sleep_fail
        self.max_retries = config.MAX_RETRIES if max_retries is None else max_retries
        self.timeout = config.TIMEOUT if timeout is None else timeout

        # When present, the budget owns pacing: it enforces a rolling hourly cap
        # that persists across runs and adds jitter. self.sleep_ok then acts only
        # as a floor. See ncaa/budget.py for why in-memory pacing is not enough.
        self.budget = budget

        self.requests_made = 0
        self.solves = 0

    def get(self, url: str) -> str:
        """Fetch ``url``, clearing the interstitial if it appears.

        Returns:
            str: the response body of a real page.

        Raises:
            ChallengeError: the interstitial could not be cleared.
            FetchError: every attempt failed.
        """
        last_error = None
        solves_here = 0

        for attempt in range(1, self.max_retries + 1):
            try:
                if self.budget is not None:
                    self.budget.acquire()
                response = self._session.get(url, timeout=self.timeout)
                self.requests_made += 1
                if self.budget is not None:
                    self.budget.record()

                # Check this before raise_for_status: a 403 is a quota block, and
                # retrying it wastes requests against an IP that is already
                # being told to stop.
                if response.status_code == 403:
                    raise QuotaExhausted(
                        url, attempt,
                        "HTTP 403 Access Denied -- the IP is blocked, not merely "
                        "rate-limited (robots.txt returns 403 too)",
                    )

                # 429 is the polite version of the same message: back off for as
                # long as we are told, then carry on. Distinct from 403, which is
                # not an invitation to retry.
                if response.status_code == 429:
                    retry_after = self._retry_after(response)
                    if self.budget is not None:
                        self.budget.hold(retry_after, "HTTP 429 Too Many Requests")
                    else:
                        time.sleep(retry_after)
                    continue

                response.raise_for_status()
                html = response.text

                if is_challenge(html):
                    if solves_here >= MAX_SOLVES_PER_URL:
                        raise ChallengeError(
                            url,
                            attempt,
                            f"still challenged after {solves_here} solve(s) -- the "
                            "handshake is being rejected, not merely re-issued",
                        )
                    self._solve(url, html)
                    solves_here += 1
                    continue

                # The budget already paced this request; sleeping again would
                # double-count the delay.
                if self.budget is None:
                    time.sleep(self.sleep_ok)
                return html

            except (ChallengeError, QuotaExhausted):
                raise
            except Exception as error:  # noqa: BLE001 -- retry on anything transient
                last_error = error
                if attempt < self.max_retries:
                    time.sleep(self.sleep_fail)

        raise FetchError(url, self.max_retries, last_error)

    @staticmethod
    def _retry_after(response, default=120.0):
        """Seconds to wait per the Retry-After header, honouring it if sane."""
        raw = (response.headers or {}).get("Retry-After")
        if not raw:
            return default
        try:
            seconds = float(str(raw).strip())
        except ValueError:
            return default  # HTTP-date form; not worth parsing for this
        # Clamp: a hostile or mistaken header should not hang the run for a day.
        return max(1.0, min(seconds, 1800.0))

    def _solve(self, url: str, html: str) -> None:
        """POST the interstitial answer. See the module docstring for the shape."""
        try:
            token, pow_value = _parse_challenge(html)
        except ValueError as error:
            raise ChallengeError(url, 1, str(error)) from error

        response = self._session.post(
            config.VERIFY_URL,
            data=json.dumps({"bm-verify": token, "pow": pow_value}),
            headers={"Content-Type": "application/json", "Referer": url},
            timeout=self.timeout,
        )
        self.requests_made += 1
        self.solves += 1

        # A successful solve answers {"reload": true}; some variants answer with a
        # location to follow. Anything else means the answer was rejected.
        body = response.text or ""
        try:
            payload = json.loads(body)
        except ValueError:
            payload = {}
        if not isinstance(payload, dict) or not ({"reload", "location"} & payload.keys()):
            raise ChallengeError(
                url,
                1,
                f"/_sec/verify returned HTTP {response.status_code} with body "
                f"{body[:200]!r}; expected 'reload' or 'location'",
            )

        time.sleep(self.sleep_ok)
