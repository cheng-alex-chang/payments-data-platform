"""Extract daily FX reference rates from the Frankfurter API (ECB data).

Phase 1 of the Snowflake ELT branch. Pulls one rate per (date, currency) for the currencies
that appear in the payments seed, normalized so every row carries ``rate_to_usd`` -- the number
of USD that one unit of ``currency`` is worth. That choice makes the downstream Snowflake join
a single multiply: ``usd_amount = amount * rate_to_usd``.

Frankfurter is free and keyless. Its time-series endpoint answers the whole window in one call,
but only for business days (no weekends/holidays); those gaps are forward-filled later in the
Snowflake SQL (``dim_fx_rates``), not here.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import time
from dataclasses import asdict, dataclass
from typing import Callable

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

FRANKFURTER_BASE = "https://api.frankfurter.app"
BASE_CURRENCY = "USD"
# Quote currencies present in the payments seed (USD is the base, so its rate_to_usd is 1.0).
QUOTE_CURRENCIES = ("EUR", "GBP", "CAD", "AUD", "CHF")

# A JSON getter takes (url, params) and returns the parsed body. Injecting it lets tests
# substitute a fake without any network, while production uses the retrying implementation.
JsonGetter = Callable[[str, dict], dict]


@dataclass(frozen=True)
class FxRate:
    rate_date: str       # ISO date, e.g. "2025-06-30"
    currency: str        # ISO 4217 code, e.g. "EUR"
    rate_to_usd: float   # USD per 1 unit of `currency`


def _get_json_with_retry(
    url: str,
    params: dict,
    *,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    timeout: float = 15.0,
) -> dict:
    """GET JSON with exponential backoff on transient failures (timeouts, HTTP 429, HTTP 5xx)."""
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            # 429 (rate limited) and 5xx (server) are worth retrying; 4xx are not.
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(f"transient HTTP {response.status_code}")
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            last_error = error
            if attempt == max_attempts:
                break
            delay = base_delay * (2 ** (attempt - 1))
            LOGGER.warning(
                "FX request failed (attempt %d/%d): %s -- retrying in %.1fs",
                attempt, max_attempts, error, delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"FX API failed after {max_attempts} attempts: {last_error}")


def fetch_fx_rates(
    start: str,
    end: str,
    *,
    base: str = BASE_CURRENCY,
    quotes: tuple[str, ...] = QUOTE_CURRENCIES,
    getter: JsonGetter = _get_json_with_retry,
) -> list[FxRate]:
    """Fetch one FxRate per (business day, currency) across the inclusive [start, end] window.

    Frankfurter's ``from=USD&to=EUR,...`` returns each rate as *currency per 1 USD*. We invert
    to ``rate_to_usd`` (USD per 1 unit of currency) so the warehouse join is a plain multiply.
    """
    url = f"{FRANKFURTER_BASE}/{start}..{end}"
    payload = getter(url, {"from": base, "to": ",".join(quotes)})
    rates_by_date: dict[str, dict[str, float]] = payload.get("rates", {})

    fx_rates: list[FxRate] = []
    for rate_date in sorted(rates_by_date):
        # The base currency (USD) never appears in `to`, so add its identity rate explicitly.
        fx_rates.append(FxRate(rate_date, base, 1.0))
        for currency, currency_per_usd in rates_by_date[rate_date].items():
            if not currency_per_usd:  # skip nulls / zeros defensively
                continue
            fx_rates.append(FxRate(rate_date, currency, round(1.0 / currency_per_usd, 8)))
    return fx_rates


def default_window(today: dt.date | None = None) -> tuple[str, str]:
    """The last 365 days, matching the payments seed span. ``today`` is injectable for tests."""
    today = today or dt.date.today()
    return (today - dt.timedelta(days=365)).isoformat(), today.isoformat()


def main(argv: list[str] | None = None) -> list[FxRate]:
    parser = argparse.ArgumentParser(description="Extract FX rates from Frankfurter (ECB).")
    parser.add_argument("--start", help="ISO start date (default: today-365d)")
    parser.add_argument("--end", help="ISO end date (default: today)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from the live API and print a sample, without staging downstream",
    )
    args = parser.parse_args(argv)

    start, end = default_window()
    start = args.start or start
    end = args.end or end

    fx_rates = fetch_fx_rates(start, end)
    LOGGER.info(
        "Fetched %d FX rows (%s..%s) for %s",
        len(fx_rates), start, end, ",".join((BASE_CURRENCY, *QUOTE_CURRENCIES)),
    )

    if args.dry_run:
        for rate in fx_rates[:3] + fx_rates[-3:]:
            print(asdict(rate))
    return fx_rates


if __name__ == "__main__":  # pragma: no cover
    main()
