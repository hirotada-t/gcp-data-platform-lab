#!/usr/bin/env python3
"""Meta Ads -> Parquet exporter.

Features
--------
* Discovers ad accounts accessible by one or more access tokens.
* Calculates spend for the latest six-month selection window.
* Deduplicates accounts by account_id without persisting access tokens.
* Exports the 33 datasets described in the project requirements.
* Fetches period-relevant campaign/ad set/ad/creative metadata from IDs found in
  period-scoped ad Insights, avoiding unbounded /ads and /adcreatives scans.
* Writes request/report statistics and partial-failure details.

Authentication
--------------
Single token:
    export FB_ACCESS_TOKEN='...'
    export FB_TOKEN_SOURCE_ID='token-1'

Multiple tokens (recommended: store only environment variable names):
    python meta_ads_export.py ... --tokens-config tokens.json

    tokens.json:
    [
      {"token_source_id": "source-a", "env_var": "FB_TOKEN_A"},
      {"token_source_id": "source-b", "env_var": "FB_TOKEN_B"}
    ]

The token itself is never written to output files or logs.
"""

from __future__ import annotations

import argparse
import calendar
import dataclasses
import datetime as dt
import hashlib
import json
import logging
import math
import os
import random
import re
import sys
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, MutableMapping, Sequence
from urllib.parse import urlparse

import pandas as pd
import requests

LOG = logging.getLogger("meta_ads_export")

DEFAULT_API_VERSION = os.environ.get("FB_API_VERSION", "v25.0")
DEFAULT_BASE_URL = "https://graph.facebook.com"
DEFAULT_PAGE_LIMIT = 500
DEFAULT_METADATA_BATCH_SIZE = 25
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_TIMEOUT_SECONDS = 180
DEFAULT_SPEND_THRESHOLD = Decimal("1")

ACTION_COLUMNS = ("actions", "action_values", "conversions", "conversion_values")
VIDEO_ACTION_COLUMNS = (
    "video_play_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_p100_watched_actions",
)
NESTED_METRIC_COLUMNS = ACTION_COLUMNS + VIDEO_ACTION_COLUMNS
NUMERIC_METRIC_COLUMNS = ("impressions", "clicks", "spend")
RANKING_COLUMNS = (
    "quality_ranking",
    "engagement_rate_ranking",
    "conversion_rate_ranking",
)
VIDEO_RATE_COLUMNS = (
    "video_play_rate",
    "video_p25_completion_rate",
    "video_p50_completion_rate",
    "video_p75_completion_rate",
    "video_p95_completion_rate",
    "video_p100_completion_rate",
)
AD_DAILY_SPLIT_JOIN_KEYS = (
    "date_start",
    "date_stop",
    "account_id",
    "campaign_id",
    "adset_id",
    "ad_id",
)
AD_DAILY_SPLIT_COMPONENTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("base", ()),
    ("conversion", ACTION_COLUMNS),
    ("video", VIDEO_ACTION_COLUMNS),
    ("ranking", RANKING_COLUMNS),
)
DATE_FALLBACK_CHUNK_DAYS = 7
DATE_FALLBACK_META_ERRORS = {
    (1, 99),
    (2, 1504044),
}

COMMON_METRICS = [
    "impressions",
    "clicks",
    "spend",
    "actions",
    "action_values",
    "conversions",
    "conversion_values",
    "video_play_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_p100_watched_actions",
]

LEVEL_DIMENSIONS: dict[str, list[str]] = {
    "account": ["account_id", "account_name"],
    "campaign": ["account_id", "account_name", "campaign_id", "campaign_name"],
    "adset": [
        "account_id",
        "account_name",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
    ],
    "ad": [
        "account_id",
        "account_name",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
    ],
}

PREFECTURE_MAP: dict[str, str] = {
    "Hokkaido": "北海道",
    "Aomori": "青森県",
    "Iwate": "岩手県",
    "Miyagi": "宮城県",
    "Akita": "秋田県",
    "Yamagata": "山形県",
    "Fukushima": "福島県",
    "Ibaraki": "茨城県",
    "Tochigi": "栃木県",
    "Gunma": "群馬県",
    "Saitama": "埼玉県",
    "Chiba": "千葉県",
    "Tokyo": "東京都",
    "Kanagawa": "神奈川県",
    "Niigata": "新潟県",
    "Toyama": "富山県",
    "Ishikawa": "石川県",
    "Fukui": "福井県",
    "Yamanashi": "山梨県",
    "Nagano": "長野県",
    "Gifu": "岐阜県",
    "Shizuoka": "静岡県",
    "Aichi": "愛知県",
    "Mie": "三重県",
    "Shiga": "滋賀県",
    "Kyoto": "京都府",
    "Osaka": "大阪府",
    "Hyogo": "兵庫県",
    "Nara": "奈良県",
    "Wakayama": "和歌山県",
    "Tottori": "鳥取県",
    "Shimane": "島根県",
    "Okayama": "岡山県",
    "Hiroshima": "広島県",
    "Yamaguchi": "山口県",
    "Tokushima": "徳島県",
    "Kagawa": "香川県",
    "Ehime": "愛媛県",
    "Kochi": "高知県",
    "Fukuoka": "福岡県",
    "Saga": "佐賀県",
    "Nagasaki": "長崎県",
    "Kumamoto": "熊本県",
    "Oita": "大分県",
    "Miyazaki": "宮崎県",
    "Kagoshima": "鹿児島県",
    "Okinawa": "沖縄県",
}

ACCOUNT_STATUS_NAMES = {
    1: "ACTIVE",
    2: "DISABLED",
    3: "UNSETTLED",
    7: "PENDING_RISK_REVIEW",
    8: "PENDING_SETTLEMENT",
    9: "IN_GRACE_PERIOD",
    100: "PENDING_CLOSURE",
    101: "CLOSED",
    201: "ANY_ACTIVE",
    202: "ANY_CLOSED",
}


@dataclasses.dataclass(frozen=True)
class TokenSource:
    token_source_id: str
    access_token: str


@dataclasses.dataclass(frozen=True)
class ReportSpec:
    dataset_id: str
    level: str
    time_increment: str | int | None = None
    breakdowns: tuple[str, ...] = ()
    include_rankings: bool = False

    def fields(self) -> list[str]:
        result = ["date_start", "date_stop", *LEVEL_DIMENSIONS[self.level], *COMMON_METRICS]
        if self.include_rankings:
            result.extend(RANKING_COLUMNS)
        return list(dict.fromkeys(result))


REPORT_SPECS: tuple[ReportSpec, ...] = (
    ReportSpec("account_daily", "account", time_increment=1),
    ReportSpec("account_monthly", "account", time_increment="monthly"),
    ReportSpec("campaign_period", "campaign"),
    ReportSpec("campaign_daily", "campaign", time_increment=1),
    ReportSpec("campaign_monthly", "campaign", time_increment="monthly"),
    ReportSpec("adset_period", "adset"),
    ReportSpec("device_period", "account", breakdowns=("device_platform",)),
    ReportSpec("device_daily", "account", time_increment=1, breakdowns=("device_platform",)),
    ReportSpec("age_period", "account", breakdowns=("age",)),
    ReportSpec("age_daily", "account", time_increment=1, breakdowns=("age",)),
    ReportSpec("gender_period", "account", breakdowns=("gender",)),
    ReportSpec("gender_daily", "account", time_increment=1, breakdowns=("gender",)),
    ReportSpec(
        "hourly_period",
        "account",
        breakdowns=("hourly_stats_aggregated_by_audience_time_zone",),
    ),
    ReportSpec(
        "hourly_daily",
        "account",
        time_increment=1,
        breakdowns=("hourly_stats_aggregated_by_audience_time_zone",),
    ),
    ReportSpec("region_daily", "account", time_increment=1, breakdowns=("region",)),
    ReportSpec("ad_period", "ad", include_rankings=True),
    ReportSpec("ad_daily", "ad", time_increment=1, include_rankings=True),
    ReportSpec("asset_image_period", "ad", breakdowns=("image_asset",)),
    ReportSpec("asset_video_period", "ad", breakdowns=("video_asset",)),
    ReportSpec("asset_body_period", "ad", breakdowns=("body_asset",)),
    ReportSpec("asset_title_period", "ad", breakdowns=("title_asset",)),
    ReportSpec("asset_description_period", "ad", breakdowns=("description_asset",)),
    ReportSpec("asset_link_url_period", "ad", breakdowns=("link_url_asset",)),
    ReportSpec(
        "asset_link_url_daily",
        "ad",
        time_increment=1,
        breakdowns=("link_url_asset",),
    ),
)

DIRECT_DATASET_IDS = {spec.dataset_id for spec in REPORT_SPECS}
METADATA_DATASET_IDS = {
    "campaign_metadata",
    "adset_metadata",
    "ad_metadata",
    "creative_metadata",
}
DERIVED_DATASET_IDS = {
    "creative_period",
    "creative_daily",
    "landing_page_period",
    "landing_page_daily",
    "weekday_period",
}
ALL_DATASET_IDS = METADATA_DATASET_IDS | DIRECT_DATASET_IDS | DERIVED_DATASET_IDS


@dataclasses.dataclass
class RequestStats:
    request_count: int = 0
    retry_count: int = 0
    rate_limit_count: int = 0
    http_5xx_count: int = 0
    elapsed_seconds: float = 0.0
    usage_headers: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    error_events: list[dict[str, Any]] = dataclasses.field(default_factory=list)


class MetaAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        code: int | None = None,
        subcode: int | None = None,
        error_type: str | None = None,
        fbtrace_id: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
        debug_link: str | None = None,
        error_mid: str | None = None,
        retry_after_seconds: float | None = None,
        retryable: bool = False,
        response_body: Any = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.subcode = subcode
        self.error_type = error_type
        self.fbtrace_id = fbtrace_id
        self.request_id = request_id
        self.trace_id = trace_id
        self.debug_link = debug_link
        self.error_mid = error_mid
        self.retry_after_seconds = retry_after_seconds
        self.retryable = retryable
        self.response_body = response_body


class ReportComponentError(RuntimeError):
    """Wrap a report-component failure while preserving the underlying API error."""

    def __init__(self, component: str, cause: BaseException) -> None:
        super().__init__(f"{component}: {cause}")
        self.component = component
        self.cause = cause


class MetaClient:
    RETRYABLE_META_CODES = {1, 2, 4, 17, 32, 613, 80004}

    def __init__(
        self,
        token: TokenSource,
        api_version: str,
        *,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_url: str = DEFAULT_BASE_URL,
    ) -> None:
        self.token = token
        self.api_version = normalize_api_version(api_version)
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token.access_token}"})
        self.stats = RequestStats()

    def url(self, path: str = "") -> str:
        clean = path.lstrip("/")
        base = f"{self.base_url}/{self.api_version}"
        return f"{base}/{clean}" if clean else f"{base}/"

    def request_json(
        self,
        method: str,
        url_or_path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = url_or_path if url_or_path.startswith("http") else self.url(url_or_path)
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            started = time.monotonic()
            self.stats.request_count += 1
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    timeout=self.timeout_seconds,
                )
                self.stats.elapsed_seconds += time.monotonic() - started
                self._capture_usage_headers(response)

                body = self._parse_json(response)
                if response.ok and "error" not in body:
                    return body

                error = self._to_api_error(response, body)
                self._capture_error_event(error, attempt)
                if response.status_code == 429 or error.code in {4, 17, 32, 613, 80004}:
                    self.stats.rate_limit_count += 1
                if response.status_code >= 500:
                    self.stats.http_5xx_count += 1
                if not error.retryable or attempt >= self.max_attempts:
                    raise error
                last_error = error
                self._sleep_before_retry(response, attempt, error)
            except (requests.Timeout, requests.ConnectionError) as exc:
                self.stats.elapsed_seconds += time.monotonic() - started
                last_error = exc
                transport_error = MetaAPIError(
                    f"通信エラー: {type(exc).__name__}",
                    error_type=type(exc).__name__,
                    retryable=True,
                )
                self._capture_error_event(transport_error, attempt)
                if attempt >= self.max_attempts:
                    raise transport_error from exc
                self._sleep_before_retry(None, attempt, transport_error)

        raise MetaAPIError(f"APIリクエスト失敗: {last_error}")

    def paged_data(
        self,
        path: str,
        params: Mapping[str, Any],
    ) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        page = 0
        next_url: str | None = self.url(path)
        next_params: Mapping[str, Any] | None = params
        while next_url:
            payload = self.request_json("GET", next_url, params=next_params)
            page += 1
            data = payload.get("data", [])
            if not isinstance(data, list):
                raise MetaAPIError("ページングレスポンスのdataが配列ではありません")
            yield page, [row for row in data if isinstance(row, dict)]
            paging = payload.get("paging") or {}
            next_url = paging.get("next")
            next_params = None

    def _parse_json(self, response: requests.Response) -> dict[str, Any]:
        try:
            parsed = response.json()
        except ValueError:
            parsed = {
                "error": {
                    "message": "Meta APIからJSON以外のレスポンスを受信しました",
                    "code": None,
                }
            }
        if not isinstance(parsed, dict):
            return {"data": parsed}
        return parsed

    def _to_api_error(
        self,
        response: requests.Response,
        body: dict[str, Any],
    ) -> MetaAPIError:
        status = response.status_code
        raw_value = body.get("error")
        raw = raw_value if isinstance(raw_value, dict) else {}
        message = str(raw.get("message") or f"HTTP {status}")
        code = int(raw["code"]) if str(raw.get("code", "")).isdigit() else None
        subcode = (
            int(raw["error_subcode"])
            if str(raw.get("error_subcode", "")).isdigit()
            else None
        )
        retry_after_seconds: float | None = None
        retry_after_raw = response.headers.get("Retry-After")
        if retry_after_raw:
            try:
                retry_after_seconds = float(retry_after_raw)
            except ValueError:
                retry_after_seconds = None
        retryable = status == 429 or status >= 500 or code in self.RETRYABLE_META_CODES
        return MetaAPIError(
            message,
            http_status=status,
            code=code,
            subcode=subcode,
            error_type=str(raw.get("type")) if raw.get("type") else None,
            fbtrace_id=str(raw.get("fbtrace_id")) if raw.get("fbtrace_id") else None,
            request_id=response.headers.get("x-fb-request-id"),
            trace_id=response.headers.get("x-fb-trace-id"),
            debug_link=response.headers.get("debug-link"),
            error_mid=response.headers.get("error-mid"),
            retry_after_seconds=retry_after_seconds,
            retryable=retryable,
            response_body=body,
        )

    def _capture_error_event(self, error: MetaAPIError, attempt: int) -> None:
        event = {
            "checked_at": utc_now_iso(),
            "attempt": attempt,
            **exception_details(error),
        }
        self.stats.error_events.append(event)

    def _sleep_before_retry(
        self,
        response: requests.Response | None,
        attempt: int,
        error: Exception,
    ) -> None:
        self.stats.retry_count += 1
        retry_after: float | None = None
        if response is not None:
            raw = response.headers.get("Retry-After")
            if raw:
                try:
                    retry_after = float(raw)
                except ValueError:
                    retry_after = None
        wait = retry_after if retry_after is not None else min(60.0, 2 ** attempt * 2.5)
        wait += random.uniform(0.0, 1.0)
        LOG.warning(
            "Meta APIを再試行します: attempt=%s wait=%.1fs %s",
            attempt,
            wait,
            format_error_for_log(error),
        )
        time.sleep(wait)

    def _capture_usage_headers(self, response: requests.Response) -> None:
        row: dict[str, Any] = {
            "checked_at": utc_now_iso(),
            "http_status": response.status_code,
        }
        for name in (
            "x-business-use-case-usage",
            "x-fb-ads-insights-throttle",
            "x-app-usage",
            "x-ad-account-usage",
        ):
            value = response.headers.get(name)
            if value:
                row[name] = parse_json_or_text(value)
        if len(row) > 2:
            self.stats.usage_headers.append(row)


def normalize_api_version(value: str) -> str:
    cleaned = value.strip().rstrip("/")
    if "/" in cleaned:
        cleaned = cleaned.rsplit("/", 1)[-1]
    return cleaned if cleaned.startswith("v") else f"v{cleaned}"


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_json_or_text(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def sanitize_message(message: str) -> str:
    sanitized = re.sub(r"(?i)access_token=[^&\s]+", "access_token=***", message)
    sanitized = re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._~+\-/=]+", r"\1***", sanitized)
    return sanitized


def exception_details(error: BaseException) -> dict[str, Any]:
    if isinstance(error, ReportComponentError):
        details = exception_details(error.cause)
        details["component"] = error.component
        return details
    if isinstance(error, MetaAPIError):
        details: dict[str, Any] = {
            "reason": sanitize_message(str(error)),
            "exception_type": type(error).__name__,
            "http_status": error.http_status,
            "meta_code": error.code,
            "meta_subcode": error.subcode,
            "meta_error_type": error.error_type,
            "fbtrace_id": error.fbtrace_id,
            "request_id": error.request_id,
            "trace_id": error.trace_id,
            "debug_link": error.debug_link,
            "error_mid": error.error_mid,
            "retry_after_seconds": error.retry_after_seconds,
            "retryable": error.retryable,
        }
    else:
        details = {
            "reason": sanitize_message(str(error)),
            "exception_type": type(error).__name__,
        }
    return {key: value for key, value in details.items() if value is not None}


def format_error_for_log(error: BaseException) -> str:
    details = exception_details(error)
    ordered_keys = (
        "component",
        "http_status",
        "meta_code",
        "meta_subcode",
        "meta_error_type",
        "fbtrace_id",
        "request_id",
        "trace_id",
        "retry_after_seconds",
        "reason",
    )
    parts = [
        f"{key}={details[key]}"
        for key in ordered_keys
        if key in details and details[key] not in (None, "")
    ]
    if not parts:
        return f"reason={sanitize_message(str(error))}"
    return " ".join(parts)


def mask_account_id(account_id: str) -> str:
    normalized = account_id.removeprefix("act_")
    if len(normalized) <= 6:
        return "***"
    return f"***{normalized[-6:]}"


def normalize_account_id(account_id: str) -> str:
    value = account_id.strip().removeprefix("act_")
    if not value.isdigit():
        raise ValueError("account_idは数字、またはact_付きの数字で指定してください")
    return value


def subtract_months(value: dt.date, months: int) -> dt.date:
    year = value.year
    month = value.month - months
    while month <= 0:
        year -= 1
        month += 12
    day = min(value.day, calendar.monthrange(year, month)[1])
    return dt.date(year, month, day)


def date_range_json(since: dt.date, until: dt.date) -> str:
    return json.dumps(
        {"since": since.isoformat(), "until": until.isoformat()},
        separators=(",", ":"),
    )


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def chunks(items: Sequence[str], size: int) -> Iterator[list[str]]:
    for index in range(0, len(items), size):
        yield list(items[index : index + size])


def stable_account_key(account_id: str) -> str:
    return hashlib.sha256(account_id.encode("utf-8")).hexdigest()[:12]


def json_compact(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except InvalidOperation:
        return Decimal("0")


def load_token_sources(config_path: Path | None) -> list[TokenSource]:
    if config_path is None:
        token = os.environ.get("FB_ACCESS_TOKEN", "").strip()
        if not token:
            raise SystemExit("FB_ACCESS_TOKEN が未設定です")
        source_id = os.environ.get("FB_TOKEN_SOURCE_ID", "token-1").strip() or "token-1"
        return [TokenSource(source_id, token)]

    config = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(config, list):
        raise SystemExit("tokens configはJSON配列にしてください")

    result: list[TokenSource] = []
    for item in config:
        if not isinstance(item, dict):
            raise SystemExit("tokens configの各要素はobjectにしてください")
        source_id = str(item.get("token_source_id") or "").strip()
        env_var = str(item.get("env_var") or "").strip()
        if not source_id or not env_var:
            raise SystemExit("tokens configにはtoken_source_idとenv_varが必要です")
        token = os.environ.get(env_var, "").strip()
        if not token:
            raise SystemExit(f"環境変数 {env_var} が未設定です")
        result.append(TokenSource(source_id, token))
    if not result:
        raise SystemExit("有効なtoken sourceがありません")
    return result


METADATA_DATASET_COLUMNS: dict[str, list[str]] = {
    "campaign_metadata": [
        "campaign_id",
        "campaign_name",
        "status",
        "effective_status",
        "campaign_labels",
    ],
    "adset_metadata": [
        "adset_id",
        "adset_name",
        "campaign_id",
        "status",
        "effective_status",
        "adset_labels",
    ],
    "ad_metadata": [
        "ad_id",
        "ad_name",
        "campaign_id",
        "adset_id",
        "status",
        "effective_status",
        "ad_labels",
        "creative_id",
    ],
    "creative_metadata": [
        "creative_id",
        "creative_name",
        "object_url",
        "asset_feed_spec",
        "url_tags",
        "fallback_lp_url",
        "fallback_lp_url_source",
        "source_ad_ids",
    ],
}


def dataset_expected_columns(dataset_id: str) -> list[str]:
    """Return stable output columns, including for zero-row Parquet files."""
    spec = next(
        (item for item in REPORT_SPECS if item.dataset_id == dataset_id),
        None,
    )
    if spec is not None:
        columns = [*spec.fields(), *spec.breakdowns, *VIDEO_RATE_COLUMNS]
        if dataset_id == "region_daily":
            columns.extend(("prefecture_ja", "prefecture_mapping_status"))
        if spec.level in {"campaign", "adset", "ad"}:
            columns.extend(
                ("campaign_labels", "adset_labels", "ad_labels", "creative_id")
            )
        return list(dict.fromkeys(columns))

    if dataset_id in METADATA_DATASET_COLUMNS:
        return list(METADATA_DATASET_COLUMNS[dataset_id])

    aggregate_metrics = [
        *NUMERIC_METRIC_COLUMNS,
        *NESTED_METRIC_COLUMNS,
        *VIDEO_RATE_COLUMNS,
    ]
    if dataset_id in {"creative_period", "creative_daily"}:
        return [
            "date_start",
            "date_stop",
            "account_id",
            "account_name",
            "creative_id",
            *aggregate_metrics,
            "creative_name",
        ]
    if dataset_id in {"landing_page_period", "landing_page_daily"}:
        return [
            "date_start",
            "date_stop",
            "account_id",
            "account_name",
            "landing_page_url",
            "landing_page_url_source",
            *aggregate_metrics,
        ]
    if dataset_id == "weekday_period":
        return [
            "account_id",
            "account_name",
            "weekday_number",
            "weekday_ja",
            *aggregate_metrics,
        ]
    return []


def empty_column_dtype(column: str) -> str:
    if column in NUMERIC_METRIC_COLUMNS or column in VIDEO_RATE_COLUMNS:
        return "float64"
    if column == "weekday_number":
        return "Int64"
    return "string"


def dataframe_for_output(
    rows: list[dict[str, Any]],
    *,
    expected_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    expected = list(dict.fromkeys(expected_columns or ()))

    # 列がない場合はまず追加する
    for column in expected:
        if column not in frame.columns:
            frame[column] = pd.NA

    # 空データ・全NULLを含め、必ず規定型へ揃える
    for column in expected:
        dtype = empty_column_dtype(column)

        if dtype == "float64":
            frame[column] = pd.to_numeric(
                frame[column],
                errors="coerce",
            ).astype("float64")

        elif dtype == "Int64":
            frame[column] = pd.to_numeric(
                frame[column],
                errors="coerce",
            ).astype("Int64")

        else:
            frame[column] = frame[column].astype("string")

    if expected:
        extras = [
            column
            for column in frame.columns
            if column not in expected
        ]
        frame = frame[[*expected, *extras]]

    return frame


def write_parquet(
    rows: list[dict[str, Any]],
    path: Path,
    *,
    expected_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = dataframe_for_output(rows, expected_columns=expected_columns)
    try:
        frame.to_parquet(path, index=False, engine="pyarrow")
    except ImportError as exc:
        raise RuntimeError(
            "Parquet出力にはpyarrowが必要です。`python -m pip install pyarrow`を実行してください。"
        ) from exc
    return {
        "path": str(path),
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "size_bytes": path.stat().st_size,
    }


def write_dataset_parquet(
    dataset_id: str,
    rows: list[dict[str, Any]],
    path: Path,
) -> dict[str, Any]:
    return write_parquet(
        rows,
        path,
        expected_columns=dataset_expected_columns(dataset_id),
    )


def write_json(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def list_accessible_accounts(client: MetaClient) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    params = {
        "fields": "account_id,name,currency,account_status",
        "limit": DEFAULT_PAGE_LIMIT,
    }
    for _, page_rows in client.paged_data("me/adaccounts", params):
        rows.extend(page_rows)
    return rows


def fetch_account_spend(
    client: MetaClient,
    account_id: str,
    since: dt.date,
    until: dt.date,
) -> Decimal:
    params = {
        "fields": "spend",
        "level": "account",
        "time_range": date_range_json(since, until),
        "limit": 10,
    }
    rows: list[dict[str, Any]] = []
    for _, page_rows in client.paged_data(f"act_{account_id}/insights", params):
        rows.extend(page_rows)
    return sum((decimal_or_zero(row.get("spend")) for row in rows), Decimal("0"))


def discover_accounts(
    token_sources: list[TokenSource],
    *,
    api_version: str,
    since: dt.date,
    until: dt.date,
    spend_threshold: Decimal,
    timeout_seconds: int,
    max_attempts: int,
) -> tuple[list[dict[str, Any]], dict[str, TokenSource], list[dict[str, Any]]]:
    candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    token_lookup: dict[str, TokenSource] = {}
    audit: list[dict[str, Any]] = []

    for token_source in token_sources:
        client = MetaClient(
            token_source,
            api_version,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )
        try:
            accounts = list_accessible_accounts(client)
        except MetaAPIError as error:
            audit.append(
                {
                    "token_source_id": token_source.token_source_id,
                    "status": "token_error",
                    **exception_details(error),
                    "checked_at": utc_now_iso(),
                }
            )
            continue

        for account in accounts:
            account_id = str(account.get("account_id") or account.get("id", "")).removeprefix("act_")
            if not account_id:
                continue
            candidate = {
                "account_id": account_id,
                "account_name": account.get("name"),
                "currency": account.get("currency"),
                "account_status": account.get("account_status"),
                "account_status_name": ACCOUNT_STATUS_NAMES.get(
                    int(account.get("account_status") or 0), "UNKNOWN"
                ),
                "token_source_id": token_source.token_source_id,
            }
            candidates[account_id].append(candidate)

    selected_rows: list[dict[str, Any]] = []
    for account_id in sorted(candidates):
        account_candidates = candidates[account_id]
        duplicate_source_ids: list[str] = []
        canonical = dict(account_candidates[0])
        spend: Decimal | None = None
        reasons: list[str] = []
        chosen_token: TokenSource | None = None

        for candidate in account_candidates:
            source = next(
                item for item in token_sources if item.token_source_id == candidate["token_source_id"]
            )
            client = MetaClient(
                source,
                api_version,
                timeout_seconds=timeout_seconds,
                max_attempts=max_attempts,
            )
            try:
                spend = fetch_account_spend(client, account_id, since, until)
                chosen_token = source
                canonical.update(candidate)
                break
            except MetaAPIError as error:
                audit.append(
                    {
                        "account_id": account_id,
                        "token_source_id": source.token_source_id,
                        "status": "spend_error",
                        **exception_details(error),
                        "checked_at": utc_now_iso(),
                    }
                )

        status = int(canonical.get("account_status") or 0)
        if status != 1:
            reasons.append(f"inactive_status:{status}")
        if spend is None:
            reasons.append("permission_or_spend_query_failed")
            spend = Decimal("0")
        elif spend < spend_threshold:
            reasons.append("spend_below_threshold")

        is_target = not reasons
        chosen_source_id = chosen_token.token_source_id if chosen_token else canonical["token_source_id"]
        duplicate_source_ids = sorted(
            {row["token_source_id"] for row in account_candidates if row["token_source_id"] != chosen_source_id}
        )
        canonical.update(
            {
                "token_source_id": chosen_source_id,
                "spend_last_6_months": float(spend),
                "spend_window_since": since.isoformat(),
                "spend_window_until": until.isoformat(),
                "is_export_target": is_target,
                "excluded_reason": ";".join(reasons) if reasons else None,
                "duplicate_token_source_ids": json_compact(duplicate_source_ids),
                "checked_at": utc_now_iso(),
            }
        )
        selected_rows.append(canonical)
        if chosen_token:
            token_lookup[account_id] = chosen_token

    return selected_rows, token_lookup, audit


def fetch_insights_report(
    client: MetaClient,
    account_id: str,
    spec: ReportSpec,
    since: dt.date,
    until: dt.date,
    *,
    page_limit: int,
    use_account_attribution_setting: bool,
) -> tuple[list[dict[str, Any]], int]:
    params: dict[str, Any] = {
        "fields": ",".join(spec.fields()),
        "level": spec.level,
        "time_range": date_range_json(since, until),
        "limit": page_limit,
    }
    if spec.time_increment is not None:
        params["time_increment"] = spec.time_increment
    if spec.breakdowns:
        params["breakdowns"] = ",".join(spec.breakdowns)
    if use_account_attribution_setting:
        params["use_account_attribution_setting"] = "true"

    rows: list[dict[str, Any]] = []
    page_count = 0
    for page, page_rows in client.paged_data(f"act_{account_id}/insights", params):
        page_count = page
        rows.extend(page_rows)
    return normalize_insights_rows(rows, spec), page_count


def fetch_insights_fields(
    client: MetaClient,
    account_id: str,
    *,
    fields: Sequence[str],
    since: dt.date,
    until: dt.date,
    page_limit: int,
    time_increment: str | int | None = None,
    breakdowns: Sequence[str] = (),
    use_account_attribution_setting: bool,
) -> tuple[list[dict[str, Any]], int]:
    params: dict[str, Any] = {
        "fields": ",".join(dict.fromkeys(fields)),
        "level": "ad",
        "time_range": date_range_json(since, until),
        "limit": page_limit,
    }
    if time_increment is not None:
        params["time_increment"] = time_increment
    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)
    if use_account_attribution_setting:
        params["use_account_attribution_setting"] = "true"

    rows: list[dict[str, Any]] = []
    page_count = 0
    for page, page_rows in client.paged_data(f"act_{account_id}/insights", params):
        page_count = page
        rows.extend(page_rows)
    return rows, page_count


def unwrap_meta_api_error(error: BaseException) -> MetaAPIError | None:
    current: BaseException = error
    while isinstance(current, ReportComponentError):
        current = current.cause
    return current if isinstance(current, MetaAPIError) else None


def should_use_date_fallback(error: BaseException) -> bool:
    api_error = unwrap_meta_api_error(error)
    if api_error is None or not api_error.retryable:
        return False
    if (api_error.code, api_error.subcode) in DATE_FALLBACK_META_ERRORS:
        return True
    return api_error.http_status is not None and api_error.http_status >= 500


def iter_date_chunks(
    since: dt.date,
    until: dt.date,
    days: int,
) -> Iterator[tuple[dt.date, dt.date]]:
    if days <= 0:
        raise ValueError("date chunk daysは1以上で指定してください")
    cursor = since
    while cursor <= until:
        chunk_until = min(until, cursor + dt.timedelta(days=days - 1))
        yield cursor, chunk_until
        cursor = chunk_until + dt.timedelta(days=1)


def annotate_error_events(
    client: MetaClient,
    start_index: int,
    *,
    dataset_id: str,
    component: str | None,
    fallback_stage: str,
    range_since: dt.date,
    range_until: dt.date,
) -> None:
    for event in client.stats.error_events[start_index:]:
        event.setdefault("dataset_id", dataset_id)
        if component:
            event.setdefault("component", component)
        event.setdefault("fallback_stage", fallback_stage)
        event.setdefault("range_since", range_since.isoformat())
        event.setdefault("range_until", range_until.isoformat())


def fetch_insights_fields_with_date_fallback(
    client: MetaClient,
    account_id: str,
    *,
    fields: Sequence[str],
    since: dt.date,
    until: dt.date,
    page_limit: int,
    time_increment: str | int | None,
    breakdowns: Sequence[str] = (),
    use_account_attribution_setting: bool,
    dataset_id: str,
    component: str | None = None,
    chunk_days: int = DATE_FALLBACK_CHUNK_DAYS,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    """Retry a failed long-range Insights query as 7-day, then 1-day chunks."""
    full_errors_before = len(client.stats.error_events)
    try:
        rows, pages = fetch_insights_fields(
            client,
            account_id,
            fields=fields,
            since=since,
            until=until,
            page_limit=page_limit,
            time_increment=time_increment,
            breakdowns=breakdowns,
            use_account_attribution_setting=use_account_attribution_setting,
        )
        return rows, pages, {
            "used": False,
            "original_since": since.isoformat(),
            "original_until": until.isoformat(),
        }
    except Exception as error:
        annotate_error_events(
            client,
            full_errors_before,
            dataset_id=dataset_id,
            component=component,
            fallback_stage="full_range",
            range_since=since,
            range_until=until,
        )
        if not should_use_date_fallback(error):
            raise
        trigger_details = exception_details(error)
        LOG.warning(
            "期間分割フォールバックを開始します: account=%s dataset=%s component=%s "
            "since=%s until=%s chunk_days=%s %s",
            mask_account_id(account_id),
            dataset_id,
            component or "-",
            since,
            until,
            chunk_days,
            format_error_for_log(error),
        )

    all_rows: list[dict[str, Any]] = []
    total_pages = 0
    chunk_stats: list[dict[str, Any]] = []
    total_days = (until - since).days + 1
    first_level_days = chunk_days if total_days > chunk_days else 1

    for chunk_since, chunk_until in iter_date_chunks(since, until, first_level_days):
        request_before = client.stats.request_count
        retries_before = client.stats.retry_count
        chunk_started = time.monotonic()
        chunk_errors_before = len(client.stats.error_events)
        fallback_level = f"{first_level_days}_day"
        try:
            chunk_rows, chunk_pages = fetch_insights_fields(
                client,
                account_id,
                fields=fields,
                since=chunk_since,
                until=chunk_until,
                page_limit=page_limit,
                time_increment=time_increment,
                breakdowns=breakdowns,
                use_account_attribution_setting=use_account_attribution_setting,
            )
        except Exception as chunk_error:
            annotate_error_events(
                client,
                chunk_errors_before,
                dataset_id=dataset_id,
                component=component,
                fallback_stage=fallback_level,
                range_since=chunk_since,
                range_until=chunk_until,
            )
            if not should_use_date_fallback(chunk_error) or chunk_since == chunk_until:
                raise

            LOG.warning(
                "日次フォールバックへ切り替えます: account=%s dataset=%s component=%s "
                "since=%s until=%s %s",
                mask_account_id(account_id),
                dataset_id,
                component or "-",
                chunk_since,
                chunk_until,
                format_error_for_log(chunk_error),
            )
            fallback_level = "1_day"
            chunk_rows = []
            chunk_pages = 0
            for day_since, day_until in iter_date_chunks(chunk_since, chunk_until, 1):
                day_errors_before = len(client.stats.error_events)
                try:
                    day_rows, day_pages = fetch_insights_fields(
                        client,
                        account_id,
                        fields=fields,
                        since=day_since,
                        until=day_until,
                        page_limit=page_limit,
                        time_increment=time_increment,
                        breakdowns=breakdowns,
                        use_account_attribution_setting=use_account_attribution_setting,
                    )
                except Exception:
                    annotate_error_events(
                        client,
                        day_errors_before,
                        dataset_id=dataset_id,
                        component=component,
                        fallback_stage="1_day",
                        range_since=day_since,
                        range_until=day_until,
                    )
                    raise
                chunk_rows.extend(day_rows)
                chunk_pages += day_pages

        all_rows.extend(chunk_rows)
        total_pages += chunk_pages
        chunk_stats.append(
            {
                "since": chunk_since.isoformat(),
                "until": chunk_until.isoformat(),
                "fallback_level": fallback_level,
                "rows": len(chunk_rows),
                "pages": chunk_pages,
                "api_calls": client.stats.request_count - request_before,
                "retries": client.stats.retry_count - retries_before,
                "elapsed_seconds": round(time.monotonic() - chunk_started, 3),
            }
        )

    return all_rows, total_pages, {
        "used": True,
        "original_since": since.isoformat(),
        "original_until": until.isoformat(),
        "chunk_days": chunk_days,
        "chunks": chunk_stats,
        "rows": len(all_rows),
        "pages": total_pages,
        "trigger_error": trigger_details,
    }

def split_row_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(row.get(column) or "") for column in AD_DAILY_SPLIT_JOIN_KEYS)


def merge_ad_daily_components(
    component_rows: Mapping[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    base_rows = component_rows.get("base", [])
    merged_by_key: dict[tuple[str, ...], dict[str, Any]] = {
        split_row_key(row): dict(row) for row in base_rows
    }
    base_keys = set(merged_by_key)
    component_key_stats: dict[str, dict[str, int]] = {}

    component_fields = dict(AD_DAILY_SPLIT_COMPONENTS)
    for component, rows in component_rows.items():
        if component == "base":
            continue
        row_map = {split_row_key(row): row for row in rows}
        row_keys = set(row_map)
        component_key_stats[component] = {
            "rows": len(rows),
            "missing_from_component": len(base_keys - row_keys),
            "extra_vs_base": len(row_keys - base_keys),
        }
        for key, source in row_map.items():
            target = merged_by_key.get(key)
            if target is None:
                # Extremely unusual, but retain the row rather than silently dropping it.
                target = {
                    column: source.get(column)
                    for column in (
                        *AD_DAILY_SPLIT_JOIN_KEYS,
                        *NUMERIC_METRIC_COLUMNS,
                    )
                }
                merged_by_key[key] = target
            for column in component_fields.get(component, ()):
                if column in source:
                    target[column] = source[column]

    rows = list(merged_by_key.values())
    rows.sort(
        key=lambda row: (
            str(row.get("date_start") or ""),
            str(row.get("ad_id") or ""),
        )
    )
    return rows, {
        "base_rows": len(base_rows),
        "merged_rows": len(rows),
        "component_key_stats": component_key_stats,
    }


def fetch_ad_daily_split(
    client: MetaClient,
    account_id: str,
    spec: ReportSpec,
    since: dt.date,
    until: dt.date,
    *,
    page_limit: int,
    use_account_attribution_setting: bool,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    """Fetch ad_daily by field group, with date fallback for heavy components."""
    component_rows: dict[str, list[dict[str, Any]]] = {}
    component_stats: list[dict[str, Any]] = []
    total_pages = 0
    used_date_fallback = False

    base_fields = [
        "date_start",
        "date_stop",
        *LEVEL_DIMENSIONS["ad"],
        *NUMERIC_METRIC_COLUMNS,
    ]
    component_dimensions = [
        "date_start",
        "date_stop",
        "account_id",
        "campaign_id",
        "adset_id",
        "ad_id",
        *NUMERIC_METRIC_COLUMNS,
    ]

    for component, metrics in AD_DAILY_SPLIT_COMPONENTS:
        fields = base_fields if component == "base" else [*component_dimensions, *metrics]
        request_before = client.stats.request_count
        retries_before = client.stats.retry_count
        component_started = time.monotonic()
        try:
            rows, pages, date_fallback = fetch_insights_fields_with_date_fallback(
                client,
                account_id,
                fields=fields,
                since=since,
                until=until,
                page_limit=page_limit,
                time_increment=spec.time_increment,
                use_account_attribution_setting=use_account_attribution_setting,
                dataset_id="ad_daily",
                component=component,
            )
        except Exception as error:
            raise ReportComponentError(component, error) from error

        used_date_fallback = used_date_fallback or bool(date_fallback.get("used"))
        component_rows[component] = rows
        total_pages += pages
        component_stat: dict[str, Any] = {
            "component": component,
            "rows": len(rows),
            "pages": pages,
            "api_calls": client.stats.request_count - request_before,
            "retries": client.stats.retry_count - retries_before,
            "elapsed_seconds": round(time.monotonic() - component_started, 3),
            "fetch_mode": (
                "date_fallback" if date_fallback.get("used") else "single_range"
            ),
        }
        if date_fallback.get("used"):
            component_stat["date_fallback"] = date_fallback
        component_stats.append(component_stat)

    merged_rows, merge_stats = merge_ad_daily_components(component_rows)
    normalized = normalize_insights_rows(merged_rows, spec)
    return normalized, total_pages, {
        "fetch_mode": (
            "split_fields_with_date_fallback" if used_date_fallback else "split"
        ),
        "split_components": component_stats,
        "split_merge": merge_stats,
    }

def normalize_insights_rows(rows: list[dict[str, Any]], spec: ReportSpec) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    asset_key = spec.breakdowns[0] if spec.breakdowns and spec.breakdowns[0].endswith("_asset") else None

    for original in rows:
        row = dict(original)
        for column in NESTED_METRIC_COLUMNS:
            if column in row:
                row[column] = json_compact(row[column])
        for column in NUMERIC_METRIC_COLUMNS:
            if column in row:
                row[column] = to_float_or_none(row[column])
        if asset_key and asset_key in row:
            row[asset_key] = normalize_asset_value(row[asset_key])
        add_video_rate_columns(row)
        if spec.dataset_id == "region_daily":
            region = str(row.get("region") or "").strip()
            prefecture = map_prefecture(region)
            row["prefecture_ja"] = prefecture
            row["prefecture_mapping_status"] = "mapped" if prefecture else "unmapped"
        normalized.append(row)
    return normalized


def to_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def action_total(value: Any) -> float | None:
    parsed = parse_json_value(value)
    if not isinstance(parsed, list):
        return None
    total = 0.0
    found = False
    for item in parsed:
        if not isinstance(item, dict):
            continue
        numeric = to_float_or_none(item.get("value"))
        if numeric is not None:
            total += numeric
            found = True
    return total if found else None


def add_video_rate_columns(row: MutableMapping[str, Any]) -> None:
    impressions = to_float_or_none(row.get("impressions"))
    plays = action_total(row.get("video_play_actions"))
    row["video_play_rate"] = safe_ratio(plays, impressions)
    for percentage in (25, 50, 75, 95, 100):
        completed = action_total(row.get(f"video_p{percentage}_watched_actions"))
        row[f"video_p{percentage}_completion_rate"] = safe_ratio(completed, plays)


def safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def parse_json_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str) or not value:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def normalize_asset_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("url", "website_url", "name", "id", "text", "video_id", "image_hash"):
            if value.get(key) not in (None, ""):
                return str(value[key])
        return json_compact(value)
    return json_compact(value)


def map_prefecture(region: str) -> str | None:
    if not region:
        return None
    cleaned = re.sub(r"\s+(Prefecture|Metropolis|府|県|都)$", "", region, flags=re.IGNORECASE).strip()
    if region in PREFECTURE_MAP:
        return PREFECTURE_MAP[region]
    if cleaned in PREFECTURE_MAP:
        return PREFECTURE_MAP[cleaned]
    if region in PREFECTURE_MAP.values():
        return region
    return None


def extract_period_ids(ad_period_rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    return {
        "campaign": sorted({str(row["campaign_id"]) for row in ad_period_rows if row.get("campaign_id")}),
        "adset": sorted({str(row["adset_id"]) for row in ad_period_rows if row.get("adset_id")}),
        "ad": sorted({str(row["ad_id"]) for row in ad_period_rows if row.get("ad_id")}),
    }


def fetch_objects_by_ids(
    client: MetaClient,
    ids: list[str],
    fields: str,
    *,
    batch_size: int,
    object_kind: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    success: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    batch_count = 0

    for batch in chunks(ids, batch_size):
        batch_count += 1
        payload = client.request_json(
            "GET",
            "",
            params={"ids": ",".join(batch), "fields": fields},
        )
        returned_ids: set[str] = set()
        for object_id, item in payload.items():
            returned_ids.add(str(object_id))
            if not isinstance(item, dict):
                failures.append(
                    {
                        "object_kind": object_kind,
                        "object_id": str(object_id),
                        "reason": "invalid_response_type",
                    }
                )
                continue
            if "error" in item:
                raw_error = item.get("error") if isinstance(item.get("error"), dict) else {}
                failures.append(
                    {
                        "object_kind": object_kind,
                        "object_id": str(object_id),
                        "reason": sanitize_message(str(raw_error.get("message") or "unknown")),
                        "meta_code": raw_error.get("code"),
                        "meta_subcode": raw_error.get("error_subcode"),
                        "meta_error_type": raw_error.get("type"),
                        "fbtrace_id": raw_error.get("fbtrace_id"),
                    }
                )
            else:
                success.append(item)
        for missing_id in set(batch) - returned_ids:
            failures.append(
                {
                    "object_kind": object_kind,
                    "object_id": missing_id,
                    "reason": "missing_from_batch_response",
                }
            )
    return success, failures, batch_count


def normalize_labels(value: Any) -> str:
    if isinstance(value, dict) and isinstance(value.get("data"), list):
        value = value["data"]
    if not isinstance(value, list):
        return "[]"
    normalized = [
        {"id": item.get("id"), "name": item.get("name")}
        for item in value
        if isinstance(item, dict)
    ]
    return json_compact(normalized) or "[]"


def build_metadata(
    client: MetaClient,
    period_ids: dict[str, list[str]],
    *,
    batch_size: int,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    stats: dict[str, Any] = {}

    campaign_rows, campaign_failures, campaign_batches = fetch_objects_by_ids(
        client,
        period_ids["campaign"],
        "id,name,status,effective_status,adlabels{id,name}",
        batch_size=batch_size,
        object_kind="campaign",
    )
    failures.extend(campaign_failures)
    campaign_metadata = [
        {
            "campaign_id": row.get("id"),
            "campaign_name": row.get("name"),
            "status": row.get("status"),
            "effective_status": row.get("effective_status"),
            "campaign_labels": normalize_labels(row.get("adlabels")),
        }
        for row in campaign_rows
    ]

    adset_rows, adset_failures, adset_batches = fetch_objects_by_ids(
        client,
        period_ids["adset"],
        "id,name,campaign_id,status,effective_status,adlabels{id,name}",
        batch_size=batch_size,
        object_kind="adset",
    )
    failures.extend(adset_failures)
    adset_metadata = [
        {
            "adset_id": row.get("id"),
            "adset_name": row.get("name"),
            "campaign_id": row.get("campaign_id"),
            "status": row.get("status"),
            "effective_status": row.get("effective_status"),
            "adset_labels": normalize_labels(row.get("adlabels")),
        }
        for row in adset_rows
    ]

    ad_fields = (
        "id,name,campaign_id,adset_id,status,effective_status,adlabels{id,name},"
        "creative{id,name,object_url,asset_feed_spec,url_tags}"
    )
    ad_rows, ad_failures, ad_batches = fetch_objects_by_ids(
        client,
        period_ids["ad"],
        ad_fields,
        batch_size=batch_size,
        object_kind="ad",
    )
    failures.extend(ad_failures)

    ad_metadata: list[dict[str, Any]] = []
    creative_by_id: dict[str, dict[str, Any]] = {}
    creative_source_ads: dict[str, list[str]] = defaultdict(list)

    for row in ad_rows:
        creative = row.get("creative") if isinstance(row.get("creative"), dict) else {}
        creative_id = str(creative.get("id")) if creative.get("id") else None
        ad_id = str(row.get("id")) if row.get("id") else None
        ad_metadata.append(
            {
                "ad_id": ad_id,
                "ad_name": row.get("name"),
                "campaign_id": row.get("campaign_id"),
                "adset_id": row.get("adset_id"),
                "status": row.get("status"),
                "effective_status": row.get("effective_status"),
                "ad_labels": normalize_labels(row.get("adlabels")),
                "creative_id": creative_id,
            }
        )
        if creative_id:
            creative_by_id[creative_id] = creative
            if ad_id:
                creative_source_ads[creative_id].append(ad_id)

    creative_metadata: list[dict[str, Any]] = []
    for creative_id, creative in sorted(creative_by_id.items()):
        fallback_url, fallback_source = extract_creative_fallback_url(creative)
        creative_metadata.append(
            {
                "creative_id": creative_id,
                "creative_name": creative.get("name"),
                "object_url": creative.get("object_url"),
                "asset_feed_spec": json_compact(creative.get("asset_feed_spec")),
                "url_tags": creative.get("url_tags"),
                "fallback_lp_url": fallback_url,
                "fallback_lp_url_source": fallback_source,
                "source_ad_ids": json_compact(sorted(set(creative_source_ads[creative_id]))),
            }
        )

    stats.update(
        {
            "campaign_ids": len(period_ids["campaign"]),
            "adset_ids": len(period_ids["adset"]),
            "ad_ids": len(period_ids["ad"]),
            "campaign_batches": campaign_batches,
            "adset_batches": adset_batches,
            "ad_batches": ad_batches,
            "campaign_success": len(campaign_metadata),
            "adset_success": len(adset_metadata),
            "ad_success": len(ad_metadata),
            "creative_ids": len(creative_metadata),
            "metadata_failures": len(failures),
        }
    )
    return (
        {
            "campaign_metadata": campaign_metadata,
            "adset_metadata": adset_metadata,
            "ad_metadata": ad_metadata,
            "creative_metadata": creative_metadata,
        },
        failures,
        stats,
    )


def extract_creative_fallback_url(creative: Mapping[str, Any]) -> tuple[str | None, str | None]:
    asset_feed_spec = creative.get("asset_feed_spec")
    urls = collect_asset_feed_link_urls(asset_feed_spec)
    if urls:
        return urls[0], "creative.asset_feed_spec.link_urls"
    object_url = creative.get("object_url")
    if isinstance(object_url, str) and is_http_url(object_url):
        return object_url, "creative.object_url"
    return None, None


def collect_asset_feed_link_urls(value: Any) -> list[str]:
    """Extract landing-page candidates without treating image/video URLs as LPs."""
    if not isinstance(value, dict):
        return []
    link_urls = value.get("link_urls")
    if not isinstance(link_urls, list):
        return []
    urls: list[str] = []
    for item in link_urls:
        if not isinstance(item, dict):
            continue
        for key in ("website_url", "url", "deeplink_url", "display_url"):
            candidate = item.get(key)
            if isinstance(candidate, str) and is_http_url(candidate):
                urls.append(candidate)
                break
    return list(dict.fromkeys(urls))


def is_http_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
    except ValueError:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def rows_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    return {str(row[key]): row for row in rows if row.get(key) not in (None, "")}


def join_metadata_into_insights(
    rows: list[dict[str, Any]],
    metadata: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    campaigns = rows_by_key(metadata["campaign_metadata"], "campaign_id")
    adsets = rows_by_key(metadata["adset_metadata"], "adset_id")
    ads = rows_by_key(metadata["ad_metadata"], "ad_id")
    result: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        campaign = campaigns.get(str(row.get("campaign_id")), {})
        adset = adsets.get(str(row.get("adset_id")), {})
        ad = ads.get(str(row.get("ad_id")), {})
        row["campaign_labels"] = campaign.get("campaign_labels", "[]")
        row["adset_labels"] = adset.get("adset_labels", "[]")
        row["ad_labels"] = ad.get("ad_labels", "[]")
        row["creative_id"] = ad.get("creative_id")
        result.append(row)
    return result


def merge_action_values(values: Iterable[Any]) -> str:
    totals: dict[str, float] = defaultdict(float)
    extra: dict[str, dict[str, Any]] = {}
    for value in values:
        parsed = parse_json_value(value)
        if not isinstance(parsed, list):
            continue
        for item in parsed:
            if not isinstance(item, dict):
                continue
            action_type = str(item.get("action_type") or "unknown")
            numeric = to_float_or_none(item.get("value"))
            if numeric is not None:
                totals[action_type] += numeric
            extra[action_type] = {k: v for k, v in item.items() if k not in {"value"}}
    result = []
    for action_type in sorted(set(totals) | set(extra)):
        row = dict(extra.get(action_type, {}))
        row["action_type"] = action_type
        row["value"] = totals.get(action_type, 0.0)
        result.append(row)
    return json_compact(result) or "[]"


def aggregate_rows(
    rows: list[dict[str, Any]],
    group_keys: list[str],
    *,
    carry_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(key) for key in group_keys)].append(row)

    output: list[dict[str, Any]] = []
    for key_values, group in groups.items():
        row = dict(zip(group_keys, key_values))
        for column in NUMERIC_METRIC_COLUMNS:
            values = [to_float_or_none(item.get(column)) for item in group]
            numeric = [value for value in values if value is not None]
            row[column] = sum(numeric) if numeric else None
        for column in NESTED_METRIC_COLUMNS:
            row[column] = merge_action_values(item.get(column) for item in group)
        for column in carry_columns or []:
            unique_values = [item.get(column) for item in group if item.get(column) not in (None, "")]
            row[column] = unique_values[0] if unique_values else None
        add_video_rate_columns(row)
        output.append(row)
    return output


def derive_creative_report(
    ad_rows: list[dict[str, Any]],
    metadata: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    enriched = join_metadata_into_insights(ad_rows, metadata)
    creative_names = rows_by_key(metadata["creative_metadata"], "creative_id")
    relevant = [row for row in enriched if row.get("creative_id")]
    output = aggregate_rows(
        relevant,
        ["date_start", "date_stop", "account_id", "account_name", "creative_id"],
    )
    for row in output:
        creative = creative_names.get(str(row.get("creative_id")), {})
        row["creative_name"] = creative.get("creative_name")
    return output


def derive_creative_reports(
    direct_rows: dict[str, list[dict[str, Any]]],
    metadata: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        derive_creative_report(direct_rows.get("ad_period", []), metadata),
        derive_creative_report(direct_rows.get("ad_daily", []), metadata),
    )


def derive_landing_page_report(
    asset_rows: list[dict[str, Any]],
    ad_rows: list[dict[str, Any]],
    metadata: dict[str, list[dict[str, Any]]],
    *,
    daily: bool,
) -> list[dict[str, Any]]:
    fallback_by_creative = rows_by_key(metadata["creative_metadata"], "creative_id")
    ad_meta = rows_by_key(metadata["ad_metadata"], "ad_id")
    candidates: list[dict[str, Any]] = []

    for source in asset_rows:
        row = dict(source)
        url = row.get("link_url_asset")
        if isinstance(url, str) and is_http_url(url):
            row["landing_page_url"] = url
            row["landing_page_url_source"] = "link_url_asset"
            candidates.append(row)

    asset_ad_ids = {str(row.get("ad_id")) for row in candidates if row.get("ad_id")}
    for source in ad_rows:
        ad_id = str(source.get("ad_id")) if source.get("ad_id") else ""
        if ad_id in asset_ad_ids:
            continue
        ad = ad_meta.get(ad_id, {})
        creative = fallback_by_creative.get(str(ad.get("creative_id")), {})
        url = creative.get("fallback_lp_url")
        if isinstance(url, str) and is_http_url(url):
            row = dict(source)
            row["landing_page_url"] = url
            row["landing_page_url_source"] = creative.get("fallback_lp_url_source")
            candidates.append(row)

    keys = ["account_id", "account_name", "landing_page_url", "landing_page_url_source"]
    if daily:
        keys = ["date_start", "date_stop", *keys]
    else:
        keys = ["date_start", "date_stop", *keys]
    return aggregate_rows(candidates, keys)


def derive_weekday_period(account_daily: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    weekday_ja = ["月", "火", "水", "木", "金", "土", "日"]
    for source in account_daily:
        row = dict(source)
        try:
            date_value = dt.date.fromisoformat(str(row.get("date_start")))
        except ValueError:
            continue
        row["weekday_number"] = date_value.weekday()
        row["weekday_ja"] = weekday_ja[date_value.weekday()]
        rows.append(row)
    return aggregate_rows(
        rows,
        ["account_id", "account_name", "weekday_number", "weekday_ja"],
    )


def export_account(
    token_source: TokenSource,
    account_id: str,
    *,
    api_version: str,
    since: dt.date,
    until: dt.date,
    output_root: Path,
    page_limit: int,
    metadata_batch_size: int,
    timeout_seconds: int,
    max_attempts: int,
    selected_datasets: set[str],
    use_account_attribution_setting: bool,
) -> dict[str, Any]:
    client = MetaClient(
        token_source,
        api_version,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
    )
    account_key = stable_account_key(account_id)
    run_dir = output_root / account_key / f"{since.isoformat()}_{until.isoformat()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    direct_rows: dict[str, list[dict[str, Any]]] = {}
    direct_status: dict[str, str] = {}
    report_results: list[dict[str, Any]] = []
    report_errors: list[dict[str, Any]] = []

    required_direct = set(selected_datasets & DIRECT_DATASET_IDS)
    if selected_datasets & METADATA_DATASET_IDS:
        required_direct.add("ad_period")
    if "creative_period" in selected_datasets:
        required_direct.add("ad_period")
    if "creative_daily" in selected_datasets:
        required_direct.add("ad_daily")
    if "landing_page_period" in selected_datasets:
        required_direct.update({"asset_link_url_period", "ad_period"})
    if "landing_page_daily" in selected_datasets:
        required_direct.update({"asset_link_url_daily", "ad_daily", "ad_period"})
    if "weekday_period" in selected_datasets:
        required_direct.add("account_daily")

    specs_to_run = [spec for spec in REPORT_SPECS if spec.dataset_id in required_direct]

    for spec in specs_to_run:
        report_started = time.monotonic()
        request_before = client.stats.request_count
        retries_before = client.stats.retry_count
        fetch_details: dict[str, Any] = {"fetch_mode": "single"}
        try:
            if spec.dataset_id == "ad_daily":
                fetch_details = {"fetch_mode": "split"}
                rows, pages, fetch_details = fetch_ad_daily_split(
                    client,
                    account_id,
                    spec,
                    since,
                    until,
                    page_limit=page_limit,
                    use_account_attribution_setting=use_account_attribution_setting,
                )
            elif spec.dataset_id == "asset_link_url_daily":
                raw_rows, pages, date_fallback = fetch_insights_fields_with_date_fallback(
                    client,
                    account_id,
                    fields=spec.fields(),
                    since=since,
                    until=until,
                    page_limit=page_limit,
                    time_increment=spec.time_increment,
                    breakdowns=spec.breakdowns,
                    use_account_attribution_setting=use_account_attribution_setting,
                    dataset_id=spec.dataset_id,
                )
                rows = normalize_insights_rows(raw_rows, spec)
                fetch_details = {
                    "fetch_mode": (
                        "date_fallback" if date_fallback.get("used") else "single"
                    )
                }
                if date_fallback.get("used"):
                    fetch_details["date_fallback"] = date_fallback
            else:
                rows, pages = fetch_insights_report(
                    client,
                    account_id,
                    spec,
                    since,
                    until,
                    page_limit=page_limit,
                    use_account_attribution_setting=use_account_attribution_setting,
                )

            direct_rows[spec.dataset_id] = rows
            direct_status[spec.dataset_id] = "success"
            result: dict[str, Any] = {
                "dataset_id": spec.dataset_id,
                "status": "success",
                "rows": len(rows),
                "pages": pages,
                "api_calls": client.stats.request_count - request_before,
                "retries": client.stats.retry_count - retries_before,
                "elapsed_seconds": round(time.monotonic() - report_started, 3),
                **fetch_details,
            }
            if spec.dataset_id in selected_datasets:
                result.update(
                    write_dataset_parquet(
                        spec.dataset_id,
                        rows,
                        run_dir / f"{spec.dataset_id}.parquet",
                    )
                )
            report_results.append(result)
            LOG.info(
                "report完了: account=%s dataset=%s rows=%s pages=%s fetch_mode=%s elapsed=%.2fs",
                mask_account_id(account_id),
                spec.dataset_id,
                len(rows),
                pages,
                result.get("fetch_mode"),
                result["elapsed_seconds"],
            )
        except Exception as error:  # continue to final summary
            direct_status[spec.dataset_id] = "failed"
            details = exception_details(error)
            report_errors.append({"dataset_id": spec.dataset_id, **details})
            report_results.append(
                {
                    "dataset_id": spec.dataset_id,
                    "status": "failed",
                    **details,
                    "api_calls": client.stats.request_count - request_before,
                    "retries": client.stats.retry_count - retries_before,
                    "elapsed_seconds": round(time.monotonic() - report_started, 3),
                    **fetch_details,
                }
            )
            LOG.error(
                "report失敗: account=%s dataset=%s %s",
                mask_account_id(account_id),
                spec.dataset_id,
                format_error_for_log(error),
            )

    metadata: dict[str, list[dict[str, Any]]] = {
        "campaign_metadata": [],
        "adset_metadata": [],
        "ad_metadata": [],
        "creative_metadata": [],
    }
    metadata_stats: dict[str, Any] = {}
    metadata_failures: list[dict[str, Any]] = []
    metadata_status = "not_required"

    if selected_datasets & (METADATA_DATASET_IDS | DERIVED_DATASET_IDS):
        if direct_status.get("ad_period") != "success":
            metadata_status = "skipped_dependency_failed"
            for dataset_id in sorted(selected_datasets & METADATA_DATASET_IDS):
                report_results.append(
                    {
                        "dataset_id": dataset_id,
                        "status": "skipped_dependency_failed",
                        "dependency": "ad_period",
                        "reason": "ad_periodの取得失敗によりmetadataを生成できません",
                    }
                )
        else:
            ad_period_rows = direct_rows.get("ad_period", [])
            if not ad_period_rows:
                # A successful zero-row ad_period means the period simply had no ads.
                metadata_status = "success"
                metadata_stats = {
                    "campaign_ids": 0,
                    "adset_ids": 0,
                    "ad_ids": 0,
                    "campaign_batches": 0,
                    "adset_batches": 0,
                    "ad_batches": 0,
                    "campaign_success": 0,
                    "adset_success": 0,
                    "ad_success": 0,
                    "creative_ids": 0,
                    "metadata_failures": 0,
                }
                for dataset_id, rows in metadata.items():
                    if dataset_id in selected_datasets:
                        info = write_dataset_parquet(
                            dataset_id,
                            rows,
                            run_dir / f"{dataset_id}.parquet",
                        )
                        report_results.append(
                            {
                                "dataset_id": dataset_id,
                                "status": "success",
                                "rows": 0,
                                "pages": 0,
                                "empty_reason": "ad_period_zero_rows",
                                **info,
                            }
                        )
            else:
                period_ids = extract_period_ids(ad_period_rows)
                try:
                    metadata, metadata_failures, metadata_stats = build_metadata(
                        client,
                        period_ids,
                        batch_size=metadata_batch_size,
                    )
                    metadata_status = "success"
                    for dataset_id, rows in metadata.items():
                        if dataset_id in selected_datasets:
                            info = write_dataset_parquet(
                                dataset_id,
                                rows,
                                run_dir / f"{dataset_id}.parquet",
                            )
                            report_results.append(
                                {
                                    "dataset_id": dataset_id,
                                    "status": "success",
                                    "rows": len(rows),
                                    "pages": 0,
                                    **info,
                                }
                            )
                except Exception as error:
                    metadata_status = "failed"
                    details = exception_details(error)
                    report_errors.append({"dataset_id": "metadata", **details})
                    for dataset_id in sorted(selected_datasets & METADATA_DATASET_IDS):
                        report_results.append(
                            {
                                "dataset_id": dataset_id,
                                "status": "failed",
                                **details,
                            }
                        )
                    LOG.error(
                        "metadata取得失敗: account=%s %s",
                        mask_account_id(account_id),
                        format_error_for_log(error),
                    )

    # Add labels and creative IDs to direct campaign/ad set/ad reports.
    if metadata["ad_metadata"]:
        for dataset_id, rows in list(direct_rows.items()):
            if any(
                row.get("campaign_id") or row.get("adset_id") or row.get("ad_id")
                for row in rows
            ):
                enriched = join_metadata_into_insights(rows, metadata)
                direct_rows[dataset_id] = enriched
                if dataset_id in selected_datasets:
                    enriched_info = write_dataset_parquet(
                        dataset_id,
                        enriched,
                        run_dir / f"{dataset_id}.parquet",
                    )
                    for report_result in report_results:
                        if (
                            report_result.get("dataset_id") == dataset_id
                            and report_result.get("status") == "success"
                        ):
                            report_result.update(enriched_info)
                            report_result["rows"] = len(enriched)
                            break

    def derived_dependency_failures(
        direct_dependencies: Sequence[str],
        *,
        requires_metadata: bool,
    ) -> list[str]:
        failures = [
            dataset_id
            for dataset_id in direct_dependencies
            if direct_status.get(dataset_id) != "success"
        ]
        if requires_metadata and metadata_status != "success":
            failures.append("metadata")
        return failures

    derived_builders: dict[
        str,
        tuple[tuple[str, ...], bool, Any],
    ] = {
        "creative_period": (
            ("ad_period",),
            True,
            lambda: derive_creative_report(
                direct_rows.get("ad_period", []),
                metadata,
            ),
        ),
        "creative_daily": (
            ("ad_daily",),
            True,
            lambda: derive_creative_report(
                direct_rows.get("ad_daily", []),
                metadata,
            ),
        ),
        "landing_page_period": (
            ("asset_link_url_period", "ad_period"),
            True,
            lambda: derive_landing_page_report(
                direct_rows.get("asset_link_url_period", []),
                direct_rows.get("ad_period", []),
                metadata,
                daily=False,
            ),
        ),
        "landing_page_daily": (
            ("asset_link_url_daily", "ad_daily", "ad_period"),
            True,
            lambda: derive_landing_page_report(
                direct_rows.get("asset_link_url_daily", []),
                direct_rows.get("ad_daily", []),
                metadata,
                daily=True,
            ),
        ),
        "weekday_period": (
            ("account_daily",),
            False,
            lambda: derive_weekday_period(
                direct_rows.get("account_daily", [])
            ),
        ),
    }

    for dataset_id in sorted(selected_datasets & DERIVED_DATASET_IDS):
        dependencies, requires_metadata, builder = derived_builders[dataset_id]
        failed_dependencies = derived_dependency_failures(
            dependencies,
            requires_metadata=requires_metadata,
        )
        if failed_dependencies:
            report_results.append(
                {
                    "dataset_id": dataset_id,
                    "status": "skipped_dependency_failed",
                    "dependencies": list(failed_dependencies),
                    "reason": "依存データセットの取得または生成に失敗しました",
                }
            )
            continue

        try:
            rows = builder()
            info = write_dataset_parquet(
                dataset_id,
                rows,
                run_dir / f"{dataset_id}.parquet",
            )
            report_results.append(
                {
                    "dataset_id": dataset_id,
                    "status": "success",
                    "rows": len(rows),
                    "pages": 0,
                    **info,
                }
            )
        except Exception as error:
            details = exception_details(error)
            report_errors.append({"dataset_id": dataset_id, **details})
            report_results.append(
                {"dataset_id": dataset_id, "status": "failed", **details}
            )

    if metadata_failures:
        write_parquet(metadata_failures, run_dir / "metadata_failures.parquet")
    write_json(client.stats.usage_headers, run_dir / "api_usage_headers.json")
    if client.stats.error_events:
        write_json(client.stats.error_events, run_dir / "api_error_events.json")

    elapsed = round(time.monotonic() - started, 3)
    summary = {
        "status": "partial_failure" if report_errors else "success",
        "account_key": account_key,
        "period": {"since": since.isoformat(), "until": until.isoformat()},
        "api_version": client.api_version,
        "token_source_id": token_source.token_source_id,
        "elapsed_seconds": elapsed,
        "request_stats": dataclasses.asdict(client.stats),
        "metadata_stats": metadata_stats,
        "reports": report_results,
        "errors": report_errors,
        "output_dir": str(run_dir),
        "checked_at": utc_now_iso(),
    }
    write_json(summary, run_dir / "run_summary.json")
    return summary


def parse_dataset_selection(raw: str | None) -> set[str]:
    if not raw or raw.strip().lower() == "all":
        return set(ALL_DATASET_IDS)
    values = {item.strip() for item in raw.split(",") if item.strip()}
    unknown = values - ALL_DATASET_IDS
    if unknown:
        raise SystemExit(f"不明なdataset: {', '.join(sorted(unknown))}")
    return values



def selection_value_is_true(value: Any) -> bool:
    """Interpret is_export_target values loaded from Parquet conservatively."""
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def selection_string(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    cleaned = str(value).strip()
    return cleaned or None


def load_selection_targets(selection_file: Path) -> list[dict[str, Any]]:
    """Load export targets previously produced by the accounts subcommand."""
    if not selection_file.is_file():
        raise SystemExit(f"selection fileが見つかりません: {selection_file}")
    try:
        frame = pd.read_parquet(selection_file)
    except Exception as error:
        raise SystemExit(
            f"selection fileを読み込めませんでした: {selection_file}: {type(error).__name__}"
        ) from error

    required_columns = {"account_id", "is_export_target"}
    missing_columns = required_columns - set(frame.columns)
    if missing_columns:
        raise SystemExit(
            "selection fileに必要な列がありません: "
            + ", ".join(sorted(missing_columns))
        )

    targets: list[dict[str, Any]] = []
    for source in frame.to_dict(orient="records"):
        if not selection_value_is_true(source.get("is_export_target")):
            continue
        raw_account_id = selection_string(source.get("account_id"))
        if raw_account_id is None:
            continue
        try:
            account_id = normalize_account_id(raw_account_id)
        except ValueError as error:
            raise SystemExit(
                f"selection fileに不正なaccount_idがあります: {sanitize_message(str(error))}"
            ) from error
        row = dict(source)
        row["account_id"] = account_id
        row["token_source_id"] = selection_string(source.get("token_source_id"))
        targets.append(row)
    return targets


def resolve_selection_token(
    row: Mapping[str, Any],
    token_sources: Sequence[TokenSource],
) -> TokenSource | None:
    source_id = selection_string(row.get("token_source_id"))
    if source_id:
        for source in token_sources:
            if source.token_source_id == source_id:
                return source
        return None
    if len(token_sources) == 1:
        return token_sources[0]
    return None


def account_run_dir(
    output_root: Path,
    account_id: str,
    since: dt.date,
    until: dt.date,
) -> Path:
    return (
        output_root
        / stable_account_key(account_id)
        / f"{since.isoformat()}_{until.isoformat()}"
    )


def load_successful_existing_summary(
    output_root: Path,
    account_id: str,
    since: dt.date,
    until: dt.date,
    selected_datasets: set[str],
) -> dict[str, Any] | None:
    """Return a reusable success summary only when all requested datasets succeeded."""
    summary_path = account_run_dir(
        output_root, account_id, since, until
    ) / "run_summary.json"
    if not summary_path.is_file():
        return None
    try:
        raw = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        LOG.warning(
            "既存run_summary.jsonを読み込めないため再実行します: account=%s path=%s",
            mask_account_id(account_id),
            summary_path,
        )
        return None
    if not isinstance(raw, dict) or raw.get("status") != "success":
        return None

    period = raw.get("period") if isinstance(raw.get("period"), dict) else {}
    if period.get("since") != since.isoformat() or period.get("until") != until.isoformat():
        return None

    reports = raw.get("reports") if isinstance(raw.get("reports"), list) else []
    successful_datasets = {
        str(item.get("dataset_id"))
        for item in reports
        if isinstance(item, dict) and item.get("status") == "success"
    }
    if not selected_datasets.issubset(successful_datasets):
        return None

    reused = dict(raw)
    reused["resume_action"] = "skipped_successful"
    reused["reused_summary_path"] = str(summary_path)
    return reused


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Meta Adsのアカウント選定と33データセットParquet出力"
    )
    parser.add_argument("--tokens-config", type=Path, help="token source設定JSON")
    parser.add_argument("--api-version", default=DEFAULT_API_VERSION)
    parser.add_argument("--output-dir", type=Path, default=Path("./meta-ads-output"))
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS)
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))

    subparsers = parser.add_subparsers(dest="command", required=True)

    accounts = subparsers.add_parser("accounts", help="対象アカウント選定データを出力")
    accounts.add_argument("--selection-since", type=parse_date)
    accounts.add_argument("--selection-until", type=parse_date)
    accounts.add_argument("--spend-threshold", type=Decimal, default=DEFAULT_SPEND_THRESHOLD)

    export = subparsers.add_parser("export", help="指定アカウントの33データセットを出力")
    export.add_argument("--account-id", required=True)
    export.add_argument("--since", type=parse_date, required=True)
    export.add_argument("--until", type=parse_date, required=True)
    export.add_argument("--token-source-id", help="複数token時の明示指定")
    export.add_argument("--page-limit", type=int, default=DEFAULT_PAGE_LIMIT)
    export.add_argument("--metadata-batch-size", type=int, default=DEFAULT_METADATA_BATCH_SIZE)
    export.add_argument("--datasets", default="all", help="allまたはカンマ区切り")
    export.add_argument("--use-account-attribution-setting", action="store_true")

    run = subparsers.add_parser("run-targets", help="対象選定後、export targetを順番に出力")
    run.add_argument("--selection-since", type=parse_date)
    run.add_argument("--selection-until", type=parse_date)
    run.add_argument("--spend-threshold", type=Decimal, default=DEFAULT_SPEND_THRESHOLD)
    run.add_argument("--since", type=parse_date, required=True)
    run.add_argument("--until", type=parse_date, required=True)
    run.add_argument("--page-limit", type=int, default=DEFAULT_PAGE_LIMIT)
    run.add_argument("--metadata-batch-size", type=int, default=DEFAULT_METADATA_BATCH_SIZE)
    run.add_argument("--datasets", default="all")
    run.add_argument("--max-accounts", type=int)
    run.add_argument("--use-account-attribution-setting", action="store_true")

    selected = subparsers.add_parser(
        "run-selected",
        help="保存済みaccount_selection.parquetからexport targetを順番に出力",
    )
    selected.add_argument(
        "--selection-file",
        type=Path,
        required=True,
        help="accountsサブコマンドが生成したaccount_selection.parquet",
    )
    selected.add_argument("--since", type=parse_date, required=True)
    selected.add_argument("--until", type=parse_date, required=True)
    selected.add_argument("--page-limit", type=int, default=DEFAULT_PAGE_LIMIT)
    selected.add_argument(
        "--metadata-batch-size", type=int, default=DEFAULT_METADATA_BATCH_SIZE
    )
    selected.add_argument("--datasets", default="all")
    selected.add_argument("--max-accounts", type=int)
    selected.add_argument(
        "--skip-successful",
        action="store_true",
        help=(
            "同じ出力先・期間に成功済みrun_summary.jsonがあり、"
            "指定datasetがすべて成功しているアカウントをスキップ"
        ),
    )
    selected.add_argument("--use-account-attribution-setting", action="store_true")

    return parser


def default_selection_window(
    since: dt.date | None,
    until: dt.date | None,
) -> tuple[dt.date, dt.date]:
    resolved_until = until or (dt.date.today() - dt.timedelta(days=1))
    resolved_since = since or subtract_months(resolved_until, 6)
    if resolved_since > resolved_until:
        raise SystemExit("selection sinceはuntil以前にしてください")
    return resolved_since, resolved_until


def choose_token(
    token_sources: list[TokenSource],
    token_source_id: str | None,
) -> TokenSource:
    if token_source_id:
        for source in token_sources:
            if source.token_source_id == token_source_id:
                return source
        raise SystemExit(f"token_source_idが見つかりません: {token_source_id}")
    if len(token_sources) != 1:
        raise SystemExit("複数token時は--token-source-idを指定してください")
    return token_sources[0]


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    token_sources = load_token_sources(args.tokens_config)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.command == "accounts":
        selection_since, selection_until = default_selection_window(
            args.selection_since, args.selection_until
        )
        rows, _, audit = discover_accounts(
            token_sources,
            api_version=args.api_version,
            since=selection_since,
            until=selection_until,
            spend_threshold=args.spend_threshold,
            timeout_seconds=args.timeout,
            max_attempts=args.max_attempts,
        )
        output = write_parquet(rows, args.output_dir / "account_selection.parquet")
        if audit:
            write_parquet(audit, args.output_dir / "account_selection_audit.parquet")
        write_json(
            {
                "selection_since": selection_since.isoformat(),
                "selection_until": selection_until.isoformat(),
                "spend_threshold": str(args.spend_threshold),
                "accounts": len(rows),
                "targets": sum(bool(row.get("is_export_target")) for row in rows),
                "output": output,
                "checked_at": utc_now_iso(),
            },
            args.output_dir / "account_selection_summary.json",
        )
        print(json.dumps(output, ensure_ascii=False))
        return 0

    if args.command == "export":
        account_id = normalize_account_id(args.account_id)
        if args.since > args.until:
            raise SystemExit("sinceはuntil以前にしてください")
        token_source = choose_token(token_sources, args.token_source_id)
        summary = export_account(
            token_source,
            account_id,
            api_version=args.api_version,
            since=args.since,
            until=args.until,
            output_root=args.output_dir,
            page_limit=args.page_limit,
            metadata_batch_size=args.metadata_batch_size,
            timeout_seconds=args.timeout,
            max_attempts=args.max_attempts,
            selected_datasets=parse_dataset_selection(args.datasets),
            use_account_attribution_setting=args.use_account_attribution_setting,
        )
        print(json.dumps(summary, ensure_ascii=False, default=str))
        return 0 if summary["status"] == "success" else 3

    if args.command == "run-selected":
        if args.since > args.until:
            raise SystemExit("sinceはuntil以前にしてください")

        targets = load_selection_targets(args.selection_file)
        if args.max_accounts is not None:
            if args.max_accounts < 0:
                raise SystemExit("max-accountsは0以上で指定してください")
            targets = targets[: args.max_accounts]

        LOG.info(
            "選定済みアカウントを読み込みました: targets=%s selection_file=%s",
            len(targets),
            args.selection_file,
        )

        selected_datasets = parse_dataset_selection(args.datasets)
        summaries: list[dict[str, Any]] = []
        skipped_successful = 0
        for index, row in enumerate(targets, start=1):
            account_id = str(row["account_id"])

            if args.skip_successful:
                existing_summary = load_successful_existing_summary(
                    args.output_dir,
                    account_id,
                    args.since,
                    args.until,
                    selected_datasets,
                )
                if existing_summary is not None:
                    skipped_successful += 1
                    summaries.append(existing_summary)
                    LOG.info(
                        "成功済みアカウントをスキップします: %s/%s account=%s",
                        index,
                        len(targets),
                        mask_account_id(account_id),
                    )
                    continue

            token_source = resolve_selection_token(row, token_sources)
            if token_source is None:
                source_id = selection_string(row.get("token_source_id"))
                reason = (
                    f"token_source_not_found:{source_id}"
                    if source_id
                    else "usable_token_not_found"
                )
                summaries.append(
                    {
                        "status": "failed",
                        "account_key": stable_account_key(account_id),
                        "reason": reason,
                    }
                )
                LOG.error(
                    "対象アカウントをスキップします: %s/%s account=%s reason=%s",
                    index,
                    len(targets),
                    mask_account_id(account_id),
                    reason,
                )
                continue

            LOG.info(
                "対象アカウントを処理します: %s/%s account=%s",
                index,
                len(targets),
                mask_account_id(account_id),
            )
            summaries.append(
                export_account(
                    token_source,
                    account_id,
                    api_version=args.api_version,
                    since=args.since,
                    until=args.until,
                    output_root=args.output_dir,
                    page_limit=args.page_limit,
                    metadata_batch_size=args.metadata_batch_size,
                    timeout_seconds=args.timeout,
                    max_attempts=args.max_attempts,
                    selected_datasets=selected_datasets,
                    use_account_attribution_setting=args.use_account_attribution_setting,
                )
            )

        write_json(summaries, args.output_dir / "run_selected_summary.json")
        result = {
            "selection_file": str(args.selection_file),
            "targets": len(targets),
            "skipped_successful": skipped_successful,
            "executed": len(targets) - skipped_successful,
            "success": sum(row.get("status") == "success" for row in summaries),
            "partial_or_failed": sum(
                row.get("status") != "success" for row in summaries
            ),
        }
        print(json.dumps(result, ensure_ascii=False))
        return 0 if all(row.get("status") == "success" for row in summaries) else 3

    if args.command == "run-targets":
        selection_since, selection_until = default_selection_window(
            args.selection_since, args.selection_until
        )
        rows, token_lookup, audit = discover_accounts(
            token_sources,
            api_version=args.api_version,
            since=selection_since,
            until=selection_until,
            spend_threshold=args.spend_threshold,
            timeout_seconds=args.timeout,
            max_attempts=args.max_attempts,
        )
        write_parquet(rows, args.output_dir / "account_selection.parquet")
        if audit:
            write_parquet(audit, args.output_dir / "account_selection_audit.parquet")
        targets = [row for row in rows if row.get("is_export_target")]
        if args.max_accounts is not None:
            targets = targets[: args.max_accounts]
        summaries: list[dict[str, Any]] = []
        for index, row in enumerate(targets, start=1):
            account_id = str(row["account_id"])
            token_source = token_lookup.get(account_id)
            if token_source is None:
                summaries.append(
                    {
                        "status": "failed",
                        "account_key": stable_account_key(account_id),
                        "reason": "usable_token_not_found",
                    }
                )
                continue
            LOG.info(
                "対象アカウントを処理します: %s/%s account=%s",
                index,
                len(targets),
                mask_account_id(account_id),
            )
            summaries.append(
                export_account(
                    token_source,
                    account_id,
                    api_version=args.api_version,
                    since=args.since,
                    until=args.until,
                    output_root=args.output_dir,
                    page_limit=args.page_limit,
                    metadata_batch_size=args.metadata_batch_size,
                    timeout_seconds=args.timeout,
                    max_attempts=args.max_attempts,
                    selected_datasets=parse_dataset_selection(args.datasets),
                    use_account_attribution_setting=args.use_account_attribution_setting,
                )
            )
        write_json(summaries, args.output_dir / "run_targets_summary.json")
        print(
            json.dumps(
                {
                    "targets": len(targets),
                    "success": sum(row.get("status") == "success" for row in summaries),
                    "partial_or_failed": sum(row.get("status") != "success" for row in summaries),
                },
                ensure_ascii=False,
            )
        )
        return 0 if all(row.get("status") == "success" for row in summaries) else 3

    parser.error("unsupported command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
