#!/usr/bin/env python3
"""Deterministic Facebook Ads Library runtime for the USA-only OpenClaw MVP."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import html as html_lib
import http.cookiejar
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple


UTC = dt.timezone.utc
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
URL_RE = re.compile(r"https?://[^\s]+")
DOMAIN_HOST_RE = re.compile(r"^[a-z0-9.-]+\.[a-z]{2,}$")
UNRESOLVED_DYNAMIC_TEMPLATE_RE = re.compile(r"\{\{\s*[^{}]+\s*\}\}")
MAX_CHAT_MESSAGE_LEN = 3900
MAX_MEDIA_CAPTION_LEN = 900
TOTAL_COUNT_SENTINEL = 50001
SESSION_UPDATE_UNSET = object()
REDIRECT_MAX_HOPS = 8
INSPECT_SCREENSHOT_DIRNAME = "inspect_screenshots"
INSPECT_SCREENSHOT_TIMEOUT_MS = 20000
INSPECT_SCREENSHOT_NETWORK_IDLE_TIMEOUT_MS = 5000
INSPECT_SCREENSHOT_VIEWPORT_WIDTH = 1440
INSPECT_SCREENSHOT_VIEWPORT_HEIGHT = 1600
STACK_HINT_SAMPLE_MAX_TARGETS = 5
STACK_HINT_REQUEST_TIMEOUT_SEC = 6
DIVERSITY_PREFETCH_MAX_PAGES = 3
DIVERSITY_PREFETCH_MAX_CANDIDATES = 30
TEMP_MEDIA_ROOT_DIRNAME = "facebook-ads-runtime"
DEFAULT_COMMAND_LOOKBACK_DAYS = 14
CREATIVE_AVAILABILITY_RESOLVED = "resolved"
CREATIVE_AVAILABILITY_PLACEHOLDER = "placeholder_unresolved"
CREATIVE_AVAILABILITY_UNAVAILABLE = "unavailable"
CREATIVE_PLACEHOLDER_NOTE = "unavailable - unresolved dynamic template placeholder"
INSPECT_NON_PIVOT_BUCKET_NOTE = "unavailable - inspect was not run from a current page/domain pivot bucket"
INSPECT_NON_PIVOT_BUCKET_CONTEXT_LINE = (
    "unavailable in this run; inspect was not started from a current page/domain pivot, "
    "so bucket and current-card comparisons were skipped."
)
SCREENSHOT_ASSESSMENT_NORMAL = "normal"
SCREENSHOT_ASSESSMENT_BLOCKED = "blocked"
SCREENSHOT_ASSESSMENT_CHALLENGE = "challenge"
SCREENSHOT_ASSESSMENT_ERROR_PAGE = "error_page"
DOMAIN_PIVOT_DUPLICATE_EXPANSION_UNAVAILABLE_MESSAGE = (
    "Domain pivot unavailable right now due to upstream Facebook response instability while expanding grouped "
    "duplicates. Try the domain pivot again, rerun the search, or use /ads inspect on a landing URL from the "
    "current results."
)

DEFAULT_DOC_IDS = {
    "search": "25987067537594875",
    "details": "34519519914328678",
    "collation": "26122490030717516",
    "aggregate": "10003638746366624",
    "filter_context": "29650582277919185",
}

SUMMARY_PROMPT_TEXT = 'Want the next 10? Reply "next 10" to this message.'
GROUP_ACTION_HINT_LABEL = "Live path:"
REFERENCE_SEARCH_PATH = "/search"
ENABLED_ENV_VALUES = {"1", "true", "yes", "on"}
SUPPORTED_META_PROXY_SCHEMES = {"http", "https"}
META_REQUEST_KINDS = {
    "bootstrap_html",
    "bootstrap_verify",
    "bootstrap_followup",
    "graphql_search",
    "graphql_details",
    "graphql_collation",
    "graphql_aggregate",
    "graphql_filter_context",
}
PROXY_CURL_MAX_ATTEMPTS = 3
PROXY_CURL_RETRY_DELAY_SEC = 1.0
PROXY_CURL_RETRYABLE_EXIT_CODES = {5, 6, 7, 18, 28, 35, 47, 52, 55, 56}
COMMON_MULTI_LABEL_PUBLIC_SUFFIXES = {
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "co.nz",
    "com.au",
    "net.au",
    "org.au",
    "co.jp",
    "com.br",
    "com.mx",
    "co.za",
}
STACK_TECH_FAMILY_EXCLUDES = {"Cloudflare"}
STACK_SIGNAL_SHORT_LABELS = {
    "Google Analytics": "GA",
    "Google Tag Manager": "GTM",
}


class AdsRuntimeError(RuntimeError):
    """Base class for deterministic runtime failures."""


class AcquisitionError(AdsRuntimeError):
    """Raised when the unofficial GraphQL path fails or drifts."""


class ValidationError(AdsRuntimeError):
    """Raised when user or tool input is invalid."""


def now_utc() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def now_iso() -> str:
    return now_utc().isoformat()


def normalize_chat_id(value: Any) -> str:
    raw = str(value or "").strip()
    if raw.lower().startswith("telegram:"):
        raw = raw.split(":", 1)[1].strip()
    if raw.lower().startswith("group:"):
        raw = raw.split(":", 1)[1].strip()
    return raw


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def maybe_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    text = safe_text(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def ensure_date(value: Optional[str], field_name: str) -> Optional[str]:
    if value in (None, ""):
        return None
    if not DATE_RE.fullmatch(value):
        raise ValidationError(f"{field_name} must use YYYY-MM-DD format")
    return value


def normalize_string(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def normalize_search_relevance_text(value: Optional[str]) -> str:
    text = safe_text(value).lower()
    # Keep apostrophe-joined fragments together so contractions like "j'ai"
    # normalize to "jai" instead of manufacturing a standalone "ai" token.
    text = re.sub(r"(?<=[a-z0-9])[\'\u2019](?=[a-z0-9])", "", text)
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def graphql_non_json_error_detail(raw_text: str, exc: json.JSONDecodeError) -> str:
    stripped = raw_text.lstrip()
    if not stripped:
        return "returned empty response body instead of JSON"
    if stripped.startswith("<"):
        return "returned HTML/non-JSON response instead of JSON"
    return f"returned malformed non-JSON response instead of JSON: {exc}"


def is_collation_query_failure(exc: Exception) -> bool:
    return "AdLibraryV3AdCollationDetailsQuery" in str(exc).strip()


def current_date_utc() -> dt.date:
    return now_utc().date()


def resolve_relative_date_range(
    *,
    lookback_days: int,
    end_offset_days: int = 0,
    anchor_date: Optional[dt.date] = None,
) -> Tuple[str, str]:
    if lookback_days <= 0:
        raise ValidationError(f"lookback_days must be positive, got {lookback_days}")
    if end_offset_days < 0:
        raise ValidationError(f"end_offset_days must be zero or positive, got {end_offset_days}")
    end_date = (anchor_date or current_date_utc()) - dt.timedelta(days=end_offset_days)
    start_date = end_date - dt.timedelta(days=lookback_days - 1)
    return start_date.isoformat(), end_date.isoformat()


def default_command_date_range() -> Tuple[str, str]:
    return resolve_relative_date_range(lookback_days=DEFAULT_COMMAND_LOOKBACK_DAYS)


def strip_conversational_search_prefix(text: str) -> Tuple[str, bool]:
    cleaned = re.sub(r"\s+", " ", safe_text(text)).strip()
    if not cleaned:
        return "", False
    prefix_patterns = [
        r"^(?:show(?:\s+me)?|find|search)\s+ads?\s+for\s+",
        r"^(?:show(?:\s+me)?|find|search)\s+for\s+",
        r"^(?:show(?:\s+me)?|find|search)\s+",
        r"^ads?\s+for\s+",
    ]
    for pattern in prefix_patterns:
        updated = re.sub(pattern, "", cleaned, count=1, flags=re.IGNORECASE).strip()
        if updated != cleaned:
            return updated, True
    return cleaned, False


def extract_conversational_relative_dates(text: str) -> Tuple[str, Optional[str], Optional[str]]:
    cleaned = re.sub(r"\s+", " ", safe_text(text)).strip()
    if not cleaned:
        return "", None, None

    days_match = re.search(r"\b(?:for\s+)?(?:the\s+)?(?:past|last)\s+(\d+)\s+days?\b", cleaned, flags=re.IGNORECASE)
    if days_match:
        lookback_days = int(days_match.group(1))
        date_from, date_to = resolve_relative_date_range(lookback_days=lookback_days)
        keyword = (cleaned[: days_match.start()] + " " + cleaned[days_match.end() :]).strip(" ,.;:-")
        return re.sub(r"\s+", " ", keyword).strip(), date_from, date_to

    today_match = re.search(r"\btoday\b", cleaned, flags=re.IGNORECASE)
    if today_match:
        date_from, date_to = resolve_relative_date_range(lookback_days=1)
        keyword = (cleaned[: today_match.start()] + " " + cleaned[today_match.end() :]).strip(" ,.;:-")
        return re.sub(r"\s+", " ", keyword).strip(), date_from, date_to

    yesterday_match = re.search(r"\byesterday\b", cleaned, flags=re.IGNORECASE)
    if yesterday_match:
        date_from, date_to = resolve_relative_date_range(lookback_days=1, end_offset_days=1)
        keyword = (cleaned[: yesterday_match.start()] + " " + cleaned[yesterday_match.end() :]).strip(" ,.;:-")
        return re.sub(r"\s+", " ", keyword).strip(), date_from, date_to

    return cleaned, None, None


def normalize_conversational_search_keyword(
    text: str,
    *,
    allow_relative_dates: bool,
) -> Tuple[str, Optional[str], Optional[str]]:
    cleaned = re.sub(r"\s+", " ", safe_text(text)).strip()
    if not cleaned:
        return "", None, None

    stripped, stripped_prefix = strip_conversational_search_prefix(cleaned)
    if allow_relative_dates:
        stripped, date_from, date_to = extract_conversational_relative_dates(stripped)
    else:
        date_from, date_to = None, None

    if stripped_prefix and re.search(r"\bads?\b$", stripped, flags=re.IGNORECASE):
        stripped = re.sub(r"\bads?\b$", "", stripped, flags=re.IGNORECASE).strip(" ,.;:-")

    stripped = stripped.strip(" \"'.,;:-")
    stripped = re.sub(r"\s+", " ", stripped).strip()
    return stripped, date_from, date_to


def normalize_session_owner(value: Any) -> Optional[str]:
    raw = safe_text(value).strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^A-Za-z0-9._:-]+", "-", raw).strip("-")
    return cleaned or None


def unwrap_meta_redirect(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    parsed = urllib.parse.urlparse(url)
    if parsed.netloc not in {"l.facebook.com", "lm.facebook.com"}:
        return url
    qs = urllib.parse.parse_qs(parsed.query)
    unwrapped = qs.get("u", [])
    return unwrapped[0] if unwrapped else url


def extract_domain(url: Optional[str]) -> Optional[str]:
    target = unwrap_meta_redirect(url)
    if not target:
        return None
    parsed = urllib.parse.urlparse(target)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


def normalize_domain_text(value: Any) -> Optional[str]:
    raw = safe_text(value).strip().lower()
    if not raw:
        return None
    candidate = raw
    if "://" not in candidate:
        candidate = "https://" + candidate.lstrip("/")
    domain = extract_domain(candidate) or None
    if not domain or not DOMAIN_HOST_RE.fullmatch(domain):
        return None
    return domain


def ordered_unique_domains(values: Iterable[Optional[str]]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for value in values:
        domain = normalize_domain_text(value)
        if not domain:
            continue
        key = domain.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(domain)
    return normalized


def broaden_domain_query_candidates(value: str) -> List[str]:
    exact = normalize_domain_text(value)
    if not exact:
        raw = safe_text(value).strip()
        return [raw] if raw else []
    parts = exact.split(".")
    queries = [exact]
    while len(parts) > 2:
        parts = parts[1:]
        queries.append(".".join(parts))
    return queries


def normalize_url_list(values: Iterable[Optional[str]]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for value in values:
        target = unwrap_meta_redirect(value)
        if not target or target in seen:
            continue
        seen.add(target)
        normalized.append(target)
    return normalized


def normalize_path_family(value: Optional[str]) -> Optional[str]:
    target = unwrap_meta_redirect(value)
    if not target:
        return None
    parsed = urllib.parse.urlparse(target)
    raw_segments = [segment for segment in parsed.path.split("/") if segment]
    if not raw_segments:
        return "root"
    first_segment = urllib.parse.unquote(raw_segments[0]).strip().lower()
    normalized = re.sub(r"[^a-z0-9._~-]+", "-", first_segment).strip("-")
    return normalized or "root"


TITLE_FAMILY_STOPWORDS = {
    "a",
    "an",
    "and",
    "by",
    "for",
    "from",
    "get",
    "in",
    "into",
    "new",
    "now",
    "of",
    "on",
    "the",
    "to",
    "with",
    "your",
}

SEARCH_RELEVANCE_STOPWORDS = TITLE_FAMILY_STOPWORDS | {
    "ad",
    "ads",
    "find",
    "last",
    "me",
    "past",
    "search",
    "show",
    "today",
    "yesterday",
}
SEARCH_RELEVANCE_GENERIC_TOKENS = {
    "class",
    "classes",
    "content",
    "course",
    "courses",
    "guide",
    "learn",
    "learning",
    "read",
    "reading",
    "training",
    "workshop",
}
SEARCH_RELEVANCE_SHORT_TOKEN_ALLOWLIST = {"ai"}
SEARCH_RELEVANCE_TIER_ORDER = ("strong_intent", "generic_only", "no_overlap")
SEARCH_RELEVANCE_WEAK_MATCH_MIN_STRONG = 3
SEARCH_RELEVANCE_WEAK_MATCH_NOTE = (
    "Relevance note: only a few strong keyword matches were found in the current live first-page buffer, "
    "so lower cards may be weak or generic."
)


def ordered_unique_tokens(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        token = safe_text(value).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def search_relevance_query_tokens(value: Optional[str]) -> List[str]:
    tokens: List[str] = []
    for token in normalize_search_relevance_text(value).split():
        if token in SEARCH_RELEVANCE_STOPWORDS:
            continue
        if len(token) <= 2 and token not in SEARCH_RELEVANCE_SHORT_TOKEN_ALLOWLIST:
            continue
        tokens.append(token)
    return ordered_unique_tokens(tokens)


def search_tokens_match(left: str, right: str) -> bool:
    if left == right:
        return True
    for longer, shorter in ((left, right), (right, left)):
        if len(longer) <= len(shorter):
            continue
        if len(longer) > 4 and longer.endswith("ies") and longer[:-3] + "y" == shorter:
            return True
        if len(longer) > 3 and longer.endswith("s") and not longer.endswith("ss") and longer[:-1] == shorter:
            return True
    return False


def matched_query_tokens(query_tokens: Sequence[str], field_tokens: Sequence[str]) -> List[str]:
    normalized_field_tokens = [token for token in field_tokens if token]
    matches: List[str] = []
    for token in query_tokens:
        if any(search_tokens_match(token, field_token) for field_token in normalized_field_tokens):
            matches.append(token)
    return ordered_unique_tokens(matches)


def search_relevance_field_tokens(value: Optional[str]) -> List[str]:
    return [token for token in normalize_search_relevance_text(value).split() if token]


def search_relevance_url_path_text(values: Iterable[Optional[str]]) -> str:
    parts: List[str] = []
    for value in values:
        target = unwrap_meta_redirect(value)
        if not target:
            continue
        parsed = urllib.parse.urlparse(target)
        for segment in parsed.path.split("/"):
            decoded = urllib.parse.unquote(segment).strip()
            if decoded:
                parts.append(decoded)
    return " ".join(parts)


@dataclass(frozen=True)
class QueryRelevanceProfile:
    keyword: str
    normalized_keyword: str
    intent_tokens: Tuple[str, ...]
    generic_tokens: Tuple[str, ...]

    @property
    def usable_tokens(self) -> Tuple[str, ...]:
        return self.intent_tokens + self.generic_tokens

    def as_dict(self) -> Dict[str, Any]:
        return {
            "keyword": self.keyword,
            "normalized_keyword": self.normalized_keyword,
            "intent_tokens": list(self.intent_tokens),
            "generic_tokens": list(self.generic_tokens),
            "usable_tokens": list(self.usable_tokens),
        }


@dataclass(frozen=True)
class BufferedCandidateRelevance:
    tier: str
    score: int
    matched_intent_tokens: Tuple[str, ...]
    matched_generic_tokens: Tuple[str, ...]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "tier": self.tier,
            "score": self.score,
            "matched_intent_tokens": list(self.matched_intent_tokens),
            "matched_generic_tokens": list(self.matched_generic_tokens),
        }


def normalize_title_family(value: Optional[str]) -> Optional[str]:
    normalized = normalize_string(safe_text(value))
    if not normalized:
        return None
    raw_tokens = [token for token in normalized.split() if token]
    if not raw_tokens:
        return None
    filtered_tokens = [token for token in raw_tokens if token not in TITLE_FAMILY_STOPWORDS and not token.isdigit()]
    family_tokens = filtered_tokens or raw_tokens
    return " ".join(family_tokens[:4]).strip() or None


def truncate_text(value: Optional[str], limit: int) -> str:
    text = re.sub(r"\s+", " ", safe_text(value)).strip()
    if len(text) <= limit:
        return text
    if limit <= 1:
        return text[:limit]
    return text[: limit - 1].rstrip() + "…"


def text_excerpt(value: Optional[str], limit: int = 240) -> Optional[str]:
    excerpt = truncate_text(value, limit)
    return excerpt or None


def clean_creative_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", safe_text(value)).strip()


def creative_text_availability(value: Optional[str]) -> str:
    text = clean_creative_text(value)
    if not text:
        return CREATIVE_AVAILABILITY_UNAVAILABLE
    if UNRESOLVED_DYNAMIC_TEMPLATE_RE.search(text):
        return CREATIVE_AVAILABILITY_PLACEHOLDER
    return CREATIVE_AVAILABILITY_RESOLVED


def sanitize_creative_text(value: Optional[str]) -> Optional[str]:
    text = clean_creative_text(value)
    return text if creative_text_availability(text) == CREATIVE_AVAILABILITY_RESOLVED else None


def best_creative_candidate(candidates: Iterable[Optional[str]]) -> Tuple[Optional[str], str]:
    saw_placeholder = False
    for candidate in candidates:
        text = clean_creative_text(candidate)
        if not text:
            continue
        availability = creative_text_availability(text)
        if availability == CREATIVE_AVAILABILITY_RESOLVED:
            return text, availability
        if availability == CREATIVE_AVAILABILITY_PLACEHOLDER:
            saw_placeholder = True
    if saw_placeholder:
        return None, CREATIVE_AVAILABILITY_PLACEHOLDER
    return None, CREATIVE_AVAILABILITY_UNAVAILABLE


def registrable_domain(value: Optional[str]) -> Optional[str]:
    host = normalize_domain_text(value)
    if not host:
        return None
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    suffix = ".".join(parts[-2:])
    if suffix in COMMON_MULTI_LABEL_PUBLIC_SUFFIXES and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def command_safe_phrase(value: Optional[str]) -> str:
    text = re.sub(r"\s+", " ", safe_text(value)).strip()
    if not text:
        return ""
    return text.replace('"', "'")


def infer_media_kind(display_format: Optional[str], media_url: Optional[str]) -> Optional[str]:
    normalized_display = safe_text(display_format).strip().upper()
    if normalized_display in {"VIDEO", "REELS", "DCO_VIDEO"}:
        return "video"
    if normalized_display in {"IMAGE", "CAROUSEL", "DCO_IMAGE"}:
        return "photo"

    target = safe_text(media_url).strip().lower()
    if not target:
        return None
    parsed = urllib.parse.urlparse(target)
    path = parsed.path.lower()
    if path.endswith((".mp4", ".mov", ".webm", ".m4v")) or "/video" in parsed.netloc.lower():
        return "video"
    if path.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")) or "/scontent." in parsed.netloc.lower():
        return "photo"
    return None


def first_media_value(items: Iterable[Any], keys: Sequence[str]) -> Optional[str]:
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in keys:
            value = safe_text(item.get(key)).strip()
            if value:
                return value
    return None


def normalize_page_ids(values: Iterable[Any]) -> List[str]:
    normalized: List[str] = []
    for value in values:
        page_id = safe_text(value).strip()
        if not page_id or page_id in normalized:
            continue
        normalized.append(page_id)
    return normalized


def to_utc_date(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        timestamp = int(value)
        if timestamp > 10_000_000_000:
            timestamp //= 1000
        return dt.datetime.fromtimestamp(timestamp, tz=UTC).date().isoformat()
    text = safe_text(value).strip()
    if not text:
        return None
    if DATE_RE.fullmatch(text):
        return text
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).date().isoformat()


def date_distance_days(date_from: Optional[str], date_to: Optional[str]) -> Optional[int]:
    if not date_from:
        return None
    start = dt.date.fromisoformat(date_from)
    end = dt.date.fromisoformat(date_to) if date_to else now_utc().date()
    diff = (end - start).days + 1
    return diff if diff > 0 else 1


def sha1_digest(parts: Sequence[str]) -> str:
    payload = "||".join(parts).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def compact_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_json_object(value: Any) -> Optional[Dict[str, Any]]:
    raw = safe_text(value).strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def pretty_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)


def env_flag(name: str, default: str = "") -> bool:
    return os.environ.get(name, default).strip().lower() in ENABLED_ENV_VALUES


def parse_qs_body(body: str) -> Dict[str, str]:
    return {key: values[-1] for key, values in urllib.parse.parse_qs(body, keep_blank_values=True).items()}


def raw_record_storage_dict(record: "RawAdRecord") -> Dict[str, Any]:
    data = dataclasses.asdict(record)
    data.pop("raw_payload", None)
    return data


@dataclass
class AdsSearchParams:
    keyword: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    geo: str = "US"
    limit: int = 10
    page_ids: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class RawAdRecord:
    ad_archive_id: str
    page_id: str
    advertiser: str
    page_profile_url: Optional[str]
    creative_body: Optional[str]
    creative_title: Optional[str]
    landing_page_url: Optional[str]
    landing_domain: Optional[str]
    media_url: Optional[str]
    active_start_date: Optional[str]
    active_end_date: Optional[str]
    collation_id: Optional[str]
    collation_count: int
    cta_text: Optional[str]
    page_like_count: Optional[int]
    creative_body_availability: str = CREATIVE_AVAILABILITY_UNAVAILABLE
    creative_title_availability: str = CREATIVE_AVAILABILITY_UNAVAILABLE
    search_domain: Optional[str] = None
    search_domains: List[str] = field(default_factory=list)
    page_categories: List[str] = field(default_factory=list)
    display_format: Optional[str] = None
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.creative_body and self.creative_body_availability == CREATIVE_AVAILABILITY_UNAVAILABLE:
            self.creative_body_availability = creative_text_availability(self.creative_body)
        if self.creative_title and self.creative_title_availability == CREATIVE_AVAILABILITY_UNAVAILABLE:
            self.creative_title_availability = creative_text_availability(self.creative_title)

    @property
    def ad_library_url(self) -> str:
        return f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=US&id={self.ad_archive_id}"

    def as_dict(self) -> Dict[str, Any]:
        data = dataclasses.asdict(self)
        data.pop("raw_payload", None)
        data["ad_library_url"] = self.ad_library_url
        return data


@dataclass
class AdDetailsRecord:
    ad_archive_id: str
    page_id: str
    page_name: Optional[str]
    page_alias: Optional[str]
    page_likes: Optional[int]
    ig_followers: Optional[int]
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        data = dataclasses.asdict(self)
        data.pop("raw_payload", None)
        return data


@dataclass
class GroupedAdEntity:
    group_key: str
    representative_ad_archive_id: str
    advertiser: str
    page_id: str
    page_profile_url: Optional[str]
    ad_library_links: List[str]
    raw_ad_ids: List[str]
    landing_page_urls: List[str]
    landing_domain: Optional[str]
    landing_domains: List[str]
    creative_text: Optional[str]
    creative_titles: List[str]
    media_url: Optional[str]
    media_urls: List[str]
    active_start_date: Optional[str]
    active_end_date: Optional[str]
    days_active: Optional[int]
    duplicate_count: int
    creative_variants_count: int
    grouped_notes: Optional[str]
    page_likes: Optional[int]
    ig_followers: Optional[int]
    representative: Dict[str, Any]
    duplicates_present: bool
    raw_ads: List[Dict[str, Any]]
    creative_text_availability: str = CREATIVE_AVAILABILITY_UNAVAILABLE
    creative_titles_availability: str = CREATIVE_AVAILABILITY_UNAVAILABLE
    search_domain: Optional[str] = None
    search_domains: List[str] = field(default_factory=list)
    display_format: Optional[str] = None
    media_kind: Optional[str] = None
    media_outcome: Optional[str] = None
    text_fallback_used: bool = False
    native_media_sent: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class SearchSession:
    search_session_id: str
    chat_id: str
    user_id: str
    keyword: str
    date_from: Optional[str]
    date_to: Optional[str]
    geo: str
    page_ids: List[str]
    graphql_session_id: str
    next_cursor: Optional[str]
    total_count: Optional[int]
    total_count_text: Optional[str]
    exhausted: bool
    prompt_message_id: Optional[int]
    status: str
    comparison_json: Optional[str]
    pivot_json: Optional[str]
    last_error: Optional[str]
    created_at: str
    updated_at: str

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class PendingGroupCandidate:
    ordinal: int
    group_key: str
    representative: RawAdRecord


@dataclass
class BufferedSelectionCandidate:
    source: str
    ordinal: int
    group_key: str
    advertiser: str
    landing_domain: Optional[str]
    group: Optional["GroupedAdEntity"] = None
    pending: Optional[PendingGroupCandidate] = None


@dataclass
class AcquisitionDiagnostic:
    keyword: str
    geo: str
    date_from: Optional[str]
    date_to: Optional[str]
    bootstrap_ok: bool
    search_ok: bool
    details_ok: bool
    collation_ok: bool
    total_count_text: Optional[str]
    total_count: Optional[int]
    doc_ids: Dict[str, str]
    notes: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class ReferenceComparisonReport:
    search_session_id: str
    keyword: str
    date_from: Optional[str]
    date_to: Optional[str]
    geo: str
    verdict: str
    advertiser_overlap: float
    landing_domain_overlap: float
    creative_similarity: float
    creative_similarity_notes: str
    duplicate_handling_notes: str
    own_result_count: int
    reference_result_count: int
    own_advertisers: List[str]
    reference_advertisers: List[str]
    own_landing_domains: List[str]
    reference_landing_domains: List[str]
    notes: List[str] = field(default_factory=list)
    raw_reference_payload: Optional[Dict[str, Any]] = None

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class MetaProxyConfig:
    enabled: bool = False
    scheme: str = "http"
    host: str = ""
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None

    def proxy_url(self) -> str:
        scheme = (self.scheme or "http").strip().lower()
        if scheme not in SUPPORTED_META_PROXY_SCHEMES:
            if scheme.startswith("socks"):
                raise ValidationError(
                    "FACEBOOK_ADS_META_PROXY_SCHEME=socks5 is not supported in this build; use HTTP/HTTPS proxy first"
                )
            raise ValidationError(f"Unsupported FACEBOOK_ADS_META_PROXY_SCHEME: {scheme}")
        if not self.host or self.port is None:
            raise ValidationError(
                "FACEBOOK_ADS_META_PROXY_HOST and FACEBOOK_ADS_META_PROXY_PORT are required when proxying Meta requests"
            )
        auth = ""
        if self.username is not None:
            quoted_user = urllib.parse.quote(self.username, safe="")
            quoted_password = urllib.parse.quote(self.password or "", safe="")
            auth = f"{quoted_user}:{quoted_password}@"
        return f"{scheme}://{auth}{self.host}:{self.port}"

    def public_summary(self) -> Dict[str, Any]:
        scheme = (self.scheme or "http").strip().lower() or "http"
        return {
            "enabled": self.enabled,
            "configured": bool(self.host and self.port),
            "scheme": scheme if self.enabled else None,
            "has_auth": bool(self.username),
            "runtime_supported": scheme in SUPPORTED_META_PROXY_SCHEMES,
        }


@dataclass
class RequestTransportEvent:
    kind: str
    transport: str
    method: str
    host: str
    status: Optional[int]
    duration_ms: int
    request_body_bytes: int
    response_body_bytes: int
    error: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


class RequestTransportTracker:
    def __init__(self, proxy_config: MetaProxyConfig) -> None:
        self.proxy_config = proxy_config
        self.reset()

    def reset(self) -> None:
        self.events: List[RequestTransportEvent] = []
        self.started_at = now_iso()

    def record(
        self,
        *,
        kind: str,
        transport: str,
        method: str,
        url: str,
        status: Optional[int],
        duration_ms: int,
        request_body_bytes: int,
        response_body_bytes: int,
        error: Optional[str] = None,
    ) -> None:
        host = urllib.parse.urlparse(url).netloc.lower()
        self.events.append(
            RequestTransportEvent(
                kind=kind,
                transport=transport,
                method=method,
                host=host,
                status=status,
                duration_ms=duration_ms,
                request_body_bytes=request_body_bytes,
                response_body_bytes=response_body_bytes,
                error=error,
            )
        )

    def summary(self) -> Dict[str, Any]:
        events = [event.as_dict() for event in self.events]
        transport_totals: Dict[str, Dict[str, int]] = {}
        per_kind: Dict[str, Dict[str, Any]] = {}

        for event in self.events:
            totals = transport_totals.setdefault(
                event.transport,
                {
                    "request_count": 0,
                    "request_body_bytes": 0,
                    "response_body_bytes": 0,
                    "duration_ms": 0,
                },
            )
            totals["request_count"] += 1
            totals["request_body_bytes"] += event.request_body_bytes
            totals["response_body_bytes"] += event.response_body_bytes
            totals["duration_ms"] += event.duration_ms

            kind_entry = per_kind.setdefault(
                event.kind,
                {
                    "request_count": 0,
                    "transports": {},
                    "statuses": {},
                    "request_body_bytes": 0,
                    "response_body_bytes": 0,
                    "duration_ms": 0,
                    "errors": [],
                },
            )
            kind_entry["request_count"] += 1
            kind_entry["request_body_bytes"] += event.request_body_bytes
            kind_entry["response_body_bytes"] += event.response_body_bytes
            kind_entry["duration_ms"] += event.duration_ms
            kind_entry["transports"][event.transport] = kind_entry["transports"].get(event.transport, 0) + 1
            status_key = "none" if event.status is None else str(event.status)
            kind_entry["statuses"][status_key] = kind_entry["statuses"].get(status_key, 0) + 1
            if event.error and event.error not in kind_entry["errors"]:
                kind_entry["errors"].append(event.error)

        proxied_totals = transport_totals.get("proxy", {})
        direct_totals = transport_totals.get("direct", {})
        return {
            "proxy": self.proxy_config.public_summary(),
            "started_at": self.started_at,
            "request_count": len(self.events),
            "proxied_request_count": proxied_totals.get("request_count", 0),
            "direct_request_count": direct_totals.get("request_count", 0),
            "proxied_request_body_bytes": proxied_totals.get("request_body_bytes", 0),
            "proxied_response_body_bytes": proxied_totals.get("response_body_bytes", 0),
            "direct_request_body_bytes": direct_totals.get("request_body_bytes", 0),
            "direct_response_body_bytes": direct_totals.get("response_body_bytes", 0),
            "per_kind": per_kind,
            "events": events,
        }


class ProxyCurlClient:
    """Sentinel client for proxy-routed Meta requests executed via curl."""


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


class RuntimeConfig:
    def __init__(self) -> None:
        plugin_root = Path(os.environ.get("FACEBOOK_ADS_PLUGIN_ROOT", Path(__file__).resolve().parents[1]))
        self.plugin_root = plugin_root
        default_db = plugin_root / "data" / "facebook_ads_sessions.db"
        default_temp_media_root = Path(tempfile.gettempdir()) / TEMP_MEDIA_ROOT_DIRNAME
        self.session_db_path = Path(os.environ.get("FACEBOOK_ADS_SESSION_DB_PATH", str(default_db)))
        self.temp_media_root = Path(
            os.environ.get("FACEBOOK_ADS_TEMP_MEDIA_ROOT", str(default_temp_media_root))
        )
        self.temp_media_ttl_hours = int(os.environ.get("FACEBOOK_ADS_TEMP_MEDIA_TTL_HOURS", "12"))
        self.request_timeout_sec = int(os.environ.get("FACEBOOK_ADS_REQUEST_TIMEOUT_SEC", "30"))
        self.session_ttl_hours = int(os.environ.get("FACEBOOK_ADS_SESSION_TTL_HOURS", "12"))
        self.reference_base_url = os.environ.get("FACEBOOK_ADS_REFERENCE_BASE_URL", "").strip()
        self.reference_token = os.environ.get("FACEBOOK_ADS_REFERENCE_TOKEN", "").strip()
        self.reference_search_path = os.environ.get("FACEBOOK_ADS_REFERENCE_SEARCH_PATH", REFERENCE_SEARCH_PATH).strip() or REFERENCE_SEARCH_PATH
        self.doc_ids = {
            "search": os.environ.get("FACEBOOK_ADS_SEARCH_DOC_ID", DEFAULT_DOC_IDS["search"]).strip() or DEFAULT_DOC_IDS["search"],
            "details": os.environ.get("FACEBOOK_ADS_DETAILS_DOC_ID", DEFAULT_DOC_IDS["details"]).strip() or DEFAULT_DOC_IDS["details"],
            "collation": os.environ.get("FACEBOOK_ADS_COLLATION_DOC_ID", DEFAULT_DOC_IDS["collation"]).strip() or DEFAULT_DOC_IDS["collation"],
            "aggregate": os.environ.get("FACEBOOK_ADS_AGGREGATE_DOC_ID", DEFAULT_DOC_IDS["aggregate"]).strip() or DEFAULT_DOC_IDS["aggregate"],
            "filter_context": os.environ.get("FACEBOOK_ADS_FILTER_CONTEXT_DOC_ID", DEFAULT_DOC_IDS["filter_context"]).strip() or DEFAULT_DOC_IDS["filter_context"],
        }
        self.meta_proxy = MetaProxyConfig(
            enabled=env_flag("FACEBOOK_ADS_META_PROXY_ENABLED"),
            scheme=os.environ.get("FACEBOOK_ADS_META_PROXY_SCHEME", "http").strip() or "http",
            host=os.environ.get("FACEBOOK_ADS_META_PROXY_HOST", "").strip(),
            port=maybe_int(os.environ.get("FACEBOOK_ADS_META_PROXY_PORT")),
            username=os.environ.get("FACEBOOK_ADS_META_PROXY_USERNAME", "").strip() or None,
            password=os.environ.get("FACEBOOK_ADS_META_PROXY_PASSWORD", "").strip() or None,
        )


class FacebookAdsRuntime:
    def __init__(self, config: Optional[RuntimeConfig] = None) -> None:
        self.config = config or RuntimeConfig()
        self.transport_tracker = RequestTransportTracker(self.config.meta_proxy)
        self._direct_opener: Optional[urllib.request.OpenerDirector] = None
        self._direct_no_redirect_opener: Optional[urllib.request.OpenerDirector] = None
        self._proxy_client: Optional[ProxyCurlClient] = None
        self._proxy_cookie_jar_path: Optional[Path] = None
        self.config.session_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.temp_media_root.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.config.session_db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout = 30000")
        self.ensure_schema()
        self.cleanup_temp_media_root()

    # ----------------------- DB schema -----------------------

    def ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
              search_session_id TEXT PRIMARY KEY,
              chat_id TEXT NOT NULL,
              user_id TEXT NOT NULL,
              keyword TEXT NOT NULL,
              date_from TEXT,
              date_to TEXT,
              geo TEXT NOT NULL,
              page_ids_json TEXT,
              graphql_session_id TEXT NOT NULL,
              next_cursor TEXT,
              total_count INTEGER,
              total_count_text TEXT,
              exhausted INTEGER NOT NULL DEFAULT 0,
              prompt_message_id INTEGER,
              status TEXT NOT NULL DEFAULT 'active',
              comparison_json TEXT,
              pivot_json TEXT,
              last_error TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS session_groups (
              search_session_id TEXT NOT NULL,
              ordinal INTEGER NOT NULL,
              group_key TEXT NOT NULL,
              group_json TEXT NOT NULL,
              emitted INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (search_session_id, ordinal)
            );

            CREATE TABLE IF NOT EXISTS session_pending_candidates (
              search_session_id TEXT NOT NULL,
              ordinal INTEGER NOT NULL,
              group_key TEXT NOT NULL,
              representative_json TEXT NOT NULL,
              PRIMARY KEY (search_session_id, ordinal),
              UNIQUE (search_session_id, group_key)
            );

            CREATE TABLE IF NOT EXISTS session_group_keys (
              search_session_id TEXT NOT NULL,
              group_key TEXT NOT NULL,
              PRIMARY KEY (search_session_id, group_key)
            );

            CREATE TABLE IF NOT EXISTS session_message_bindings (
              chat_id TEXT NOT NULL,
              user_id TEXT NOT NULL,
              message_id INTEGER NOT NULL,
              search_session_id TEXT NOT NULL,
              group_key TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (chat_id, user_id, message_id)
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_chat_user_status
            ON sessions(chat_id, user_id, status, updated_at);

            CREATE INDEX IF NOT EXISTS idx_session_groups_emitted
            ON session_groups(search_session_id, emitted, ordinal);

            CREATE INDEX IF NOT EXISTS idx_session_pending_candidates_ordinal
            ON session_pending_candidates(search_session_id, ordinal);

            CREATE INDEX IF NOT EXISTS idx_session_message_bindings_session_group
            ON session_message_bindings(search_session_id, group_key);
            """
        )
        session_columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(sessions)")
        }
        if "page_ids_json" not in session_columns:
            self.conn.execute("ALTER TABLE sessions ADD COLUMN page_ids_json TEXT")
        if "pivot_json" not in session_columns:
            self.conn.execute("ALTER TABLE sessions ADD COLUMN pivot_json TEXT")
        self.conn.commit()

    def cleanup_expired_sessions(self) -> None:
        cutoff = (now_utc() - dt.timedelta(hours=self.config.session_ttl_hours)).isoformat()
        stale_ids = [
            row["search_session_id"]
            for row in self.conn.execute("SELECT search_session_id FROM sessions WHERE updated_at < ?", (cutoff,))
        ]
        if not stale_ids:
            return
        self.conn.executemany("DELETE FROM session_groups WHERE search_session_id = ?", [(item,) for item in stale_ids])
        self.conn.executemany(
            "DELETE FROM session_pending_candidates WHERE search_session_id = ?",
            [(item,) for item in stale_ids],
        )
        self.conn.executemany("DELETE FROM session_group_keys WHERE search_session_id = ?", [(item,) for item in stale_ids])
        self.conn.executemany("DELETE FROM session_message_bindings WHERE search_session_id = ?", [(item,) for item in stale_ids])
        self.conn.executemany("DELETE FROM sessions WHERE search_session_id = ?", [(item,) for item in stale_ids])
        self.conn.commit()

    def cleanup_temp_media_root(self) -> None:
        cutoff = now_utc() - dt.timedelta(hours=self.config.temp_media_ttl_hours)
        root = self.config.temp_media_root
        if not root.exists():
            return
        for child in root.iterdir():
            try:
                modified_at = dt.datetime.fromtimestamp(child.stat().st_mtime, tz=UTC)
            except FileNotFoundError:
                continue
            if modified_at >= cutoff:
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                try:
                    child.unlink()
                except FileNotFoundError:
                    pass

    def make_temp_media_dir(self, *, prefix: str) -> Path:
        self.cleanup_temp_media_root()
        return Path(tempfile.mkdtemp(prefix=prefix, dir=str(self.config.temp_media_root)))

    # ----------------------- HTTP helpers -----------------------

    def request_transport_for_kind(self, request_kind: str) -> str:
        if request_kind in META_REQUEST_KINDS and self.config.meta_proxy.enabled:
            return "proxy"
        return "direct"

    def _build_opener(
        self,
        *,
        transport: str,
        cookie_jar: Optional[http.cookiejar.CookieJar] = None,
    ) -> urllib.request.OpenerDirector:
        handlers: List[Any] = []
        if transport == "proxy":
            raise AcquisitionError("Proxy transport should use curl client, not urllib opener")
        else:
            # Keep non-Meta paths direct even if the host process has HTTP_PROXY env configured.
            handlers.append(urllib.request.ProxyHandler({}))
        if cookie_jar is not None:
            handlers.append(urllib.request.HTTPCookieProcessor(cookie_jar))
        return urllib.request.build_opener(*handlers)

    def proxy_cookie_jar_path(self) -> str:
        if self._proxy_cookie_jar_path is None:
            fd, path = tempfile.mkstemp(prefix="facebook-ads-proxy-cookies-", suffix=".txt")
            os.close(fd)
            self._proxy_cookie_jar_path = Path(path)
        return str(self._proxy_cookie_jar_path)

    def reset_request_clients(self) -> None:
        self._proxy_client = None
        if self._proxy_cookie_jar_path is not None:
            try:
                self._proxy_cookie_jar_path.unlink()
            except FileNotFoundError:
                pass
        self._proxy_cookie_jar_path = None

    def request_opener(
        self,
        *,
        request_kind: str,
        cookie_jar: Optional[http.cookiejar.CookieJar] = None,
    ) -> Any:
        transport = self.request_transport_for_kind(request_kind)
        if transport == "proxy":
            if self._proxy_client is None:
                self._proxy_client = ProxyCurlClient()
            return self._proxy_client
        if cookie_jar is not None:
            return self._build_opener(transport=transport, cookie_jar=cookie_jar)
        if self._direct_opener is None:
            self._direct_opener = self._build_opener(transport="direct")
        return self._direct_opener

    def direct_no_redirect_opener(self) -> urllib.request.OpenerDirector:
        if self._direct_no_redirect_opener is None:
            self._direct_no_redirect_opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({}),
                NoRedirectHandler(),
            )
        return self._direct_no_redirect_opener

    def request_transport_summary(self) -> Dict[str, Any]:
        return self.transport_tracker.summary()

    def _record_request_event(
        self,
        *,
        request_kind: str,
        transport: str,
        method: str,
        url: str,
        started: float,
        request_body_bytes: int,
        response_body_bytes: int,
        status: Optional[int],
        error: Optional[str] = None,
    ) -> None:
        self.transport_tracker.record(
            kind=request_kind,
            transport=transport,
            method=method,
            url=url,
            status=status,
            duration_ms=int((time.monotonic() - started) * 1000),
            request_body_bytes=request_body_bytes,
            response_body_bytes=response_body_bytes,
            error=error,
        )

    def _request_via_opener(
        self,
        opener: urllib.request.OpenerDirector,
        req: urllib.request.Request,
        *,
        method: str,
        url: str,
        request_kind: str,
        transport: str,
        request_body_bytes: int,
        started: float,
        timeout_sec: Optional[int] = None,
    ) -> Tuple[int, bytes, Dict[str, str]]:
        try:
            with opener.open(req, timeout=timeout_sec or self.config.request_timeout_sec) as resp:  # type: ignore[arg-type]
                response_body = resp.read()
                self._record_request_event(
                    request_kind=request_kind,
                    transport=transport,
                    method=method,
                    url=url,
                    started=started,
                    request_body_bytes=request_body_bytes,
                    response_body_bytes=len(response_body),
                    status=resp.status,
                )
                return resp.status, response_body, dict(resp.headers.items())
        except urllib.error.HTTPError as exc:
            response_body = exc.read()
            self._record_request_event(
                request_kind=request_kind,
                transport=transport,
                method=method,
                url=url,
                started=started,
                request_body_bytes=request_body_bytes,
                response_body_bytes=len(response_body),
                status=exc.code,
            )
            return exc.code, response_body, dict(exc.headers.items())
        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            message = str(getattr(exc, "reason", exc)).strip() or exc.__class__.__name__
            self._record_request_event(
                request_kind=request_kind,
                transport=transport,
                method=method,
                url=url,
                started=started,
                request_body_bytes=request_body_bytes,
                response_body_bytes=0,
                status=None,
                error=message,
            )
            raise AcquisitionError(f"{request_kind} request failed: {message}") from exc

    def _request_via_proxy_curl(
        self,
        *,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]],
        body: Optional[bytes],
        request_kind: str,
        transport: str,
        request_body_bytes: int,
        timeout_sec: Optional[int] = None,
    ) -> Tuple[int, bytes, Dict[str, str]]:
        if shutil.which("curl") is None:
            raise AcquisitionError("Proxy transport requires curl, but curl is not installed")
        proxy_parts = urllib.parse.urlparse(self.config.meta_proxy.proxy_url())
        proxy_endpoint = f"{proxy_parts.scheme}://{proxy_parts.hostname}:{proxy_parts.port}"
        proxy_auth = None
        if proxy_parts.username is not None:
            proxy_auth = f"{urllib.parse.unquote(proxy_parts.username)}:{urllib.parse.unquote(proxy_parts.password or '')}"

        last_message: Optional[str] = None
        for attempt in range(1, PROXY_CURL_MAX_ATTEMPTS + 1):
            attempt_started = time.monotonic()
            body_path = Path(tempfile.mkstemp(prefix="facebook-ads-proxy-body-", suffix=".bin")[1])
            cmd = [
                "curl",
                "-sS",
                "--http1.1",
                "--location",
                "--max-time",
                str(timeout_sec or self.config.request_timeout_sec),
                "--insecure",
                "--proxy",
                proxy_endpoint,
                "--cookie",
                self.proxy_cookie_jar_path(),
                "--cookie-jar",
                self.proxy_cookie_jar_path(),
                "-o",
                str(body_path),
                "-w",
                "%{http_code}",
            ]
            if proxy_auth is not None:
                cmd.extend(["--proxy-user", proxy_auth])
            if proxy_parts.scheme.lower() == "https":
                cmd.append("--proxy-insecure")
            if method.upper() != "GET":
                cmd.extend(["-X", method.upper()])
            for key, value in (headers or {}).items():
                cmd.extend(["-H", f"{key}: {value}"])
            if body is not None:
                cmd.extend(["--data-binary", "@-"])
            cmd.append(url)
            try:
                completed = subprocess.run(
                    cmd,
                    input=body,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                response_body = body_path.read_bytes() if body_path.exists() else b""
            finally:
                try:
                    body_path.unlink()
                except FileNotFoundError:
                    pass
            status_text = completed.stdout.decode("utf-8", "replace").strip()
            status = int(status_text) if status_text.isdigit() else None
            if completed.returncode == 0:
                self._record_request_event(
                    request_kind=request_kind,
                    transport=transport,
                    method=method,
                    url=url,
                    started=attempt_started,
                    request_body_bytes=request_body_bytes,
                    response_body_bytes=len(response_body),
                    status=status,
                )
                return status or 0, response_body, {}

            message = completed.stderr.decode("utf-8", "replace").strip() or f"curl exited {completed.returncode}"
            last_message = message
            self._record_request_event(
                request_kind=request_kind,
                transport=transport,
                method=method,
                url=url,
                started=attempt_started,
                request_body_bytes=request_body_bytes,
                response_body_bytes=len(response_body),
                status=status,
                error=message,
            )
            if completed.returncode not in PROXY_CURL_RETRYABLE_EXIT_CODES or attempt >= PROXY_CURL_MAX_ATTEMPTS:
                break
            time.sleep(PROXY_CURL_RETRY_DELAY_SEC * (2 ** (attempt - 1)))
        raise AcquisitionError(f"{request_kind} request failed: {last_message or 'curl request failed'}")

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        request_kind: str = "generic",
        opener: Optional[urllib.request.OpenerDirector] = None,
        timeout_sec: Optional[int] = None,
    ) -> Tuple[int, bytes, Dict[str, str]]:
        transport = self.request_transport_for_kind(request_kind)
        req = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
        active_opener = opener or self.request_opener(request_kind=request_kind)
        request_body_bytes = len(body or b"")
        started = time.monotonic()
        if hasattr(active_opener, "open"):
            return self._request_via_opener(
                active_opener,
                req,
                method=method,
                url=url,
                request_kind=request_kind,
                transport=transport,
                request_body_bytes=request_body_bytes,
                started=started,
                timeout_sec=timeout_sec,
            )
        return self._request_via_proxy_curl(
            method=method,
            url=url,
            headers=headers,
            body=body,
            request_kind=request_kind,
            transport=transport,
            request_body_bytes=request_body_bytes,
            timeout_sec=timeout_sec,
        )

    def _graphql_post(self, friendly_name: str, doc_id: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": friendly_name,
            "server_timestamps": "true",
            "variables": compact_json(variables),
            "doc_id": doc_id,
        }
        request_kind = {
            "AdLibrarySearchPaginationQuery": "graphql_search",
            "AdLibraryV3AdDetailsQuery": "graphql_details",
            "AdLibraryV3AdCollationDetailsQuery": "graphql_collation",
            "AdLibraryV3AggregatePageContentQuery": "graphql_aggregate",
            "AdLibraryMobileFocusedStateProviderRefetchQuery": "graphql_filter_context",
        }.get(friendly_name, "graphql_generic")
        status, body, _headers = self._request(
            "POST",
            "https://www.facebook.com/api/graphql/",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0",
            },
            body=urllib.parse.urlencode(payload).encode("utf-8"),
            request_kind=request_kind,
        )
        text = body.decode("utf-8", "replace")
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError as exc:
            raise AcquisitionError(f"{friendly_name} {graphql_non_json_error_detail(text, exc)}") from exc
        decoded["_http_status"] = status
        decoded["_request"] = payload
        return decoded

    def _fetch_bootstrap_html(self, params: AdsSearchParams) -> Tuple[str, Optional[int], Optional[str]]:
        search_url = self.build_ads_library_search_url(params)
        jar = http.cookiejar.CookieJar()
        opener = self.request_opener(request_kind="bootstrap_html", cookie_jar=jar)
        headers = {"User-Agent": "Mozilla/5.0"}

        status, body, _ = self._request("GET", search_url, headers=headers, request_kind="bootstrap_html", opener=opener)
        html = body.decode("utf-8", "replace")
        if status != 403:
            return html, self.parse_bootstrap_count(html), None

        match = re.search(r'"(\\/__rd_verify_[^"?]+\?challenge=\d+)"', html) or re.search(
            r"'(/__rd_verify_[^'?]+\?challenge=\d+)'", html
        )
        if not match:
            return html, None, "Bootstrap challenge path not found"

        verify_path = match.group(1).replace("\\/", "/")
        verify_url = "https://www.facebook.com" + verify_path
        verify_status, _verify_body, _ = self._request(
            "POST",
            verify_url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0",
            },
            body=b"",
            request_kind="bootstrap_verify",
            opener=opener,
        )
        if verify_status != 200:
            return html, None, f"Challenge verify failed with HTTP {verify_status}"

        final_status, final_body, _ = self._request(
            "GET",
            search_url,
            headers=headers,
            request_kind="bootstrap_followup",
            opener=opener,
        )
        final_html = final_body.decode("utf-8", "replace")
        if final_status != 200:
            return final_html, None, f"Challenge follow-up GET failed with HTTP {final_status}"
        return final_html, self.parse_bootstrap_count(final_html), None

    # ----------------------- Search URL and parsing -----------------------

    def build_ads_library_search_url(self, params: AdsSearchParams) -> str:
        query: List[Tuple[str, str]] = [
            ("active_status", "all"),
            ("ad_type", "all"),
            ("country", params.geo),
            ("q", params.keyword),
            ("search_type", "keyword_unordered"),
        ]
        if params.date_from:
            query.append(("start_date[min]", params.date_from))
        if params.date_to:
            query.append(("start_date[max]", params.date_to))
        return "https://www.facebook.com/ads/library/?" + urllib.parse.urlencode(query)

    def parse_bootstrap_count(self, html: str) -> Optional[int]:
        match = re.search(r'search_results_connection\\?":\{[^}]*?"count":(\d+)', html)
        return int(match.group(1)) if match else None

    def total_count_text(self, count: Optional[int]) -> Optional[str]:
        if count is None:
            return None
        if count >= TOTAL_COUNT_SENTINEL:
            return ">50,000"
        return f"{count:,}"

    def build_search_variables(
        self,
        params: AdsSearchParams,
        *,
        graphql_session_id: str,
        cursor: Optional[str],
    ) -> Dict[str, Any]:
        normalized_page_ids = normalize_page_ids(params.page_ids)
        query_string = params.keyword
        if len(normalized_page_ids) == 1 and query_string == normalized_page_ids[0]:
            query_string = ""
        variables: Dict[str, Any] = {
            "activeStatus": "all",
            "adType": "ALL",
            "bylines": [],
            "collationToken": None,
            "contentLanguages": [],
            "countries": [params.geo],
            "cursor": cursor,
            "excludedIDs": None,
            "first": 10,
            "isTargetedCountry": False,
            "location": None,
            "mediaType": "all",
            "multiCountryFilterMode": None,
            "pageIDs": normalized_page_ids,
            "potentialReachInput": None,
            "publisherPlatforms": [],
            "queryString": query_string,
            "regions": None,
            "searchType": "keyword_unordered",
            "sessionID": graphql_session_id,
            "sortData": {
                "direction": "DESCENDING",
                "mode": "SORT_BY_TOTAL_IMPRESSIONS",
            },
            "source": None,
            "startDate": None,
            "v": "76c38e",
            "viewAllPageID": "0",
        }
        if params.date_from or params.date_to:
            variables["startDate"] = {
                "min": params.date_from,
                "max": params.date_to,
            }
        return variables

    def build_details_variables(self, ad_archive_id: str, page_id: str, graphql_session_id: str) -> Dict[str, Any]:
        return {
            "adArchiveID": ad_archive_id,
            "pageID": page_id,
            "country": "US",
            "sessionID": graphql_session_id,
            "source": None,
            "isAdNonPolitical": True,
            "isAdNotAAAEligible": False,
        }

    def build_collation_variables(
        self,
        collation_id: str,
        graphql_session_id: str,
        *,
        forward_cursor: Optional[str] = None,
        backward_cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "collationGroupID": str(collation_id),
            "forwardCursor": forward_cursor,
            "backwardCursor": backward_cursor,
            "activeStatus": "ALL",
            "adType": "ALL",
            "bylines": [],
            "countries": ["US"],
            "location": None,
            "potentialReach": [],
            "publisherPlatforms": [],
            "regions": [],
            "sessionID": graphql_session_id,
            "startDate": None,
        }

    def build_aggregate_variables(self, collation_id: str, graphql_session_id: str) -> Dict[str, Any]:
        return {
            "collationID": str(collation_id),
            "activeStatus": "ALL",
            "adType": "ALL",
            "bylines": [],
            "location": None,
            "potentialReach": [],
            "publisherPlatforms": [],
            "regions": [],
            "sessionID": graphql_session_id,
            "startDate": None,
        }

    def extract_media_url(self, snapshot: Dict[str, Any]) -> Optional[str]:
        cards = snapshot.get("cards") or []
        card_video = first_media_value(cards, ("video_hd_url", "video_sd_url", "watermarked_video_hd_url", "watermarked_video_sd_url"))
        if card_video:
            return card_video
        card_image = first_media_value(
            cards,
            (
                "original_image_url",
                "resized_image_url",
                "watermarked_resized_image_url",
                "video_preview_image_url",
                "image_url",
            ),
        )
        if card_image:
            return card_image
        videos = (snapshot.get("videos") or []) + (snapshot.get("extra_videos") or [])
        top_level_video = first_media_value(
            videos,
            (
                "video_hd_url",
                "video_sd_url",
                "watermarked_video_hd_url",
                "watermarked_video_sd_url",
                "video_preview_image_url",
            ),
        )
        if top_level_video:
            return top_level_video
        images = (snapshot.get("images") or []) + (snapshot.get("extra_images") or [])
        top_level_image = first_media_value(
            images,
            ("original_image_url", "resized_image_url", "watermarked_resized_image_url", "image_url"),
        )
        if top_level_image:
            return top_level_image
        return None

    def creative_body_candidates(self, snapshot: Dict[str, Any]) -> List[Optional[str]]:
        candidates: List[Optional[str]] = []
        body = snapshot.get("body")
        if isinstance(body, dict):
            candidates.append(body.get("text"))
        for key in ("caption", "link_description"):
            candidates.append(snapshot.get(key))
        cards = snapshot.get("cards") or []
        for card in cards:
            for key in ("body", "title"):
                candidates.append(card.get(key))
        return candidates

    def creative_title_candidates(self, snapshot: Dict[str, Any]) -> List[Optional[str]]:
        candidates: List[Optional[str]] = []
        candidates.append(snapshot.get("title"))
        cards = snapshot.get("cards") or []
        for card in cards:
            candidates.append(card.get("title"))
        return candidates

    def extract_creative_body(self, snapshot: Dict[str, Any]) -> Tuple[Optional[str], str]:
        return best_creative_candidate(self.creative_body_candidates(snapshot))

    def extract_creative_title(self, snapshot: Dict[str, Any]) -> Tuple[Optional[str], str]:
        return best_creative_candidate(self.creative_title_candidates(snapshot))

    def extract_landing_url(self, snapshot: Dict[str, Any]) -> Optional[str]:
        direct = safe_text(snapshot.get("link_url")).strip()
        if direct:
            return unwrap_meta_redirect(direct)
        cards = snapshot.get("cards") or []
        for card in cards:
            url = unwrap_meta_redirect(safe_text(card.get("link_url")).strip())
            if url:
                return url
        return None

    def extract_search_domains(self, snapshot: Dict[str, Any], landing_url: Optional[str]) -> List[str]:
        candidates: List[Optional[str]] = [snapshot.get("caption")]
        for card in snapshot.get("cards") or []:
            candidates.append(card.get("caption"))
        normalized = ordered_unique_domains(candidates)
        if normalized:
            return normalized
        landing_domain = extract_domain(landing_url)
        return [landing_domain] if landing_domain else []

    def normalize_search_record(self, raw: Dict[str, Any]) -> RawAdRecord:
        snapshot = raw.get("snapshot") or {}
        landing_url = self.extract_landing_url(snapshot)
        search_domains = self.extract_search_domains(snapshot, landing_url)
        creative_body, creative_body_availability = self.extract_creative_body(snapshot)
        creative_title, creative_title_availability = self.extract_creative_title(snapshot)
        return RawAdRecord(
            ad_archive_id=str(raw.get("ad_archive_id") or ""),
            page_id=str(raw.get("page_id") or ""),
            advertiser=safe_text(snapshot.get("page_name")).strip() or "Unknown advertiser",
            page_profile_url=safe_text(snapshot.get("page_profile_uri")).strip() or None,
            creative_body=creative_body,
            creative_body_availability=creative_body_availability,
            creative_title=creative_title,
            creative_title_availability=creative_title_availability,
            landing_page_url=landing_url,
            landing_domain=extract_domain(landing_url),
            search_domain=search_domains[0] if search_domains else None,
            search_domains=search_domains,
            media_url=self.extract_media_url(snapshot),
            active_start_date=to_utc_date(raw.get("start_date")),
            active_end_date=to_utc_date(raw.get("end_date")),
            collation_id=safe_text(raw.get("collation_id")).strip() or None,
            collation_count=maybe_int(raw.get("collation_count")) or 1,
            cta_text=safe_text(snapshot.get("cta_text")).strip() or None,
            page_like_count=maybe_int(snapshot.get("page_like_count")),
            page_categories=[safe_text(item).strip() for item in (snapshot.get("page_categories") or []) if safe_text(item).strip()],
            display_format=safe_text(snapshot.get("display_format")).strip() or None,
            raw_payload=raw,
        )

    def normalize_collation_card(self, raw: Dict[str, Any], collation_id: str, representative: RawAdRecord) -> RawAdRecord:
        snapshot = raw.get("snapshot") or {}
        landing_url = self.extract_landing_url(snapshot)
        search_domains = self.extract_search_domains(snapshot, landing_url)
        creative_body, creative_body_availability = self.extract_creative_body(snapshot)
        creative_title, creative_title_availability = self.extract_creative_title(snapshot)
        return RawAdRecord(
            ad_archive_id=str(raw.get("ad_archive_id") or ""),
            page_id=str(raw.get("page_id") or representative.page_id),
            advertiser=safe_text(snapshot.get("page_name")).strip() or representative.advertiser,
            page_profile_url=safe_text(snapshot.get("page_profile_uri")).strip() or representative.page_profile_url,
            creative_body=creative_body,
            creative_body_availability=creative_body_availability,
            creative_title=creative_title,
            creative_title_availability=creative_title_availability,
            landing_page_url=landing_url,
            landing_domain=extract_domain(landing_url),
            search_domain=search_domains[0] if search_domains else None,
            search_domains=search_domains,
            media_url=self.extract_media_url(snapshot),
            active_start_date=representative.active_start_date,
            active_end_date=representative.active_end_date,
            collation_id=collation_id,
            collation_count=representative.collation_count,
            cta_text=safe_text(snapshot.get("cta_text")).strip() or representative.cta_text,
            page_like_count=maybe_int(snapshot.get("page_like_count")) or representative.page_like_count,
            page_categories=[safe_text(item).strip() for item in (snapshot.get("page_categories") or []) if safe_text(item).strip()],
            display_format=safe_text(snapshot.get("display_format")).strip() or representative.display_format,
            raw_payload=raw,
        )

    def normalize_details_response(self, ad_archive_id: str, page_id: str, payload: Dict[str, Any]) -> AdDetailsRecord:
        details = (((payload.get("data") or {}).get("ad_library_main") or {}).get("ad_details") or {})
        advertiser = ((((details.get("advertiser") or {}).get("ad_library_page_info") or {}).get("page_info")) or {})
        return AdDetailsRecord(
            ad_archive_id=ad_archive_id,
            page_id=page_id,
            page_name=safe_text(details.get("page_name")).strip() or safe_text(advertiser.get("page_name")).strip() or None,
            page_alias=safe_text(advertiser.get("page_alias")).strip() or None,
            page_likes=maybe_int(advertiser.get("likes")),
            ig_followers=maybe_int(advertiser.get("ig_followers")),
            raw_payload=payload,
        )

    def search_page(
        self,
        params: AdsSearchParams,
        *,
        graphql_session_id: str,
        cursor: Optional[str],
    ) -> Tuple[List[RawAdRecord], Optional[str], bool, Dict[str, Any]]:
        variables = self.build_search_variables(params, graphql_session_id=graphql_session_id, cursor=cursor)
        payload = self._graphql_post("AdLibrarySearchPaginationQuery", self.config.doc_ids["search"], variables)
        if payload.get("errors"):
            raise AcquisitionError(f"AdLibrarySearchPaginationQuery error: {payload['errors'][0].get('message')}")
        conn = (((payload.get("data") or {}).get("ad_library_main") or {}).get("search_results_connection")) or {}
        edges = conn.get("edges") or []
        records: List[RawAdRecord] = []
        for edge in edges:
            node = edge.get("node") or {}
            collated_results = node.get("collated_results") or []
            for raw_record in collated_results:
                if not isinstance(raw_record, dict):
                    continue
                record = self.normalize_search_record(raw_record)
                if record.ad_archive_id:
                    records.append(record)
        page_info = conn.get("page_info") or {}
        return records, safe_text(page_info.get("end_cursor")).strip() or None, bool(page_info.get("has_next_page")), payload

    def get_ad_details_record(
        self, ad_archive_id: str, page_id: str, graphql_session_id: str
    ) -> Tuple[AdDetailsRecord, Dict[str, Any]]:
        variables = self.build_details_variables(ad_archive_id, page_id, graphql_session_id)
        payload = self._graphql_post("AdLibraryV3AdDetailsQuery", self.config.doc_ids["details"], variables)
        if payload.get("errors"):
            raise AcquisitionError(f"AdLibraryV3AdDetailsQuery error: {payload['errors'][0].get('message')}")
        return self.normalize_details_response(ad_archive_id, page_id, payload), payload

    def get_collation_records(
        self,
        record: RawAdRecord,
        graphql_session_id: str,
    ) -> Tuple[List[RawAdRecord], Dict[str, Any], Dict[str, Any]]:
        if record.collation_count <= 1:
            return [record], {}, {}
        if not record.collation_id:
            raise AcquisitionError(
                f"Grouped duplicate card requires collation_id for ad {record.ad_archive_id}, but the field is missing"
            )
        payload = self._graphql_post(
            "AdLibraryV3AdCollationDetailsQuery",
            self.config.doc_ids["collation"],
            self.build_collation_variables(record.collation_id, graphql_session_id),
        )
        if payload.get("errors"):
            raise AcquisitionError(f"AdLibraryV3AdCollationDetailsQuery error: {payload['errors'][0].get('message')}")
        cards = (
            ((((payload.get("data") or {}).get("ad_library_main") or {}).get("collation_results")) or {}).get("ad_cards")
            or []
        )
        normalized = [self.normalize_collation_card(card, record.collation_id, record) for card in cards if card.get("ad_archive_id")]
        if not normalized:
            raise AcquisitionError(
                f"AdLibraryV3AdCollationDetailsQuery returned no related ad cards for collation_id={record.collation_id}"
            )
        return normalized, payload, {}

    # ----------------------- Grouping -----------------------

    def creative_signature(self, record: RawAdRecord) -> str:
        landing_domain = extract_domain(record.landing_page_url) or record.landing_domain
        creative_body = record.creative_body if record.creative_body_availability == CREATIVE_AVAILABILITY_RESOLVED else ""
        creative_title = record.creative_title if record.creative_title_availability == CREATIVE_AVAILABILITY_RESOLVED else ""
        parts = [
            normalize_string(record.advertiser),
            normalize_string(creative_body),
            normalize_string(creative_title),
            normalize_string(landing_domain or ""),
            normalize_string(record.display_format or ""),
            normalize_string(record.media_url or ""),
        ]
        return sha1_digest(parts)

    def fallback_group_key(self, record: RawAdRecord) -> str:
        landing_domain = extract_domain(record.landing_page_url) or record.landing_domain
        creative_body = record.creative_body if record.creative_body_availability == CREATIVE_AVAILABILITY_RESOLVED else ""
        creative_title = record.creative_title if record.creative_title_availability == CREATIVE_AVAILABILITY_RESOLVED else ""
        parts = [
            normalize_string(record.page_id),
            normalize_string(record.advertiser),
            normalize_string(creative_body),
            normalize_string(creative_title),
            normalize_string(landing_domain or ""),
            normalize_string(record.media_url or ""),
        ]
        return "fallback:" + sha1_digest(parts)

    def group_key_for_record(self, record: RawAdRecord) -> str:
        return f"collation:{record.collation_id}" if record.collation_id else self.fallback_group_key(record)

    @staticmethod
    def record_primary_domain(record: RawAdRecord) -> Optional[str]:
        search_domains = ordered_unique_domains(list(record.search_domains) + [record.search_domain])
        if search_domains:
            return search_domains[0]
        return normalize_domain_text(extract_domain(record.landing_page_url) or record.landing_domain)

    def grouped_notes(self, raw_ads: Sequence[RawAdRecord]) -> Optional[str]:
        if not raw_ads:
            return None
        title_variants = {
            item.creative_title
            for item in raw_ads
            if item.creative_title and item.creative_title_availability == CREATIVE_AVAILABILITY_RESOLVED
        }
        domains = {extract_domain(item.landing_page_url) or item.landing_domain for item in raw_ads if (extract_domain(item.landing_page_url) or item.landing_domain)}
        notes: List[str] = []
        if len(title_variants) > 1:
            notes.append(f"{len(title_variants)} creative title variants")
        if len(domains) > 1:
            notes.append(f"{len(domains)} landing domains")
        if len(raw_ads) > 1 and not notes:
            notes.append("duplicate runs or near-duplicate variants detected")
        return "; ".join(notes) if notes else None

    @staticmethod
    def grouped_creative_text_value(
        representative: RawAdRecord,
        raw_ads: Sequence[RawAdRecord],
    ) -> Tuple[Optional[str], str]:
        preferred_order = [representative, *[item for item in raw_ads if item is not representative]]
        saw_placeholder = False
        for item in preferred_order:
            if item.creative_body and item.creative_body_availability == CREATIVE_AVAILABILITY_RESOLVED:
                return item.creative_body, CREATIVE_AVAILABILITY_RESOLVED
            if item.creative_body_availability == CREATIVE_AVAILABILITY_PLACEHOLDER:
                saw_placeholder = True
        for item in preferred_order:
            if item.creative_title and item.creative_title_availability == CREATIVE_AVAILABILITY_RESOLVED:
                return item.creative_title, CREATIVE_AVAILABILITY_RESOLVED
            if item.creative_title_availability == CREATIVE_AVAILABILITY_PLACEHOLDER:
                saw_placeholder = True
        if saw_placeholder:
            return None, CREATIVE_AVAILABILITY_PLACEHOLDER
        return None, CREATIVE_AVAILABILITY_UNAVAILABLE

    @staticmethod
    def grouped_creative_titles_availability(raw_ads: Sequence[RawAdRecord], creative_titles: Sequence[str]) -> str:
        if creative_titles:
            return CREATIVE_AVAILABILITY_RESOLVED
        if any(item.creative_title_availability == CREATIVE_AVAILABILITY_PLACEHOLDER for item in raw_ads):
            return CREATIVE_AVAILABILITY_PLACEHOLDER
        return CREATIVE_AVAILABILITY_UNAVAILABLE

    def build_grouped_entity(
        self,
        representative: RawAdRecord,
        raw_ads: Sequence[RawAdRecord],
        details: Optional[AdDetailsRecord],
    ) -> GroupedAdEntity:
        start_dates = [item.active_start_date for item in raw_ads if item.active_start_date]
        end_dates = [item.active_end_date for item in raw_ads if item.active_end_date]
        landing_urls = normalize_url_list(item.landing_page_url for item in raw_ads)
        landing_domains = sorted(
            {
                extract_domain(item.landing_page_url) or item.landing_domain
                for item in raw_ads
                if (extract_domain(item.landing_page_url) or item.landing_domain)
            }
        )
        search_domains = ordered_unique_domains(
            domain
            for item in raw_ads
            for domain in list(item.search_domains) + [item.search_domain]
        )
        media_urls = normalize_url_list(item.media_url for item in raw_ads)
        creative_titles = sorted(
            {
                item.creative_title
                for item in raw_ads
                if item.creative_title and item.creative_title_availability == CREATIVE_AVAILABILITY_RESOLVED
            }
        )
        creative_text, creative_text_availability = self.grouped_creative_text_value(representative, raw_ads)
        creative_titles_availability = self.grouped_creative_titles_availability(raw_ads, creative_titles)
        active_start = min(start_dates) if start_dates else representative.active_start_date
        active_end = max(end_dates) if end_dates else representative.active_end_date
        duplicate_count = max(len(raw_ads), representative.collation_count or 1)
        creative_variants = {self.creative_signature(item) for item in raw_ads}
        landing_domain = landing_domains[0] if len(landing_domains) == 1 else None
        search_domain = search_domains[0] if len(search_domains) == 1 else None
        display_format = representative.display_format
        if not display_format:
            for item in raw_ads:
                if item.display_format:
                    display_format = item.display_format
                    break
        media_url = media_urls[0] if media_urls else representative.media_url
        media_kind = infer_media_kind(display_format, media_url)
        return GroupedAdEntity(
            group_key=self.group_key_for_record(representative),
            representative_ad_archive_id=representative.ad_archive_id,
            advertiser=representative.advertiser,
            page_id=representative.page_id,
            page_profile_url=representative.page_profile_url,
            ad_library_links=[item.ad_library_url for item in raw_ads],
            raw_ad_ids=[item.ad_archive_id for item in raw_ads],
            landing_page_urls=landing_urls,
            landing_domain=landing_domain,
            landing_domains=landing_domains,
            search_domain=search_domain,
            search_domains=search_domains,
            creative_text=creative_text,
            creative_text_availability=creative_text_availability,
            creative_titles=creative_titles,
            creative_titles_availability=creative_titles_availability,
            display_format=display_format,
            media_url=media_url,
            media_urls=media_urls,
            media_kind=media_kind,
            active_start_date=active_start,
            active_end_date=active_end,
            days_active=date_distance_days(active_start, active_end),
            duplicate_count=duplicate_count,
            creative_variants_count=max(1, len(creative_variants)),
            grouped_notes=self.grouped_notes(raw_ads),
            page_likes=(details.page_likes if details else representative.page_like_count),
            ig_followers=(details.ig_followers if details else None),
            representative=representative.as_dict(),
            duplicates_present=duplicate_count > 1,
            raw_ads=[item.as_dict() for item in raw_ads],
        )

    # ----------------------- Session DB -----------------------

    def supersede_active_sessions(self, chat_id: str, user_id: str) -> None:
        self.conn.execute(
            "UPDATE sessions SET status = 'superseded', updated_at = ? WHERE chat_id = ? AND user_id = ? AND status = 'active'",
            (now_iso(), chat_id, user_id),
        )
        self.conn.commit()

    def create_session(self, chat_id: str, user_id: str, params: AdsSearchParams) -> SearchSession:
        self.supersede_active_sessions(chat_id, user_id)
        session = SearchSession(
            search_session_id=str(uuid.uuid4()),
            chat_id=chat_id,
            user_id=user_id,
            keyword=params.keyword,
            date_from=params.date_from,
            date_to=params.date_to,
            geo=params.geo,
            page_ids=normalize_page_ids(params.page_ids),
            graphql_session_id=str(uuid.uuid4()),
            next_cursor=None,
            total_count=None,
            total_count_text=None,
            exhausted=False,
            prompt_message_id=None,
            status="active",
            comparison_json=None,
            pivot_json=None,
            last_error=None,
            created_at=now_iso(),
            updated_at=now_iso(),
        )
        self.conn.execute(
            """
            INSERT INTO sessions(
              search_session_id, chat_id, user_id, keyword, date_from, date_to, geo, page_ids_json,
              graphql_session_id, next_cursor, total_count, total_count_text, exhausted,
              prompt_message_id, status, comparison_json, pivot_json, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session.search_session_id,
                session.chat_id,
                session.user_id,
                session.keyword,
                session.date_from,
                session.date_to,
                session.geo,
                compact_json(session.page_ids),
                session.graphql_session_id,
                session.next_cursor,
                session.total_count,
                session.total_count_text,
                1 if session.exhausted else 0,
                session.prompt_message_id,
                session.status,
                session.comparison_json,
                session.pivot_json,
                session.last_error,
                session.created_at,
                session.updated_at,
            ),
        )
        self.conn.commit()
        return session

    def current_session_for_chat_user(self, chat_id: str, user_id: str) -> SearchSession:
        row = self.conn.execute(
            """
            SELECT * FROM sessions
            WHERE chat_id = ? AND user_id = ?
            ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END ASC, updated_at DESC
            LIMIT 1
            """,
            (chat_id, user_id),
        ).fetchone()
        if not row:
            raise ValidationError('No current ads search session found. Run /ads "keyword" first.')
        return self.load_session(row["search_session_id"])

    def load_session(self, search_session_id: str) -> SearchSession:
        row = self.conn.execute("SELECT * FROM sessions WHERE search_session_id = ?", (search_session_id,)).fetchone()
        if not row:
            raise ValidationError(f"search_session_id not found: {search_session_id}")
        raw_page_ids = safe_text(row["page_ids_json"]).strip() if "page_ids_json" in row.keys() else ""
        try:
            page_ids = normalize_page_ids(json.loads(raw_page_ids)) if raw_page_ids else []
        except json.JSONDecodeError:
            page_ids = []
        return SearchSession(
            search_session_id=row["search_session_id"],
            chat_id=row["chat_id"],
            user_id=row["user_id"],
            keyword=row["keyword"],
            date_from=row["date_from"],
            date_to=row["date_to"],
            geo=row["geo"],
            page_ids=page_ids,
            graphql_session_id=row["graphql_session_id"],
            next_cursor=row["next_cursor"],
            total_count=row["total_count"],
            total_count_text=row["total_count_text"],
            exhausted=bool(row["exhausted"]),
            prompt_message_id=row["prompt_message_id"],
            status=row["status"],
            comparison_json=row["comparison_json"],
            pivot_json=row["pivot_json"] if "pivot_json" in row.keys() else None,
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def latest_active_session(self, chat_id: str, user_id: str) -> SearchSession:
        row = self.conn.execute(
            """
            SELECT * FROM sessions
            WHERE chat_id = ? AND user_id = ? AND status = 'active'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (chat_id, user_id),
        ).fetchone()
        if not row:
            raise ValidationError("No active ads search session found. Run /ads \"keyword\" first.")
        return self.load_session(row["search_session_id"])

    def session_for_prompt(self, chat_id: str, user_id: str, prompt_message_id: int) -> SearchSession:
        row = self.conn.execute(
            """
            SELECT * FROM sessions
            WHERE chat_id = ? AND user_id = ? AND prompt_message_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (chat_id, user_id, int(prompt_message_id)),
        ).fetchone()
        if not row:
            raise ValidationError("No active ads session is bound to this prompt reply.")
        session = self.load_session(row["search_session_id"])
        current = self.current_session_for_chat_user(chat_id, user_id)
        if session.search_session_id != current.search_session_id or session.status != "active":
            raise ValidationError("This next-10 prompt belongs to an older ads search. Start a new /ads search.")
        return session

    def update_session(
        self,
        search_session_id: str,
        *,
        next_cursor: Any = SESSION_UPDATE_UNSET,
        total_count: Any = SESSION_UPDATE_UNSET,
        total_count_text: Any = SESSION_UPDATE_UNSET,
        exhausted: Any = SESSION_UPDATE_UNSET,
        prompt_message_id: Any = SESSION_UPDATE_UNSET,
        status: Any = SESSION_UPDATE_UNSET,
        comparison_json: Any = SESSION_UPDATE_UNSET,
        pivot_json: Any = SESSION_UPDATE_UNSET,
        last_error: Any = SESSION_UPDATE_UNSET,
    ) -> None:
        current = self.load_session(search_session_id)
        self.conn.execute(
            """
            UPDATE sessions
            SET next_cursor = ?, total_count = ?, total_count_text = ?, exhausted = ?,
                prompt_message_id = ?, status = ?, comparison_json = ?, pivot_json = ?, last_error = ?, updated_at = ?
            WHERE search_session_id = ?
            """,
            (
                current.next_cursor if next_cursor is SESSION_UPDATE_UNSET else next_cursor,
                current.total_count if total_count is SESSION_UPDATE_UNSET else total_count,
                current.total_count_text if total_count_text is SESSION_UPDATE_UNSET else total_count_text,
                int(current.exhausted if exhausted is SESSION_UPDATE_UNSET else exhausted),
                current.prompt_message_id if prompt_message_id is SESSION_UPDATE_UNSET else prompt_message_id,
                current.status if status is SESSION_UPDATE_UNSET else status,
                current.comparison_json if comparison_json is SESSION_UPDATE_UNSET else comparison_json,
                current.pivot_json if pivot_json is SESSION_UPDATE_UNSET else pivot_json,
                current.last_error if last_error is SESSION_UPDATE_UNSET else last_error,
                now_iso(),
                search_session_id,
            ),
        )
        self.conn.commit()

    def append_group(self, search_session_id: str, group: GroupedAdEntity) -> None:
        existing = self.conn.execute(
            "SELECT 1 FROM session_groups WHERE search_session_id = ? AND group_key = ?",
            (search_session_id, group.group_key),
        ).fetchone()
        if existing:
            return
        self.remember_group_key(search_session_id, group.group_key)
        ordinal = self.conn.execute(
            "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM session_groups WHERE search_session_id = ?",
            (search_session_id,),
        ).fetchone()[0]
        self.conn.execute(
            "INSERT INTO session_groups(search_session_id, ordinal, group_key, group_json, emitted) VALUES (?, ?, ?, ?, 0)",
            (search_session_id, ordinal, group.group_key, compact_json(group.as_dict())),
        )
        self.conn.commit()

    def has_seen_group_key(self, search_session_id: str, group_key: str) -> bool:
        existing = self.conn.execute(
            "SELECT 1 FROM session_group_keys WHERE search_session_id = ? AND group_key = ?",
            (search_session_id, group_key),
        ).fetchone()
        return existing is not None

    def remember_group_key(self, search_session_id: str, group_key: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO session_group_keys(search_session_id, group_key) VALUES (?, ?)",
            (search_session_id, group_key),
        )

    def append_pending_candidate(self, search_session_id: str, record: RawAdRecord) -> bool:
        group_key = self.group_key_for_record(record)
        if self.has_seen_group_key(search_session_id, group_key):
            return False
        ordinal = self.conn.execute(
            "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM session_pending_candidates WHERE search_session_id = ?",
            (search_session_id,),
        ).fetchone()[0]
        self.remember_group_key(search_session_id, group_key)
        self.conn.execute(
            """
            INSERT INTO session_pending_candidates(search_session_id, ordinal, group_key, representative_json)
            VALUES (?, ?, ?, ?)
            """,
            (search_session_id, ordinal, group_key, compact_json(raw_record_storage_dict(record))),
        )
        self.conn.commit()
        return True

    def pending_candidates(self, search_session_id: str, limit: Optional[int] = None) -> List[PendingGroupCandidate]:
        query = """
            SELECT * FROM session_pending_candidates
            WHERE search_session_id = ?
            ORDER BY ordinal ASC
        """
        params: List[Any] = [search_session_id]
        if limit is not None:
            query += "\n            LIMIT ?"
            params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [self.pending_candidate_from_row(row) for row in rows]

    def pending_candidate_count(self, search_session_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS count FROM session_pending_candidates WHERE search_session_id = ?",
            (search_session_id,),
        ).fetchone()
        return int(row["count"] if row else 0)

    def available_candidate_count(self, search_session_id: str) -> int:
        return self.un_emitted_group_count(search_session_id) + self.pending_candidate_count(search_session_id)

    def pending_candidate_from_row(self, row: sqlite3.Row) -> PendingGroupCandidate:
        payload = json.loads(row["representative_json"])
        return PendingGroupCandidate(
            ordinal=row["ordinal"],
            group_key=row["group_key"],
            representative=RawAdRecord(**payload),
        )

    def has_pending_candidates(self, search_session_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM session_pending_candidates WHERE search_session_id = ? LIMIT 1",
            (search_session_id,),
        ).fetchone()
        return row is not None

    def delete_pending_candidate(self, search_session_id: str, group_key: str) -> None:
        self.conn.execute(
            "DELETE FROM session_pending_candidates WHERE search_session_id = ? AND group_key = ?",
            (search_session_id, group_key),
        )
        self.conn.commit()

    def un_emitted_groups(self, search_session_id: str, limit: int) -> List[GroupedAdEntity]:
        rows = self.conn.execute(
            """
            SELECT * FROM session_groups
            WHERE search_session_id = ? AND emitted = 0
            ORDER BY ordinal ASC
            LIMIT ?
            """,
            (search_session_id, limit),
        ).fetchall()
        return [self.group_from_row(row) for row in rows]

    def un_emitted_group_count(self, search_session_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS count FROM session_groups WHERE search_session_id = ? AND emitted = 0",
            (search_session_id,),
        ).fetchone()
        return int(row["count"] if row else 0)

    def emitted_group_count(self, search_session_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS count FROM session_groups WHERE search_session_id = ? AND emitted = 1",
            (search_session_id,),
        ).fetchone()
        return int(row["count"] if row else 0)

    def all_groups(self, search_session_id: str, *, emitted_only: bool = False, limit: Optional[int] = None) -> List[GroupedAdEntity]:
        query = "SELECT * FROM session_groups WHERE search_session_id = ?"
        params: List[Any] = [search_session_id]
        if emitted_only:
            query += " AND emitted = 1"
        query += " ORDER BY ordinal ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [self.group_from_row(row) for row in rows]

    def load_group(self, search_session_id: str, group_key: str) -> GroupedAdEntity:
        row = self.conn.execute(
            "SELECT * FROM session_groups WHERE search_session_id = ? AND group_key = ? LIMIT 1",
            (search_session_id, group_key),
        ).fetchone()
        if not row:
            raise ValidationError(f"group_key not found for session: {group_key}")
        return self.group_from_row(row)

    def group_from_row(self, row: sqlite3.Row) -> GroupedAdEntity:
        payload = json.loads(row["group_json"])
        return GroupedAdEntity(**payload)

    def mark_groups_emitted(self, search_session_id: str, group_keys: Sequence[str]) -> None:
        self.conn.executemany(
            "UPDATE session_groups SET emitted = 1 WHERE search_session_id = ? AND group_key = ?",
            [(search_session_id, item) for item in group_keys],
        )
        self.conn.execute(
            "UPDATE sessions SET updated_at = ?, prompt_message_id = NULL WHERE search_session_id = ?",
            (now_iso(), search_session_id),
        )
        self.conn.commit()

    def bind_session_prompt(self, search_session_id: str, prompt_message_id: int) -> None:
        self.conn.execute(
            "UPDATE sessions SET prompt_message_id = ?, updated_at = ? WHERE search_session_id = ?",
            (int(prompt_message_id), now_iso(), search_session_id),
        )
        self.conn.commit()

    def bind_group_message(self, search_session_id: str, group_key: str, message_id: int) -> None:
        session = self.load_session(search_session_id)
        self.load_group(search_session_id, group_key)
        self.conn.execute(
            """
            INSERT OR REPLACE INTO session_message_bindings(
              chat_id, user_id, message_id, search_session_id, group_key, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                session.chat_id,
                session.user_id,
                int(message_id),
                search_session_id,
                group_key,
                now_iso(),
            ),
        )
        self.conn.execute(
            "UPDATE sessions SET updated_at = ? WHERE search_session_id = ?",
            (now_iso(), search_session_id),
        )
        self.conn.commit()

    def bound_group_for_message(self, chat_id: str, user_id: str, message_id: int) -> Tuple[SearchSession, GroupedAdEntity]:
        row = self.conn.execute(
            """
            SELECT search_session_id, group_key
            FROM session_message_bindings
            WHERE chat_id = ? AND user_id = ? AND message_id = ?
            LIMIT 1
            """,
            (chat_id, user_id, int(message_id)),
        ).fetchone()
        if not row:
            raise ValidationError("No grouped ad card is bound to this reply.")
        session = self.load_session(row["search_session_id"])
        current = self.current_session_for_chat_user(chat_id, user_id)
        if session.search_session_id != current.search_session_id:
            raise ValidationError(
                "This grouped ad card belongs to an older ads search. Start a new /ads search or use a card from the current results."
            )
        group = self.load_group(row["search_session_id"], row["group_key"])
        return session, group

    def persist_session_error(self, search_session_id: str, exc: Exception) -> None:
        self.update_session(search_session_id, last_error=str(exc))

    def tool_session_scope(self, payload: Dict[str, Any]) -> Tuple[str, str]:
        chat_id = normalize_chat_id(payload.get("chat_id"))
        user_id = safe_text(payload.get("user_id")).strip()
        if chat_id or user_id:
            if not chat_id or not user_id:
                raise ValidationError("search_ads requires both chat_id and user_id when using explicit session scope")
            return chat_id, user_id

        session_owner = normalize_session_owner(payload.get("session_owner"))
        if not session_owner:
            raise ValidationError("search_ads requires chat_id + user_id or session_owner")
        scoped_owner = f"tool:{session_owner}"
        return scoped_owner, scoped_owner

    # ----------------------- Search execution -----------------------

    def maybe_total_count(self, params: AdsSearchParams) -> Tuple[Optional[int], Optional[str], Optional[str]]:
        if normalize_page_ids(params.page_ids) and not safe_text(params.keyword).strip():
            return None, None, "Bootstrap count unavailable for exact page-id pivots."
        html, count, error = self._fetch_bootstrap_html(params)
        _ = html
        return count, self.total_count_text(count), error

    def ensure_buffered_candidates(self, session: SearchSession, min_count: int) -> Tuple[SearchSession, Dict[str, Any]]:
        session = self.load_session(session.search_session_id)
        initial_batch = self.emitted_group_count(session.search_session_id) == 0
        buffer_goal = max(min_count, DIVERSITY_PREFETCH_MAX_CANDIDATES) if initial_batch else min_count
        pages_fetched = 0
        pending_candidates_fetched = 0

        def fetch_next_page() -> SearchSession:
            nonlocal pages_fetched, pending_candidates_fetched
            active_session = self.load_session(session.search_session_id)
            if active_session.exhausted:
                return active_session
            params = AdsSearchParams(
                keyword=active_session.keyword,
                date_from=active_session.date_from,
                date_to=active_session.date_to,
                geo=active_session.geo,
                limit=10,
                page_ids=active_session.page_ids,
            )
            records, next_cursor, has_next_page, _payload = self.search_page(
                params,
                graphql_session_id=active_session.graphql_session_id,
                cursor=active_session.next_cursor,
            )
            pages_fetched += 1
            if not records:
                self.update_session(active_session.search_session_id, next_cursor=next_cursor, exhausted=True)
                return self.load_session(active_session.search_session_id)
            for record in records:
                if self.append_pending_candidate(active_session.search_session_id, record):
                    pending_candidates_fetched += 1
            self.update_session(
                active_session.search_session_id,
                next_cursor=next_cursor,
                exhausted=not has_next_page,
            )
            return self.load_session(active_session.search_session_id)

        while True:
            session = self.load_session(session.search_session_id)
            pool_count = self.available_candidate_count(session.search_session_id)
            if pool_count >= buffer_goal or session.exhausted:
                break
            if initial_batch and pages_fetched >= DIVERSITY_PREFETCH_MAX_PAGES:
                break
            session = fetch_next_page()

        return session, {
            "initial_batch": initial_batch,
            "prefetched_search_pages": pages_fetched,
            "buffer_goal": buffer_goal,
            "buffered_group_count": self.un_emitted_group_count(session.search_session_id),
            "pending_candidate_count": self.pending_candidate_count(session.search_session_id),
            "pending_candidates_fetched": pending_candidates_fetched,
            "candidates_expanded_into_groups": 0,
            "unexpanded_candidates_buffered": self.pending_candidate_count(session.search_session_id),
        }

    @staticmethod
    def group_primary_domain(group: GroupedAdEntity) -> Optional[str]:
        domains = FacebookAdsRuntime.group_pivot_domains(group)
        return domains[0] if domains else None

    @staticmethod
    def group_suggested_pivot_domain(group: GroupedAdEntity) -> Optional[str]:
        domains = FacebookAdsRuntime.group_pivot_domains(group)
        return domains[0] if len(domains) == 1 else None

    @staticmethod
    def group_pivot_domains(group: GroupedAdEntity) -> List[str]:
        search_domains = ordered_unique_domains(list(group.search_domains) + [group.search_domain])
        if search_domains:
            return search_domains
        return ordered_unique_domains(list(group.landing_domains) + [group.landing_domain])

    @staticmethod
    def session_pivot_context(session: SearchSession) -> Optional[Dict[str, Any]]:
        return parse_json_object(session.pivot_json)

    @staticmethod
    def group_lp_cluster_signature(group: GroupedAdEntity) -> Optional[Dict[str, Any]]:
        landing_url = group.landing_page_urls[0] if group.landing_page_urls else None
        host = normalize_domain_text(
            extract_domain(landing_url)
            or group.landing_domain
            or (group.landing_domains[0] if group.landing_domains else None)
        )
        if not host:
            return None
        path_family = normalize_path_family(landing_url) if landing_url else None
        if path_family:
            key = f"{host}|{path_family}"
            label = f"{host} / {path_family}"
            cluster_mode = "host_path_family"
        else:
            key = f"{host}|host_only"
            label = f"{host} / host-only"
            cluster_mode = "host_only"
        return {
            "key": key,
            "label": label,
            "host": host,
            "path_family": path_family,
            "cluster_mode": cluster_mode,
        }

    @staticmethod
    def public_lp_cluster(cluster: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": cluster["key"],
            "label": cluster["label"],
            "host": cluster["host"],
            "path_family": cluster.get("path_family"),
            "cluster_mode": cluster.get("cluster_mode"),
            "count": cluster["count"],
            "position": cluster["position"],
        }

    @classmethod
    def group_title_family_signature(cls, group: GroupedAdEntity) -> Optional[Dict[str, Any]]:
        representative_title = safe_text((group.representative or {}).get("creative_title")).strip()
        creative_title = representative_title or (group.creative_titles[0] if group.creative_titles else "")
        title_family = normalize_title_family(creative_title)
        if title_family:
            return {
                "title_family": title_family,
                "source": "creative_title",
                "source_text": creative_title,
            }
        creative_text = safe_text(group.creative_text).strip()
        title_family = normalize_title_family(creative_text)
        if not title_family:
            return None
        return {
            "title_family": title_family,
            "source": "creative_text",
            "source_text": creative_text,
        }

    @classmethod
    def group_overlap_family_signature(cls, group: GroupedAdEntity) -> Optional[Dict[str, Any]]:
        lp_cluster = cls.group_lp_cluster_signature(group)
        title_family = cls.group_title_family_signature(group)
        if lp_cluster and title_family:
            return {
                "key": f"{lp_cluster['key']}|title:{title_family['title_family']}",
                "label": f"{lp_cluster['label']} + {title_family['title_family']}",
                "family_mode": "lp_title",
                "lp_cluster": {
                    "key": lp_cluster["key"],
                    "label": lp_cluster["label"],
                    "host": lp_cluster["host"],
                    "path_family": lp_cluster.get("path_family"),
                    "cluster_mode": lp_cluster.get("cluster_mode"),
                },
                "title_family": title_family["title_family"],
                "title_family_source": title_family["source"],
            }
        if lp_cluster:
            return {
                "key": f"{lp_cluster['key']}|title:unavailable",
                "label": f"{lp_cluster['label']} + no-title-family",
                "family_mode": "lp_only",
                "lp_cluster": {
                    "key": lp_cluster["key"],
                    "label": lp_cluster["label"],
                    "host": lp_cluster["host"],
                    "path_family": lp_cluster.get("path_family"),
                    "cluster_mode": lp_cluster.get("cluster_mode"),
                },
                "title_family": None,
                "title_family_source": None,
            }
        if title_family:
            return {
                "key": f"title-only|{title_family['title_family']}",
                "label": f"title-only / {title_family['title_family']}",
                "family_mode": "title_only",
                "lp_cluster": None,
                "title_family": title_family["title_family"],
                "title_family_source": title_family["source"],
            }
        return None

    @staticmethod
    def public_overlap_family(family: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": family["key"],
            "label": family["label"],
            "family_mode": family["family_mode"],
            "lp_cluster": family.get("lp_cluster"),
            "title_family": family.get("title_family"),
            "title_family_source": family.get("title_family_source"),
            "count": family["count"],
            "position": family["position"],
        }

    @classmethod
    def stack_signal_label(cls, value: str) -> str:
        return STACK_SIGNAL_SHORT_LABELS.get(value, value)

    @classmethod
    def stack_family_signature(
        cls,
        *,
        trackers: Sequence[str],
        technologies: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        tracker_values = sorted({safe_text(item).strip() for item in trackers if safe_text(item).strip()})
        technology_values = sorted(
            {
                safe_text(item).strip()
                for item in technologies
                if safe_text(item).strip() and safe_text(item).strip() not in STACK_TECH_FAMILY_EXCLUDES
            }
        )
        if not tracker_values and not technology_values:
            return None

        if tracker_values and technology_values:
            family_mode = "tracker_plus_tech"
            label = (
                " + ".join(cls.stack_signal_label(item) for item in tracker_values)
                + "; tech: "
                + " + ".join(cls.stack_signal_label(item) for item in technology_values)
            )
        elif tracker_values:
            family_mode = "tracker_only"
            label = " + ".join(cls.stack_signal_label(item) for item in tracker_values)
        else:
            family_mode = "tech_only"
            label = " + ".join(cls.stack_signal_label(item) for item in technology_values)

        tracker_key = ",".join(tracker_values) if tracker_values else "none"
        technology_key = ",".join(technology_values) if technology_values else "none"
        return {
            "key": f"trackers:{tracker_key}|tech:{technology_key}",
            "label": label,
            "family_mode": family_mode,
            "trackers": tracker_values,
            "technologies": technology_values,
        }

    @staticmethod
    def public_stack_family(family: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": family["key"],
            "label": family["label"],
            "family_mode": family["family_mode"],
            "trackers": list(family.get("trackers") or []),
            "technologies": list(family.get("technologies") or []),
            "count": family["count"],
            "position": family["position"],
        }

    @classmethod
    def delivery_family_signature(
        cls,
        *,
        final_url: Optional[str],
        final_host: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        host = normalize_domain_text(final_host or extract_domain(final_url))
        if not host:
            return None
        path_family = normalize_path_family(final_url) if final_url else None
        if path_family:
            return {
                "key": f"final:{host}|path:{path_family}",
                "label": f"{host} / {path_family}",
                "family_mode": "final_host_path_family",
                "final_host": host,
                "final_path_family": path_family,
            }
        return {
            "key": f"final:{host}|path:host_only",
            "label": f"{host} / host-only",
            "family_mode": "final_host_only",
            "final_host": host,
            "final_path_family": None,
        }

    @staticmethod
    def redirect_depth_class(hops: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        redirect_count = sum(
            1
            for hop in hops
            if hop.get("location") or hop.get("meta_refresh_location")
        )
        if redirect_count <= 0:
            return {
                "key": "direct",
                "label": "direct",
                "redirect_count": 0,
            }
        if redirect_count == 1:
            return {
                "key": "1_redirect",
                "label": "1 redirect",
                "redirect_count": 1,
            }
        if redirect_count == 2:
            return {
                "key": "2_redirects",
                "label": "2 redirects",
                "redirect_count": 2,
            }
        return {
            "key": "3_plus_redirects",
            "label": "3+ redirects",
            "redirect_count": redirect_count,
        }

    @classmethod
    def redirect_family_signature(
        cls,
        *,
        redirect_interpretation: Optional[str],
        hops: Sequence[Dict[str, Any]],
        delivery_family: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        interpretation = safe_text(redirect_interpretation).strip() or None
        if interpretation is None or not isinstance(delivery_family, dict):
            return None
        depth = cls.redirect_depth_class(hops)
        nested_delivery_family = {
            "key": delivery_family["key"],
            "label": delivery_family["label"],
            "family_mode": delivery_family["family_mode"],
            "final_host": delivery_family.get("final_host"),
            "final_path_family": delivery_family.get("final_path_family"),
        }
        return {
            "key": (
                f"redirect:{interpretation}|depth:{depth['key']}|delivery:{delivery_family['key']}"
            ),
            "label": f"{interpretation} ({depth['label']}) -> {delivery_family['label']}",
            "family_mode": "redirect_route_delivery_family",
            "redirect_interpretation": interpretation,
            "redirect_depth_class": depth["key"],
            "redirect_depth_label": depth["label"],
            "redirect_count": depth["redirect_count"],
            "delivery_family": nested_delivery_family,
            "final_host": delivery_family.get("final_host"),
            "final_path_family": delivery_family.get("final_path_family"),
        }

    @staticmethod
    def public_delivery_family(family: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": family["key"],
            "label": family["label"],
            "family_mode": family["family_mode"],
            "final_host": family.get("final_host"),
            "final_path_family": family.get("final_path_family"),
            "count": family["count"],
            "position": family["position"],
        }

    @staticmethod
    def public_redirect_family(family: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": family["key"],
            "label": family["label"],
            "family_mode": family["family_mode"],
            "redirect_interpretation": family.get("redirect_interpretation"),
            "redirect_depth_class": family.get("redirect_depth_class"),
            "redirect_depth_label": family.get("redirect_depth_label"),
            "redirect_count": family.get("redirect_count"),
            "delivery_family": family.get("delivery_family"),
            "final_host": family.get("final_host"),
            "final_path_family": family.get("final_path_family"),
            "count": family["count"],
            "position": family["position"],
        }

    @classmethod
    def group_bucket_target_signature(cls, group: GroupedAdEntity) -> Optional[Dict[str, Any]]:
        landing_url = group.landing_page_urls[0] if group.landing_page_urls else None
        compare_key = cls.normalize_inspect_compare_key(landing_url)
        if not landing_url or compare_key is None:
            return None
        _scheme, host, path = compare_key
        label = host if path == "/" else f"{host}{path}"
        return {
            "key": f"{host}{path}",
            "label": label,
            "landing_url": unwrap_meta_redirect(landing_url) or landing_url,
            "host": host,
            "path": path,
        }

    @classmethod
    def group_stack_target_signature(cls, group: GroupedAdEntity) -> Optional[Dict[str, Any]]:
        return cls.group_bucket_target_signature(group)

    @classmethod
    def sample_pivot_bucket_targets(
        cls,
        groups: Sequence[GroupedAdEntity],
        *,
        current_group_key: Optional[str] = None,
        sample_limit: int = STACK_HINT_SAMPLE_MAX_TARGETS,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        target_map: Dict[str, Dict[str, Any]] = {}
        missing_target_group_keys: List[str] = []

        for group in groups:
            signature = cls.group_bucket_target_signature(group)
            if not signature:
                missing_target_group_keys.append(group.group_key)
                continue
            target = target_map.setdefault(
                signature["key"],
                {
                    **signature,
                    "count": 0,
                    "group_keys": [],
                    "contains_current": False,
                },
            )
            target["count"] += 1
            target["group_keys"].append(group.group_key)
            if group.group_key == current_group_key:
                target["contains_current"] = True

        targets = sorted(
            target_map.values(),
            key=lambda item: (0 if item.get("contains_current") else 1, -item["count"], item["label"]),
        )
        sampled_targets = targets[:sample_limit]
        sampled_keys = {item["key"] for item in sampled_targets}
        unsampled_group_count = sum(item["count"] for item in targets if item["key"] not in sampled_keys)
        sampled_group_count = sum(item["count"] for item in sampled_targets)
        return sampled_targets, {
            "unique_target_count": len(targets),
            "sampled_target_count": len(sampled_targets),
            "sample_limit": sample_limit,
            "sampled_group_count": sampled_group_count,
            "unsampled_group_count": unsampled_group_count,
            "missing_target_group_count": len(missing_target_group_keys),
        }

    @classmethod
    def stack_sampling_targets(
        cls,
        groups: Sequence[GroupedAdEntity],
        *,
        current_group_key: Optional[str] = None,
        sample_limit: int = STACK_HINT_SAMPLE_MAX_TARGETS,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        return cls.sample_pivot_bucket_targets(
            groups,
            current_group_key=current_group_key,
            sample_limit=sample_limit,
        )

    def sample_pivot_bucket_delivery_reports(
        self,
        groups: Sequence[GroupedAdEntity],
        *,
        current_group_key: Optional[str] = None,
        sample_limit: int = STACK_HINT_SAMPLE_MAX_TARGETS,
    ) -> Dict[str, Any]:
        sampled_targets, sampling = self.sample_pivot_bucket_targets(
            groups,
            current_group_key=current_group_key,
            sample_limit=sample_limit,
        )
        target_reports: Dict[str, Dict[str, Any]] = {}
        for target in sampled_targets:
            target_reports[target["key"]] = self.inspect_delivery_target(
                target["landing_url"],
                timeout_sec=STACK_HINT_REQUEST_TIMEOUT_SEC,
            )
        return {
            "sample_group_count": len(groups),
            "sampled_targets": sampled_targets,
            "sampling": sampling,
            "target_reports": target_reports,
        }

    @classmethod
    def summarize_pivot_bucket_groups(
        cls,
        groups: Sequence[GroupedAdEntity],
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        sample_group_count = len(groups)
        cluster_map: Dict[str, Dict[str, Any]] = {}

        for group in groups:
            signature = cls.group_lp_cluster_signature(group)
            if not signature:
                continue
            cluster = cluster_map.setdefault(
                signature["key"],
                {
                    **signature,
                    "count": 0,
                    "position": "unavailable",
                    "group_keys": [],
                },
            )
            cluster["count"] += 1
            cluster["group_keys"].append(group.group_key)

        clusters = sorted(cluster_map.values(), key=lambda item: (-item["count"], item["label"]))
        analyzed_group_count = sum(item["count"] for item in clusters)
        if not clusters:
            return (
                {
                    "sample_group_count": sample_group_count,
                    "analyzed_group_count": 0,
                    "cluster_count": 0,
                    "dominance_mode": "unavailable",
                    "dominant_cluster": None,
                    "secondary_clusters": [],
                    "outlier_count": 0,
                    "summary_text": (
                        "unavailable - shown pivot bucket sample had no landing URLs or hosts to cluster"
                    ),
                    "clusters": [],
                },
                {},
            )

        max_count = clusters[0]["count"]
        top_clusters = [cluster for cluster in clusters if cluster["count"] == max_count]
        dominance_mode = "mixed_no_clear_dominant" if len(top_clusters) > 1 else "dominant"
        dominant_cluster = top_clusters[0] if len(top_clusters) == 1 else None
        secondary_clusters: List[Dict[str, Any]] = []
        outlier_count = 0
        group_context: Dict[str, Dict[str, Any]] = {}

        for cluster in clusters:
            if dominant_cluster is None:
                cluster["position"] = "mixed_no_clear_dominant"
            elif cluster["key"] == dominant_cluster["key"]:
                cluster["position"] = "dominant"
            elif cluster["count"] == 1:
                cluster["position"] = "outlier"
                outlier_count += 1
            else:
                cluster["position"] = "secondary"
                secondary_clusters.append(cluster)

            public_cluster = cls.public_lp_cluster(cluster)
            for group_key in cluster["group_keys"]:
                group_context[group_key] = {
                    "position": cluster["position"],
                    "cluster": public_cluster,
                }

        public_clusters = [cls.public_lp_cluster(cluster) for cluster in clusters]
        if dominant_cluster is None:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"{analyzed_group_count} analyzable for LP clustering, {len(clusters)} LP clusters. "
                f"No single dominant LP cluster; top clusters are tied at {max_count} groups. "
                f"Singleton outliers: {outlier_count}."
            )
        else:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"{analyzed_group_count} analyzable for LP clustering, {len(clusters)} LP clusters. "
                f"Dominant LP cluster: {dominant_cluster['label']} ({dominant_cluster['count']} groups). "
                f"Other recurring clusters: {len(secondary_clusters)}. "
                f"Singleton outliers: {outlier_count}."
            )

        return (
            {
                "sample_group_count": sample_group_count,
                "analyzed_group_count": analyzed_group_count,
                "cluster_count": len(clusters),
                "dominance_mode": dominance_mode,
                "dominant_cluster": cls.public_lp_cluster(dominant_cluster) if dominant_cluster else None,
                "secondary_clusters": [cls.public_lp_cluster(cluster) for cluster in secondary_clusters],
                "outlier_count": outlier_count,
                "summary_text": summary_text,
                "clusters": public_clusters,
            },
            group_context,
        )

    @classmethod
    def summarize_pivot_bucket_overlap(
        cls,
        groups: Sequence[GroupedAdEntity],
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        sample_group_count = len(groups)
        family_map: Dict[str, Dict[str, Any]] = {}

        for group in groups:
            signature = cls.group_overlap_family_signature(group)
            if not signature:
                continue
            family = family_map.setdefault(
                signature["key"],
                {
                    **signature,
                    "count": 0,
                    "position": "unavailable",
                    "group_keys": [],
                },
            )
            family["count"] += 1
            family["group_keys"].append(group.group_key)

        families = sorted(family_map.values(), key=lambda item: (-item["count"], item["label"]))
        analyzed_group_count = sum(item["count"] for item in families)
        if not families:
            return (
                {
                    "sample_group_count": sample_group_count,
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "outlier_count": 0,
                    "summary_text": (
                        "unavailable - shown pivot bucket sample had no landing/title signals for overlap hints"
                    ),
                    "families": [],
                },
                {},
            )

        max_count = families[0]["count"]
        top_families = [family for family in families if family["count"] == max_count]
        dominance_mode = "mixed_no_clear_dominant" if len(top_families) > 1 else "dominant"
        dominant_family = top_families[0] if len(top_families) == 1 else None
        secondary_families: List[Dict[str, Any]] = []
        outlier_count = 0
        group_context: Dict[str, Dict[str, Any]] = {}

        for family in families:
            if dominant_family is None:
                family["position"] = "mixed_no_clear_dominant"
            elif family["key"] == dominant_family["key"]:
                family["position"] = "dominant"
            elif family["count"] == 1:
                family["position"] = "outlier"
                outlier_count += 1
            else:
                family["position"] = "secondary"
                secondary_families.append(family)

            public_family = cls.public_overlap_family(family)
            for group_key in family["group_keys"]:
                group_context[group_key] = {
                    "position": family["position"],
                    "family": public_family,
                }

        public_families = [cls.public_overlap_family(family) for family in families]
        if dominant_family is None:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"{analyzed_group_count} analyzable for LP/title overlap, {len(families)} overlap families. "
                f"No single dominant overlap family; top families are tied at {max_count} groups. "
                f"Singleton outliers: {outlier_count}."
            )
        else:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"{analyzed_group_count} analyzable for LP/title overlap, {len(families)} overlap families. "
                f"Dominant overlap family: {dominant_family['label']} ({dominant_family['count']} groups). "
                f"Other recurring families: {len(secondary_families)}. "
                f"Singleton outliers: {outlier_count}."
            )

        return (
            {
                "sample_group_count": sample_group_count,
                "analyzed_group_count": analyzed_group_count,
                "family_count": len(families),
                "dominance_mode": dominance_mode,
                "dominant_family": cls.public_overlap_family(dominant_family) if dominant_family else None,
                "secondary_families": [cls.public_overlap_family(family) for family in secondary_families],
                "outlier_count": outlier_count,
                "summary_text": summary_text,
                "families": public_families,
            },
            group_context,
        )

    def summarize_pivot_bucket_stack(
        self,
        groups: Sequence[GroupedAdEntity],
        *,
        current_group_key: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        sample_group_count = len(groups)
        sampled_targets, sampling = self.sample_pivot_bucket_targets(
            groups,
            current_group_key=current_group_key,
            sample_limit=STACK_HINT_SAMPLE_MAX_TARGETS,
        )
        missing_target_group_count = sampling["missing_target_group_count"]
        unsampled_group_count = sampling["unsampled_group_count"]
        sampled_target_count = sampling["sampled_target_count"]
        unique_target_count = sampling["unique_target_count"]

        if sample_group_count == 0:
            return (
                {
                    "sample_group_count": 0,
                    "sampled_group_count": 0,
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": 0,
                    "unique_target_count": 0,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": 0,
                    "unavailable_breakdown": {
                        "missing_target_group_count": 0,
                        "unsampled_group_count": 0,
                        "failed_group_count": 0,
                        "no_signal_group_count": 0,
                    },
                    "summary_text": "unavailable - shown pivot bucket sample had no grouped ads for stack hints",
                    "families": [],
                },
                {},
            )

        if unique_target_count == 0:
            return (
                {
                    "sample_group_count": sample_group_count,
                    "sampled_group_count": 0,
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": 0,
                    "unique_target_count": 0,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": sample_group_count,
                    "unavailable_breakdown": {
                        "missing_target_group_count": sample_group_count,
                        "unsampled_group_count": 0,
                        "failed_group_count": 0,
                        "no_signal_group_count": 0,
                    },
                    "summary_text": "unavailable - shown pivot bucket sample had no landing URLs for stack hints",
                    "families": [],
                },
                {},
            )

        family_map: Dict[str, Dict[str, Any]] = {}
        group_context: Dict[str, Dict[str, Any]] = {}
        failed_group_count = 0
        no_signal_group_count = 0

        for group in groups:
            if self.group_bucket_target_signature(group) is None:
                group_context[group.group_key] = {
                    "position": "unavailable",
                    "family": None,
                    "issue": "missing_target",
                    "issue_note": "unavailable - current card had no landing URL for sampled stack hints",
                }

        sampled_target_keys = {item["key"] for item in sampled_targets}
        for target in sampled_targets:
            target_report = self.inspect_stack_target(
                target["landing_url"],
                timeout_sec=STACK_HINT_REQUEST_TIMEOUT_SEC,
            )
            family_signature = self.stack_family_signature(
                trackers=target_report.get("tracker_hints") or [],
                technologies=target_report.get("technology_hints") or [],
            )
            if family_signature is None:
                issue = "failed" if target_report.get("fetch_error") else "no_signal"
                issue_note = (
                    "unavailable - sampled stack fetch failed for the current card target"
                    if issue == "failed"
                    else "unavailable - sampled off-browser fetch found no useful tracker/tech hints for the current card target"
                )
                if issue == "failed":
                    failed_group_count += target["count"]
                else:
                    no_signal_group_count += target["count"]
                for group_key in target["group_keys"]:
                    group_context[group_key] = {
                        "position": "unavailable",
                        "family": None,
                        "issue": issue,
                        "issue_note": issue_note,
                    }
                continue

            family = family_map.setdefault(
                family_signature["key"],
                {
                    **family_signature,
                    "count": 0,
                    "position": "unavailable",
                    "group_keys": [],
                },
            )
            family["count"] += target["count"]
            family["group_keys"].extend(target["group_keys"])
            for group_key in target["group_keys"]:
                group_context[group_key] = {
                    "position": "unavailable",
                    "family": None,
                    "issue": None,
                    "issue_note": None,
                    "family_key": family["key"],
                }

        for group in groups:
            signature = self.group_bucket_target_signature(group)
            if not signature or signature["key"] in sampled_target_keys:
                continue
            group_context[group.group_key] = {
                "position": "unavailable",
                "family": None,
                "issue": "unsampled",
                "issue_note": "unavailable - current card target was not included in the bounded stack sample",
            }

        families = sorted(family_map.values(), key=lambda item: (-item["count"], item["label"]))
        analyzed_group_count = sum(item["count"] for item in families)
        unavailable_group_count = (
            sample_group_count - analyzed_group_count
        )

        if not families:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"stack hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"0 analyzable for stack overlap, 0 stack families. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )
            return (
                {
                    "sample_group_count": sample_group_count,
                    "sampled_group_count": sampling["sampled_group_count"],
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": sampled_target_count,
                    "unique_target_count": unique_target_count,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": unavailable_group_count,
                    "unavailable_breakdown": {
                        "missing_target_group_count": missing_target_group_count,
                        "unsampled_group_count": unsampled_group_count,
                        "failed_group_count": failed_group_count,
                        "no_signal_group_count": no_signal_group_count,
                    },
                    "summary_text": summary_text,
                    "families": [],
                },
                group_context,
            )

        max_count = families[0]["count"]
        top_families = [family for family in families if family["count"] == max_count]
        dominance_mode = "mixed_no_clear_dominant" if len(top_families) > 1 else "dominant"
        dominant_family = top_families[0] if len(top_families) == 1 else None
        secondary_families: List[Dict[str, Any]] = []

        for family in families:
            if dominant_family is None:
                family["position"] = "mixed_no_clear_dominant"
            elif family["key"] == dominant_family["key"]:
                family["position"] = "dominant"
            elif family["count"] == 1:
                family["position"] = "outlier"
            else:
                family["position"] = "secondary"
                secondary_families.append(family)

            public_family = self.public_stack_family(family)
            for group_key in family["group_keys"]:
                group_context[group_key]["position"] = family["position"]
                group_context[group_key]["family"] = public_family

        public_families = [self.public_stack_family(family) for family in families]
        if dominant_family is None:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"stack hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"{analyzed_group_count} analyzable for stack overlap, {len(families)} stack families. "
                f"No single dominant stack family; top families are tied at {max_count} groups. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )
        else:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"stack hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"{analyzed_group_count} analyzable for stack overlap, {len(families)} stack families. "
                f"Dominant stack family: {dominant_family['label']} ({dominant_family['count']} groups). "
                f"Other recurring families: {len(secondary_families)}. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )

        return (
            {
                "sample_group_count": sample_group_count,
                "sampled_group_count": sampling["sampled_group_count"],
                "analyzed_group_count": analyzed_group_count,
                "family_count": len(families),
                "sampled_target_count": sampled_target_count,
                "unique_target_count": unique_target_count,
                "dominance_mode": dominance_mode,
                "dominant_family": self.public_stack_family(dominant_family) if dominant_family else None,
                "secondary_families": [self.public_stack_family(family) for family in secondary_families],
                "unavailable_group_count": unavailable_group_count,
                "unavailable_breakdown": {
                    "missing_target_group_count": missing_target_group_count,
                    "unsampled_group_count": unsampled_group_count,
                    "failed_group_count": failed_group_count,
                    "no_signal_group_count": no_signal_group_count,
                },
                "summary_text": summary_text,
                "families": public_families,
            },
            group_context,
        )

    def summarize_pivot_bucket_delivery(
        self,
        groups: Sequence[GroupedAdEntity],
        *,
        current_group_key: Optional[str] = None,
        sampled_delivery_data: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        sampled_delivery_data = sampled_delivery_data or self.sample_pivot_bucket_delivery_reports(
            groups,
            current_group_key=current_group_key,
            sample_limit=STACK_HINT_SAMPLE_MAX_TARGETS,
        )
        sample_group_count = sampled_delivery_data["sample_group_count"]
        sampled_targets = sampled_delivery_data["sampled_targets"]
        sampling = sampled_delivery_data["sampling"]
        target_reports = sampled_delivery_data["target_reports"]
        missing_target_group_count = sampling["missing_target_group_count"]
        unsampled_group_count = sampling["unsampled_group_count"]
        sampled_target_count = sampling["sampled_target_count"]
        unique_target_count = sampling["unique_target_count"]

        if sample_group_count == 0:
            return (
                {
                    "sample_group_count": 0,
                    "sampled_group_count": 0,
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": 0,
                    "unique_target_count": 0,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": 0,
                    "unavailable_breakdown": {
                        "missing_target_group_count": 0,
                        "unsampled_group_count": 0,
                        "failed_group_count": 0,
                        "no_signal_group_count": 0,
                    },
                    "summary_text": "unavailable - shown pivot bucket sample had no grouped ads for delivery hints",
                    "families": [],
                },
                {},
            )

        if unique_target_count == 0:
            return (
                {
                    "sample_group_count": sample_group_count,
                    "sampled_group_count": 0,
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": 0,
                    "unique_target_count": 0,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": sample_group_count,
                    "unavailable_breakdown": {
                        "missing_target_group_count": sample_group_count,
                        "unsampled_group_count": 0,
                        "failed_group_count": 0,
                        "no_signal_group_count": 0,
                    },
                    "summary_text": "unavailable - shown pivot bucket sample had no landing URLs for delivery hints",
                    "families": [],
                },
                {},
            )

        family_map: Dict[str, Dict[str, Any]] = {}
        group_context: Dict[str, Dict[str, Any]] = {}
        failed_group_count = 0
        no_signal_group_count = 0

        for group in groups:
            if self.group_bucket_target_signature(group) is None:
                group_context[group.group_key] = {
                    "position": "unavailable",
                    "family": None,
                    "issue": "missing_target",
                    "issue_note": "unavailable - current card had no landing URL for sampled delivery hints",
                }

        sampled_target_keys = {item["key"] for item in sampled_targets}
        for target in sampled_targets:
            target_report = target_reports.get(target["key"], {})
            family_signature = target_report.get("delivery_family")
            if family_signature is None:
                issue = "failed" if target_report.get("fetch_error") else "no_signal"
                issue_note = (
                    "unavailable - sampled delivery fetch failed for the current card target"
                    if issue == "failed"
                    else "unavailable - sampled off-browser fetch did not resolve a useful final-destination family for the current card target"
                )
                if issue == "failed":
                    failed_group_count += target["count"]
                else:
                    no_signal_group_count += target["count"]
                for group_key in target["group_keys"]:
                    group_context[group_key] = {
                        "position": "unavailable",
                        "family": None,
                        "issue": issue,
                        "issue_note": issue_note,
                    }
                continue

            family = family_map.setdefault(
                family_signature["key"],
                {
                    **family_signature,
                    "count": 0,
                    "position": "unavailable",
                    "group_keys": [],
                },
            )
            family["count"] += target["count"]
            family["group_keys"].extend(target["group_keys"])
            for group_key in target["group_keys"]:
                group_context[group_key] = {
                    "position": "unavailable",
                    "family": None,
                    "issue": None,
                    "issue_note": None,
                    "family_key": family["key"],
                }

        for group in groups:
            signature = self.group_bucket_target_signature(group)
            if not signature or signature["key"] in sampled_target_keys:
                continue
            group_context[group.group_key] = {
                "position": "unavailable",
                "family": None,
                "issue": "unsampled",
                "issue_note": "unavailable - current card target was not included in the bounded delivery sample",
            }

        families = sorted(family_map.values(), key=lambda item: (-item["count"], item["label"]))
        analyzed_group_count = sum(item["count"] for item in families)
        unavailable_group_count = sample_group_count - analyzed_group_count

        if not families:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"delivery hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"0 analyzable for final-destination overlap, 0 delivery families. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )
            return (
                {
                    "sample_group_count": sample_group_count,
                    "sampled_group_count": sampling["sampled_group_count"],
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": sampled_target_count,
                    "unique_target_count": unique_target_count,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": unavailable_group_count,
                    "unavailable_breakdown": {
                        "missing_target_group_count": missing_target_group_count,
                        "unsampled_group_count": unsampled_group_count,
                        "failed_group_count": failed_group_count,
                        "no_signal_group_count": no_signal_group_count,
                    },
                    "summary_text": summary_text,
                    "families": [],
                },
                group_context,
            )

        max_count = families[0]["count"]
        top_families = [family for family in families if family["count"] == max_count]
        dominance_mode = "mixed_no_clear_dominant" if len(top_families) > 1 else "dominant"
        dominant_family = top_families[0] if len(top_families) == 1 else None
        secondary_families: List[Dict[str, Any]] = []

        for family in families:
            if dominant_family is None:
                family["position"] = "mixed_no_clear_dominant"
            elif family["key"] == dominant_family["key"]:
                family["position"] = "dominant"
            elif family["count"] == 1:
                family["position"] = "outlier"
            else:
                family["position"] = "secondary"
                secondary_families.append(family)

            public_family = self.public_delivery_family(family)
            for group_key in family["group_keys"]:
                group_context[group_key]["position"] = family["position"]
                group_context[group_key]["family"] = public_family

        public_families = [self.public_delivery_family(family) for family in families]
        if dominant_family is None:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"delivery hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"{analyzed_group_count} analyzable for final-destination overlap, {len(families)} delivery families. "
                f"No single dominant delivery family; top families are tied at {max_count} groups. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )
        else:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"delivery hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"{analyzed_group_count} analyzable for final-destination overlap, {len(families)} delivery families. "
                f"Dominant delivery family: {dominant_family['label']} ({dominant_family['count']} groups). "
                f"Other recurring families: {len(secondary_families)}. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )

        return (
            {
                "sample_group_count": sample_group_count,
                "sampled_group_count": sampling["sampled_group_count"],
                "analyzed_group_count": analyzed_group_count,
                "family_count": len(families),
                "sampled_target_count": sampled_target_count,
                "unique_target_count": unique_target_count,
                "dominance_mode": dominance_mode,
                "dominant_family": self.public_delivery_family(dominant_family) if dominant_family else None,
                "secondary_families": [self.public_delivery_family(family) for family in secondary_families],
                "unavailable_group_count": unavailable_group_count,
                "unavailable_breakdown": {
                    "missing_target_group_count": missing_target_group_count,
                    "unsampled_group_count": unsampled_group_count,
                    "failed_group_count": failed_group_count,
                    "no_signal_group_count": no_signal_group_count,
                },
                "summary_text": summary_text,
                "families": public_families,
            },
            group_context,
        )

    def summarize_pivot_bucket_redirect(
        self,
        groups: Sequence[GroupedAdEntity],
        *,
        current_group_key: Optional[str] = None,
        sampled_delivery_data: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        sampled_delivery_data = sampled_delivery_data or self.sample_pivot_bucket_delivery_reports(
            groups,
            current_group_key=current_group_key,
            sample_limit=STACK_HINT_SAMPLE_MAX_TARGETS,
        )
        sample_group_count = sampled_delivery_data["sample_group_count"]
        sampled_targets = sampled_delivery_data["sampled_targets"]
        sampling = sampled_delivery_data["sampling"]
        target_reports = sampled_delivery_data["target_reports"]
        missing_target_group_count = sampling["missing_target_group_count"]
        unsampled_group_count = sampling["unsampled_group_count"]
        sampled_target_count = sampling["sampled_target_count"]
        unique_target_count = sampling["unique_target_count"]

        if sample_group_count == 0:
            return (
                {
                    "sample_group_count": 0,
                    "sampled_group_count": 0,
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": 0,
                    "unique_target_count": 0,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": 0,
                    "unavailable_breakdown": {
                        "missing_target_group_count": 0,
                        "unsampled_group_count": 0,
                        "failed_group_count": 0,
                        "no_signal_group_count": 0,
                    },
                    "summary_text": "unavailable - shown pivot bucket sample had no grouped ads for redirect hints",
                    "families": [],
                },
                {},
            )

        if unique_target_count == 0:
            return (
                {
                    "sample_group_count": sample_group_count,
                    "sampled_group_count": 0,
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": 0,
                    "unique_target_count": 0,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": sample_group_count,
                    "unavailable_breakdown": {
                        "missing_target_group_count": sample_group_count,
                        "unsampled_group_count": 0,
                        "failed_group_count": 0,
                        "no_signal_group_count": 0,
                    },
                    "summary_text": "unavailable - shown pivot bucket sample had no landing URLs for redirect hints",
                    "families": [],
                },
                {},
            )

        family_map: Dict[str, Dict[str, Any]] = {}
        group_context: Dict[str, Dict[str, Any]] = {}
        failed_group_count = 0
        no_signal_group_count = 0

        for group in groups:
            if self.group_bucket_target_signature(group) is None:
                group_context[group.group_key] = {
                    "position": "unavailable",
                    "family": None,
                    "issue": "missing_target",
                    "issue_note": "unavailable - current card had no landing URL for sampled redirect hints",
                }

        sampled_target_keys = {item["key"] for item in sampled_targets}
        for target in sampled_targets:
            target_report = target_reports.get(target["key"], {})
            delivery_family = target_report.get("delivery_family")
            if delivery_family is None:
                delivery_family = self.delivery_family_signature(
                    final_url=target_report.get("final_url"),
                    final_host=target_report.get("final_host"),
                )
            family_signature = target_report.get("redirect_family") or self.redirect_family_signature(
                redirect_interpretation=target_report.get("redirect_interpretation"),
                hops=target_report.get("redirect_chain") or [],
                delivery_family=delivery_family,
            )
            if family_signature is None:
                issue = "failed" if target_report.get("fetch_error") else "no_signal"
                issue_note = (
                    "unavailable - sampled redirect fetch failed for the current card target"
                    if issue == "failed"
                    else "unavailable - sampled off-browser fetch did not resolve a useful redirect-route family for the current card target"
                )
                if issue == "failed":
                    failed_group_count += target["count"]
                else:
                    no_signal_group_count += target["count"]
                for group_key in target["group_keys"]:
                    group_context[group_key] = {
                        "position": "unavailable",
                        "family": None,
                        "issue": issue,
                        "issue_note": issue_note,
                    }
                continue

            family = family_map.setdefault(
                family_signature["key"],
                {
                    **family_signature,
                    "count": 0,
                    "position": "unavailable",
                    "group_keys": [],
                },
            )
            family["count"] += target["count"]
            family["group_keys"].extend(target["group_keys"])
            for group_key in target["group_keys"]:
                group_context[group_key] = {
                    "position": "unavailable",
                    "family": None,
                    "issue": None,
                    "issue_note": None,
                    "family_key": family["key"],
                }

        for group in groups:
            signature = self.group_bucket_target_signature(group)
            if not signature or signature["key"] in sampled_target_keys:
                continue
            group_context[group.group_key] = {
                "position": "unavailable",
                "family": None,
                "issue": "unsampled",
                "issue_note": "unavailable - current card target was not included in the bounded redirect sample",
            }

        families = sorted(family_map.values(), key=lambda item: (-item["count"], item["label"]))
        analyzed_group_count = sum(item["count"] for item in families)
        unavailable_group_count = sample_group_count - analyzed_group_count

        if not families:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"redirect hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"0 analyzable for redirect-route overlap, 0 redirect families. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )
            return (
                {
                    "sample_group_count": sample_group_count,
                    "sampled_group_count": sampling["sampled_group_count"],
                    "analyzed_group_count": 0,
                    "family_count": 0,
                    "sampled_target_count": sampled_target_count,
                    "unique_target_count": unique_target_count,
                    "dominance_mode": "unavailable",
                    "dominant_family": None,
                    "secondary_families": [],
                    "unavailable_group_count": unavailable_group_count,
                    "unavailable_breakdown": {
                        "missing_target_group_count": missing_target_group_count,
                        "unsampled_group_count": unsampled_group_count,
                        "failed_group_count": failed_group_count,
                        "no_signal_group_count": no_signal_group_count,
                    },
                    "summary_text": summary_text,
                    "families": [],
                },
                group_context,
            )

        max_count = families[0]["count"]
        top_families = [family for family in families if family["count"] == max_count]
        dominance_mode = "mixed_no_clear_dominant" if len(top_families) > 1 else "dominant"
        dominant_family = top_families[0] if len(top_families) == 1 else None
        secondary_families: List[Dict[str, Any]] = []

        for family in families:
            if dominant_family is None:
                family["position"] = "mixed_no_clear_dominant"
            elif family["key"] == dominant_family["key"]:
                family["position"] = "dominant"
            elif family["count"] == 1:
                family["position"] = "outlier"
            else:
                family["position"] = "secondary"
                secondary_families.append(family)

            public_family = self.public_redirect_family(family)
            for group_key in family["group_keys"]:
                group_context[group_key]["position"] = family["position"]
                group_context[group_key]["family"] = public_family

        public_families = [self.public_redirect_family(family) for family in families]
        if dominant_family is None:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"redirect hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"{analyzed_group_count} analyzable for redirect-route overlap, {len(families)} redirect families. "
                f"No single dominant redirect family; top families are tied at {max_count} groups. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )
        else:
            summary_text = (
                f"shown pivot bucket sample: {sample_group_count} grouped ads shown, "
                f"redirect hints sampled {sampled_target_count} of {unique_target_count} unique landing targets, "
                f"{analyzed_group_count} analyzable for redirect-route overlap, {len(families)} redirect families. "
                f"Dominant redirect family: {dominant_family['label']} ({dominant_family['count']} groups). "
                f"Other recurring families: {len(secondary_families)}. "
                f"Unavailable or unsampled groups: {unavailable_group_count}."
            )

        return (
            {
                "sample_group_count": sample_group_count,
                "sampled_group_count": sampling["sampled_group_count"],
                "analyzed_group_count": analyzed_group_count,
                "family_count": len(families),
                "sampled_target_count": sampled_target_count,
                "unique_target_count": unique_target_count,
                "dominance_mode": dominance_mode,
                "dominant_family": self.public_redirect_family(dominant_family) if dominant_family else None,
                "secondary_families": [self.public_redirect_family(family) for family in secondary_families],
                "unavailable_group_count": unavailable_group_count,
                "unavailable_breakdown": {
                    "missing_target_group_count": missing_target_group_count,
                    "unsampled_group_count": unsampled_group_count,
                    "failed_group_count": failed_group_count,
                    "no_signal_group_count": no_signal_group_count,
                },
                "summary_text": summary_text,
                "families": public_families,
            },
            group_context,
        )

    @classmethod
    def describe_current_bucket_position(
        cls,
        *,
        bucket_summary: Dict[str, Any],
        current_context: Optional[Dict[str, Any]],
    ) -> str:
        if current_context is None:
            return "unavailable - current card had no landing URL or host to place into the current pivot bucket LP summary"

        cluster = current_context["cluster"]
        position = current_context["position"]
        analyzed_group_count = bucket_summary["analyzed_group_count"]
        dominant_cluster = bucket_summary.get("dominant_cluster")

        if position == "dominant":
            return (
                f"dominant - current card matches dominant LP cluster {cluster['label']} "
                f"({cluster['count']} of {analyzed_group_count} analyzed grouped ads shown in this pivot bucket sample)"
            )
        if position == "secondary":
            dominant_label = dominant_cluster["label"] if isinstance(dominant_cluster, dict) else "unavailable"
            dominant_count = dominant_cluster["count"] if isinstance(dominant_cluster, dict) else "unavailable"
            return (
                f"secondary - current card sits in recurring LP cluster {cluster['label']} "
                f"({cluster['count']} of {analyzed_group_count} analyzed grouped ads shown); "
                f"dominant cluster is {dominant_label} ({dominant_count} groups)"
            )
        if position == "outlier":
            dominant_label = dominant_cluster["label"] if isinstance(dominant_cluster, dict) else "unavailable"
            dominant_count = dominant_cluster["count"] if isinstance(dominant_cluster, dict) else "unavailable"
            return (
                f"outlier - current card sits alone in LP cluster {cluster['label']}; "
                f"dominant cluster is {dominant_label} ({dominant_count} groups)"
            )
        return (
            f"mixed_no_clear_dominant - no single dominant LP cluster; current card sits in cluster {cluster['label']} "
            f"({cluster['count']} of {analyzed_group_count} analyzed grouped ads shown)"
        )

    @classmethod
    def describe_current_overlap_position(
        cls,
        *,
        overlap_summary: Dict[str, Any],
        current_context: Optional[Dict[str, Any]],
    ) -> str:
        if current_context is None:
            return "unavailable - current card had no landing/title signals for the current pivot bucket overlap hints"

        family = current_context["family"]
        position = current_context["position"]
        analyzed_group_count = overlap_summary["analyzed_group_count"]
        dominant_family = overlap_summary.get("dominant_family")

        if position == "dominant":
            return (
                f"dominant - current card matches dominant overlap family {family['label']} "
                f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown in this pivot bucket sample)"
            )
        if position == "secondary":
            dominant_label = dominant_family["label"] if isinstance(dominant_family, dict) else "unavailable"
            dominant_count = dominant_family["count"] if isinstance(dominant_family, dict) else "unavailable"
            return (
                f"secondary - current card sits in recurring overlap family {family['label']} "
                f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown); "
                f"dominant overlap family is {dominant_label} ({dominant_count} groups)"
            )
        if position == "outlier":
            dominant_label = dominant_family["label"] if isinstance(dominant_family, dict) else "unavailable"
            dominant_count = dominant_family["count"] if isinstance(dominant_family, dict) else "unavailable"
            return (
                f"outlier - current card sits alone in overlap family {family['label']}; "
                f"dominant overlap family is {dominant_label} ({dominant_count} groups)"
            )
        return (
            f"mixed_no_clear_dominant - no single dominant overlap family; current card sits in family {family['label']} "
            f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown)"
        )

    @classmethod
    def describe_current_stack_position(
        cls,
        *,
        stack_summary: Dict[str, Any],
        current_context: Optional[Dict[str, Any]],
    ) -> str:
        if current_context is None:
            return INSPECT_NON_PIVOT_BUCKET_NOTE

        issue = current_context.get("issue")
        if issue == "missing_target":
            return "unavailable - current card had no landing URL for sampled stack hints"
        if issue == "failed":
            return "unavailable - sampled stack fetch failed for the current card target"
        if issue == "no_signal":
            return "unavailable - sampled off-browser fetch found no useful tracker/tech hints for the current card target"
        if issue == "unsampled":
            return "unavailable - current card target was not included in the bounded stack sample"

        family = current_context.get("family")
        if not isinstance(family, dict):
            return "unavailable - current card had no sampled stack family in the current pivot bucket sample"

        position = current_context["position"]
        analyzed_group_count = stack_summary["analyzed_group_count"]
        dominant_family = stack_summary.get("dominant_family")

        if position == "dominant":
            return (
                f"dominant - current card matches dominant stack family {family['label']} "
                f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown in this pivot bucket sample)"
            )
        if position == "secondary":
            dominant_label = dominant_family["label"] if isinstance(dominant_family, dict) else "unavailable"
            dominant_count = dominant_family["count"] if isinstance(dominant_family, dict) else "unavailable"
            return (
                f"secondary - current card sits in recurring stack family {family['label']} "
                f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown); "
                f"dominant stack family is {dominant_label} ({dominant_count} groups)"
            )
        if position == "outlier":
            dominant_label = dominant_family["label"] if isinstance(dominant_family, dict) else "unavailable"
            dominant_count = dominant_family["count"] if isinstance(dominant_family, dict) else "unavailable"
            return (
                f"outlier - current card sits alone in stack family {family['label']}; "
                f"dominant stack family is {dominant_label} ({dominant_count} groups)"
            )
        return (
            f"mixed_no_clear_dominant - no single dominant stack family; current card sits in family {family['label']} "
            f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown)"
        )

    @classmethod
    def describe_current_delivery_position(
        cls,
        *,
        delivery_summary: Dict[str, Any],
        current_context: Optional[Dict[str, Any]],
    ) -> str:
        if current_context is None:
            return INSPECT_NON_PIVOT_BUCKET_NOTE

        issue = current_context.get("issue")
        if issue == "missing_target":
            return "unavailable - current card had no landing URL for sampled delivery hints"
        if issue == "failed":
            return "unavailable - sampled delivery fetch failed for the current card target"
        if issue == "no_signal":
            return "unavailable - sampled off-browser fetch did not resolve a useful final-destination family for the current card target"
        if issue == "unsampled":
            return "unavailable - current card target was not included in the bounded delivery sample"

        family = current_context.get("family")
        if not isinstance(family, dict):
            return "unavailable - current card had no sampled delivery family in the current pivot bucket sample"

        position = current_context["position"]
        analyzed_group_count = delivery_summary["analyzed_group_count"]
        dominant_family = delivery_summary.get("dominant_family")

        if position == "dominant":
            return (
                f"dominant - current card matches dominant delivery family {family['label']} "
                f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown in this pivot bucket sample)"
            )
        if position == "secondary":
            dominant_label = dominant_family["label"] if isinstance(dominant_family, dict) else "unavailable"
            dominant_count = dominant_family["count"] if isinstance(dominant_family, dict) else "unavailable"
            return (
                f"secondary - current card sits in recurring delivery family {family['label']} "
                f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown); "
                f"dominant delivery family is {dominant_label} ({dominant_count} groups)"
            )
        if position == "outlier":
            dominant_label = dominant_family["label"] if isinstance(dominant_family, dict) else "unavailable"
            dominant_count = dominant_family["count"] if isinstance(dominant_family, dict) else "unavailable"
            return (
                f"outlier - current card sits alone in delivery family {family['label']}; "
                f"dominant delivery family is {dominant_label} ({dominant_count} groups)"
            )
        return (
            f"mixed_no_clear_dominant - no single dominant delivery family; current card sits in family {family['label']} "
            f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown)"
        )

    @classmethod
    def describe_current_redirect_position(
        cls,
        *,
        redirect_summary: Dict[str, Any],
        current_context: Optional[Dict[str, Any]],
    ) -> str:
        if current_context is None:
            return INSPECT_NON_PIVOT_BUCKET_NOTE

        issue = current_context.get("issue")
        if issue == "missing_target":
            return "unavailable - current card had no landing URL for sampled redirect hints"
        if issue == "failed":
            return "unavailable - sampled redirect fetch failed for the current card target"
        if issue == "no_signal":
            return "unavailable - sampled off-browser fetch did not resolve a useful redirect-route family for the current card target"
        if issue == "unsampled":
            return "unavailable - current card target was not included in the bounded redirect sample"

        family = current_context.get("family")
        if not isinstance(family, dict):
            return "unavailable - current card had no sampled redirect family in the current pivot bucket sample"

        position = current_context["position"]
        analyzed_group_count = redirect_summary["analyzed_group_count"]
        dominant_family = redirect_summary.get("dominant_family")

        if position == "dominant":
            return (
                f"dominant - current card matches dominant redirect family {family['label']} "
                f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown in this pivot bucket sample)"
            )
        if position == "secondary":
            dominant_label = dominant_family["label"] if isinstance(dominant_family, dict) else "unavailable"
            dominant_count = dominant_family["count"] if isinstance(dominant_family, dict) else "unavailable"
            return (
                f"secondary - current card sits in recurring redirect family {family['label']} "
                f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown); "
                f"dominant redirect family is {dominant_label} ({dominant_count} groups)"
            )
        if position == "outlier":
            dominant_label = dominant_family["label"] if isinstance(dominant_family, dict) else "unavailable"
            dominant_count = dominant_family["count"] if isinstance(dominant_family, dict) else "unavailable"
            return (
                f"outlier - current card sits alone in redirect family {family['label']}; "
                f"dominant redirect family is {dominant_label} ({dominant_count} groups)"
            )
        return (
            f"mixed_no_clear_dominant - no single dominant redirect family; current card sits in family {family['label']} "
            f"({family['count']} of {analyzed_group_count} analyzed grouped ads shown)"
        )

    def pivot_bucket_context_for_session_group(
        self,
        session: SearchSession,
        group: GroupedAdEntity,
    ) -> Dict[str, Any]:
        pivot_context = self.session_pivot_context(session)
        pivot_surface = safe_text((pivot_context or {}).get("pivot_surface")).strip().lower()
        if pivot_surface not in {"page", "domain"}:
            return {
                "pivot_bucket_summary": None,
                "pivot_bucket_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "current_lp_cluster": None,
                "current_bucket_position": "unavailable",
                "current_bucket_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "pivot_bucket_overlap_summary": None,
                "pivot_bucket_overlap_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "current_overlap_family": None,
                "current_overlap_position": "unavailable",
                "current_overlap_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "pivot_bucket_stack_summary": None,
                "pivot_bucket_stack_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "current_stack_family": None,
                "current_stack_position": "unavailable",
                "current_stack_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "pivot_bucket_delivery_summary": None,
                "pivot_bucket_delivery_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "current_delivery_family": None,
                "current_delivery_position": "unavailable",
                "current_delivery_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "pivot_bucket_redirect_summary": None,
                "pivot_bucket_redirect_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
                "current_redirect_family": None,
                "current_redirect_position": "unavailable",
                "current_redirect_note": INSPECT_NON_PIVOT_BUCKET_NOTE,
            }

        groups = self.all_groups(session.search_session_id, emitted_only=True)
        bucket_summary, group_context = self.summarize_pivot_bucket_groups(groups)
        overlap_summary, overlap_context = self.summarize_pivot_bucket_overlap(groups)
        stack_summary, stack_context = self.summarize_pivot_bucket_stack(groups, current_group_key=group.group_key)
        sampled_delivery_data = self.sample_pivot_bucket_delivery_reports(
            groups,
            current_group_key=group.group_key,
            sample_limit=STACK_HINT_SAMPLE_MAX_TARGETS,
        )
        delivery_summary, delivery_context = self.summarize_pivot_bucket_delivery(
            groups,
            current_group_key=group.group_key,
            sampled_delivery_data=sampled_delivery_data,
        )
        redirect_summary, redirect_context = self.summarize_pivot_bucket_redirect(
            groups,
            current_group_key=group.group_key,
            sampled_delivery_data=sampled_delivery_data,
        )
        current_context = group_context.get(group.group_key)
        current_overlap_context = overlap_context.get(group.group_key)
        current_stack_context = stack_context.get(group.group_key)
        current_delivery_context = delivery_context.get(group.group_key)
        current_redirect_context = redirect_context.get(group.group_key)
        return {
            "pivot_bucket_summary": bucket_summary,
            "pivot_bucket_note": bucket_summary["summary_text"],
            "current_lp_cluster": current_context["cluster"] if current_context else None,
            "current_bucket_position": current_context["position"] if current_context else "unavailable",
            "current_bucket_note": self.describe_current_bucket_position(
                bucket_summary=bucket_summary,
                current_context=current_context,
            ),
            "pivot_bucket_overlap_summary": overlap_summary,
            "pivot_bucket_overlap_note": overlap_summary["summary_text"],
            "current_overlap_family": current_overlap_context["family"] if current_overlap_context else None,
            "current_overlap_position": current_overlap_context["position"] if current_overlap_context else "unavailable",
            "current_overlap_note": self.describe_current_overlap_position(
                overlap_summary=overlap_summary,
                current_context=current_overlap_context,
            ),
            "pivot_bucket_stack_summary": stack_summary,
            "pivot_bucket_stack_note": stack_summary["summary_text"],
            "current_stack_family": current_stack_context["family"] if current_stack_context else None,
            "current_stack_position": current_stack_context["position"] if current_stack_context else "unavailable",
            "current_stack_note": self.describe_current_stack_position(
                stack_summary=stack_summary,
                current_context=current_stack_context,
            ),
            "pivot_bucket_delivery_summary": delivery_summary,
            "pivot_bucket_delivery_note": delivery_summary["summary_text"],
            "current_delivery_family": current_delivery_context["family"] if current_delivery_context else None,
            "current_delivery_position": current_delivery_context["position"] if current_delivery_context else "unavailable",
            "current_delivery_note": self.describe_current_delivery_position(
                delivery_summary=delivery_summary,
                current_context=current_delivery_context,
            ),
            "pivot_bucket_redirect_summary": redirect_summary,
            "pivot_bucket_redirect_note": redirect_summary["summary_text"],
            "current_redirect_family": current_redirect_context["family"] if current_redirect_context else None,
            "current_redirect_position": current_redirect_context["position"] if current_redirect_context else "unavailable",
            "current_redirect_note": self.describe_current_redirect_position(
                redirect_summary=redirect_summary,
                current_context=current_redirect_context,
            ),
        }

    def persist_pivot_context(self, search_session_id: Optional[str], pivot_payload: Dict[str, Any]) -> Optional[SearchSession]:
        if not search_session_id:
            return None
        self.update_session(search_session_id, pivot_json=compact_json(pivot_payload))
        return self.load_session(search_session_id)

    def attach_pivot_bucket_summary_to_result(self, result: Dict[str, Any]) -> None:
        pivot_payload = ((result.get("data") or {}).get("pivot")) or {}
        pivot_surface = safe_text(pivot_payload.get("pivot_surface")).strip().lower()
        if pivot_surface not in {"page", "domain"}:
            return

        search_session_id = safe_text((((result.get("data") or {}).get("search_session")) or {}).get("search_session_id")).strip()
        if not search_session_id:
            return

        session = self.load_session(search_session_id)
        emitted_groups = self.all_groups(session.search_session_id, emitted_only=True)
        bucket_summary, _group_context = self.summarize_pivot_bucket_groups(emitted_groups)
        overlap_summary, _overlap_context = self.summarize_pivot_bucket_overlap(emitted_groups)
        result["data"]["pivot"]["pivot_bucket_summary"] = bucket_summary
        result["data"]["pivot"]["pivot_bucket_overlap_summary"] = overlap_summary

        grouped_results = ((result.get("data") or {}).get("grouped_results")) or []
        if not grouped_results or not result.get("messages"):
            return

        summary_lines = [
            line
            for line in safe_text(result["messages"][0].get("text")).splitlines()
            if not line.startswith("Pivot bucket LP summary:")
            and not line.startswith("Bucket overlap hints:")
        ]
        summary_text = "\n".join(summary_lines).strip()
        result["messages"][0]["text"] = (
            summary_text
            + "\n"
            + f"Pivot bucket LP summary: {bucket_summary['summary_text']}"
            + "\n"
            + f"Bucket overlap hints: {overlap_summary['summary_text']}"
        ).strip()
        result["summary"] = result["messages"][0]["text"]

    def un_emitted_group_candidates(self, search_session_id: str) -> List[BufferedSelectionCandidate]:
        rows = self.conn.execute(
            """
            SELECT * FROM session_groups
            WHERE search_session_id = ? AND emitted = 0
            ORDER BY ordinal ASC
            """,
            (search_session_id,),
        ).fetchall()
        candidates: List[BufferedSelectionCandidate] = []
        for row in rows:
            group = self.group_from_row(row)
            candidates.append(
                BufferedSelectionCandidate(
                    source="expanded_group",
                    ordinal=row["ordinal"],
                    group_key=group.group_key,
                    advertiser=group.advertiser,
                    landing_domain=self.group_primary_domain(group),
                    group=group,
                )
            )
        return candidates

    def buffered_selection_candidates(self, search_session_id: str) -> List[BufferedSelectionCandidate]:
        candidates = self.un_emitted_group_candidates(search_session_id)
        for pending in self.pending_candidates(search_session_id):
            candidates.append(
                BufferedSelectionCandidate(
                    source="pending_candidate",
                    ordinal=pending.ordinal,
                    group_key=pending.group_key,
                    advertiser=pending.representative.advertiser,
                    landing_domain=self.record_primary_domain(pending.representative),
                    pending=pending,
                )
            )
        return candidates

    @staticmethod
    def build_query_relevance_profile(keyword: str) -> QueryRelevanceProfile:
        normalized_keyword = normalize_string(keyword)
        query_tokens = search_relevance_query_tokens(keyword)
        generic_tokens = tuple(token for token in query_tokens if token in SEARCH_RELEVANCE_GENERIC_TOKENS)
        intent_tokens = tuple(token for token in query_tokens if token not in SEARCH_RELEVANCE_GENERIC_TOKENS)
        return QueryRelevanceProfile(
            keyword=safe_text(keyword).strip(),
            normalized_keyword=normalized_keyword,
            intent_tokens=intent_tokens,
            generic_tokens=generic_tokens,
        )

    @staticmethod
    def buffered_candidate_text_fields(candidate: BufferedSelectionCandidate) -> Dict[str, str]:
        if candidate.group is not None:
            domains = ordered_unique_domains(list(candidate.group.search_domains) + [candidate.group.search_domain] + candidate.group.landing_domains)
            landing_urls = candidate.group.landing_page_urls
            creative_titles = candidate.group.creative_titles
            creative_text = candidate.group.creative_text
            advertiser = candidate.group.advertiser
        else:
            if candidate.pending is None:
                raise AcquisitionError(f"Buffered selection candidate {candidate.group_key} has no materialized group or pending record")
            representative = candidate.pending.representative
            domains = ordered_unique_domains(list(representative.search_domains) + [representative.search_domain, representative.landing_domain])
            landing_urls = [representative.landing_page_url]
            creative_titles = [representative.creative_title] if representative.creative_title else []
            creative_text = representative.creative_body
            advertiser = representative.advertiser
        return {
            "advertiser": advertiser,
            "creative_text": safe_text(creative_text).strip(),
            "creative_titles": " ".join(safe_text(item).strip() for item in creative_titles if safe_text(item).strip()),
            "domains": " ".join(domains),
            "paths": search_relevance_url_path_text(landing_urls),
        }

    @classmethod
    def score_buffered_candidate_relevance(
        cls,
        profile: QueryRelevanceProfile,
        candidate: BufferedSelectionCandidate,
    ) -> BufferedCandidateRelevance:
        fields = cls.buffered_candidate_text_fields(candidate)
        field_weights = {
            "creative_titles": (8, 2),
            "creative_text": (6, 1),
            "paths": (5, 1),
            "domains": (4, 1),
            "advertiser": (3, 0),
        }
        matched_intent_tokens: List[str] = []
        matched_generic_tokens: List[str] = []
        score = 0
        for field_name, field_value in fields.items():
            field_tokens = search_relevance_field_tokens(field_value)
            intent_hits = matched_query_tokens(profile.intent_tokens, field_tokens)
            generic_hits = matched_query_tokens(profile.generic_tokens, field_tokens)
            matched_intent_tokens.extend(intent_hits)
            matched_generic_tokens.extend(generic_hits)
            intent_weight, generic_weight = field_weights[field_name]
            score += len(intent_hits) * intent_weight
            score += len(generic_hits) * generic_weight
        matched_intent = tuple(ordered_unique_tokens(matched_intent_tokens))
        matched_generic = tuple(ordered_unique_tokens(matched_generic_tokens))
        if matched_intent:
            tier = "strong_intent"
        elif matched_generic:
            tier = "generic_only"
        else:
            tier = "no_overlap"
        return BufferedCandidateRelevance(
            tier=tier,
            score=score,
            matched_intent_tokens=matched_intent,
            matched_generic_tokens=matched_generic,
        )

    @staticmethod
    def better_buffered_candidate(
        left: Tuple[BufferedSelectionCandidate, BufferedCandidateRelevance],
        right: Tuple[BufferedSelectionCandidate, BufferedCandidateRelevance],
    ) -> bool:
        left_candidate, left_relevance = left
        right_candidate, right_relevance = right
        if left_relevance.score != right_relevance.score:
            return left_relevance.score > right_relevance.score
        return left_candidate.ordinal < right_candidate.ordinal

    def choose_diverse_buffered_candidate(
        self,
        remaining: Sequence[Tuple[BufferedSelectionCandidate, BufferedCandidateRelevance]],
        *,
        seen_advertisers: set[str],
        seen_domains: set[str],
        tier: str,
    ) -> Tuple[Optional[int], str]:
        tier_candidates = [(index, item) for index, item in enumerate(remaining) if item[1].tier == tier]
        if not tier_candidates:
            return None, "source_order_fallback"

        def pick_best(
            predicate: Optional[Callable[[str, Optional[str]], bool]],
            reason: str,
        ) -> Tuple[Optional[int], str]:
            best: Optional[Tuple[int, Tuple[BufferedSelectionCandidate, BufferedCandidateRelevance]]] = None
            for index, item in tier_candidates:
                candidate, _relevance = item
                advertiser_key = normalize_string(candidate.advertiser)
                domain_key = candidate.landing_domain
                if predicate is not None and not predicate(advertiser_key, domain_key):
                    continue
                if best is None or self.better_buffered_candidate(item, best[1]):
                    best = (index, item)
            return (best[0], reason) if best is not None else (None, reason)

        chosen_index, reason = pick_best(
            lambda advertiser_key, domain_key: bool(
                advertiser_key
                and advertiser_key not in seen_advertisers
                and domain_key
                and domain_key not in seen_domains
            ),
            "new_advertiser_and_domain",
        )
        if chosen_index is not None:
            return chosen_index, reason
        chosen_index, reason = pick_best(
            lambda advertiser_key, _domain_key: bool(advertiser_key and advertiser_key not in seen_advertisers),
            "new_advertiser",
        )
        if chosen_index is not None:
            return chosen_index, reason
        chosen_index, reason = pick_best(None, "source_order_fallback")
        return chosen_index, reason

    def select_initial_buffered_candidates(
        self,
        session: SearchSession,
        *,
        limit: int,
    ) -> Tuple[List[BufferedSelectionCandidate], Dict[str, Any]]:
        candidates = self.buffered_selection_candidates(session.search_session_id)
        profile = self.build_query_relevance_profile(session.keyword)
        if not profile.usable_tokens:
            selected, debug = self.select_diverse_buffered_candidates(session.search_session_id, limit=limit)
            debug.update(
                {
                    "relevance_enabled": False,
                    "relevance_profile": profile.as_dict(),
                    "selected_strong_intent_count": 0,
                    "weak_match_note": None,
                }
            )
            return selected, debug

        emitted_groups = self.all_groups(session.search_session_id, emitted_only=True)
        seen_advertisers = {
            normalize_string(group.advertiser) for group in emitted_groups if normalize_string(group.advertiser)
        }
        seen_domains = {
            domain
            for group in emitted_groups
            for domain in [self.group_primary_domain(group)]
            if domain
        }
        scored_candidates = [(candidate, self.score_buffered_candidate_relevance(profile, candidate)) for candidate in candidates]
        selected: List[BufferedSelectionCandidate] = []
        selection_rows: List[Dict[str, Any]] = []
        remaining = list(scored_candidates)

        while remaining and len(selected) < limit:
            chosen_index: Optional[int] = None
            chosen_reason = "source_order_fallback"
            for tier in SEARCH_RELEVANCE_TIER_ORDER:
                chosen_index, chosen_reason = self.choose_diverse_buffered_candidate(
                    remaining,
                    seen_advertisers=seen_advertisers,
                    seen_domains=seen_domains,
                    tier=tier,
                )
                if chosen_index is not None:
                    break
            if chosen_index is None:
                break
            candidate, relevance = remaining.pop(chosen_index)
            selected.append(candidate)
            advertiser_key = normalize_string(candidate.advertiser)
            domain_key = candidate.landing_domain
            if advertiser_key:
                seen_advertisers.add(advertiser_key)
            if domain_key:
                seen_domains.add(domain_key)
            selection_rows.append(
                {
                    "group_key": candidate.group_key,
                    "advertiser": candidate.advertiser,
                    "landing_domain": domain_key,
                    "selection_reason": chosen_reason,
                    "source": candidate.source,
                    **relevance.as_dict(),
                }
            )

        selected_strong_intent_count = sum(1 for item in selection_rows if item["tier"] == "strong_intent")
        weak_match_note = None
        if selection_rows and selected_strong_intent_count < SEARCH_RELEVANCE_WEAK_MATCH_MIN_STRONG:
            weak_match_note = SEARCH_RELEVANCE_WEAK_MATCH_NOTE
        return selected, {
            "selected_count": len(selected),
            "candidate_count": len(candidates),
            "selection": selection_rows,
            "relevance_enabled": True,
            "relevance_profile": profile.as_dict(),
            "selected_strong_intent_count": selected_strong_intent_count,
            "weak_match_note": weak_match_note,
        }

    def select_diverse_buffered_candidates(
        self,
        search_session_id: str,
        *,
        limit: int,
    ) -> Tuple[List[BufferedSelectionCandidate], Dict[str, Any]]:
        candidates = self.buffered_selection_candidates(search_session_id)
        emitted_groups = self.all_groups(search_session_id, emitted_only=True)
        seen_advertisers = {
            normalize_string(group.advertiser) for group in emitted_groups if normalize_string(group.advertiser)
        }
        seen_domains = {
            domain
            for group in emitted_groups
            for domain in [self.group_primary_domain(group)]
            if domain
        }
        selected: List[BufferedSelectionCandidate] = []
        selection_rows: List[Dict[str, Any]] = []
        remaining = list(candidates)

        while remaining and len(selected) < limit:
            chosen_index: Optional[int] = None
            chosen_reason = "source_order_fallback"
            for index, candidate in enumerate(remaining):
                advertiser_key = normalize_string(candidate.advertiser)
                domain_key = candidate.landing_domain
                if advertiser_key and advertiser_key not in seen_advertisers and domain_key and domain_key not in seen_domains:
                    chosen_index = index
                    chosen_reason = "new_advertiser_and_domain"
                    break
            if chosen_index is None:
                for index, candidate in enumerate(remaining):
                    advertiser_key = normalize_string(candidate.advertiser)
                    if advertiser_key and advertiser_key not in seen_advertisers:
                        chosen_index = index
                        chosen_reason = "new_advertiser"
                        break
            if chosen_index is None:
                chosen_index = 0
            candidate = remaining.pop(chosen_index)
            selected.append(candidate)
            advertiser_key = normalize_string(candidate.advertiser)
            domain_key = candidate.landing_domain
            if advertiser_key:
                seen_advertisers.add(advertiser_key)
            if domain_key:
                seen_domains.add(domain_key)
            selection_rows.append(
                {
                    "group_key": candidate.group_key,
                    "advertiser": candidate.advertiser,
                    "landing_domain": domain_key,
                    "selection_reason": chosen_reason,
                    "source": candidate.source,
                }
            )
        return selected, {
            "selected_count": len(selected),
            "candidate_count": len(candidates),
            "selection": selection_rows,
        }

    def realize_selected_groups(
        self,
        session: SearchSession,
        candidates: Sequence[BufferedSelectionCandidate],
    ) -> Tuple[List[GroupedAdEntity], Dict[str, Any]]:
        realized: List[GroupedAdEntity] = []
        expanded_count = 0
        for candidate in candidates:
            if candidate.group is not None:
                realized.append(candidate.group)
                continue
            if candidate.pending is None:
                raise AcquisitionError(f"Buffered selection candidate {candidate.group_key} has no materialized group or pending record")
            collation_records, _collation_payload, _aggregate_payload = self.get_collation_records(
                candidate.pending.representative, session.graphql_session_id
            )
            group = self.build_grouped_entity(candidate.pending.representative, collation_records, details=None)
            self.append_group(session.search_session_id, group)
            self.delete_pending_candidate(session.search_session_id, candidate.group_key)
            realized.append(self.load_group(session.search_session_id, group.group_key))
            expanded_count += 1
        return realized, {
            "candidates_expanded_into_groups": expanded_count,
            "unexpanded_candidates_buffered": self.pending_candidate_count(session.search_session_id),
        }

    def select_diverse_groups(
        self,
        search_session_id: str,
        *,
        limit: int,
    ) -> Tuple[List[GroupedAdEntity], Dict[str, Any]]:
        candidates = self.un_emitted_groups(
            search_session_id,
            max(limit, self.un_emitted_group_count(search_session_id)),
        )
        emitted_groups = self.all_groups(search_session_id, emitted_only=True)
        seen_advertisers = {
            normalize_string(group.advertiser) for group in emitted_groups if normalize_string(group.advertiser)
        }
        seen_domains = {
            domain
            for group in emitted_groups
            for domain in [self.group_primary_domain(group)]
            if domain
        }
        selected: List[GroupedAdEntity] = []
        selection_rows: List[Dict[str, Any]] = []
        remaining = list(candidates)

        while remaining and len(selected) < limit:
            chosen_index: Optional[int] = None
            chosen_reason = "source_order_fallback"
            for index, group in enumerate(remaining):
                advertiser_key = normalize_string(group.advertiser)
                domain_key = self.group_primary_domain(group)
                if advertiser_key and advertiser_key not in seen_advertisers and domain_key and domain_key not in seen_domains:
                    chosen_index = index
                    chosen_reason = "new_advertiser_and_domain"
                    break
            if chosen_index is None:
                for index, group in enumerate(remaining):
                    advertiser_key = normalize_string(group.advertiser)
                    if advertiser_key and advertiser_key not in seen_advertisers:
                        chosen_index = index
                        chosen_reason = "new_advertiser"
                        break
            if chosen_index is None:
                chosen_index = 0
            group = remaining.pop(chosen_index)
            selected.append(group)
            advertiser_key = normalize_string(group.advertiser)
            domain_key = self.group_primary_domain(group)
            if advertiser_key:
                seen_advertisers.add(advertiser_key)
            if domain_key:
                seen_domains.add(domain_key)
            selection_rows.append(
                {
                    "group_key": group.group_key,
                    "advertiser": group.advertiser,
                    "landing_domain": domain_key,
                    "selection_reason": chosen_reason,
                }
            )
        return selected, {
            "selected_count": len(selected),
            "candidate_count": len(candidates),
            "selection": selection_rows,
        }

    def emit_next_groups(self, session: SearchSession, limit: int) -> Tuple[SearchSession, List[GroupedAdEntity], Dict[str, Any]]:
        session, buffer_debug = self.ensure_buffered_candidates(session, limit)
        initial_batch = self.emitted_group_count(session.search_session_id) == 0
        if initial_batch:
            selected, selection_debug = self.select_initial_buffered_candidates(session, limit=limit)
        else:
            selected, selection_debug = self.select_diverse_buffered_candidates(session.search_session_id, limit=limit)
        if not selected and session.exhausted:
            return session, [], {
                **buffer_debug,
                **selection_debug,
            }
        groups, realization_debug = self.realize_selected_groups(session, selected)
        self.mark_groups_emitted(session.search_session_id, [group.group_key for group in groups])
        session = self.load_session(session.search_session_id)
        return session, groups, {
            **buffer_debug,
            **selection_debug,
            **realization_debug,
        }

    # ----------------------- Formatting -----------------------

    def build_grouped_card_lines(
        self,
        group: GroupedAdEntity,
        *,
        include_media_line: bool,
        include_action_hint: bool,
        include_creative_text: bool = True,
        include_creative_titles: bool = True,
        include_variant_notes: bool = True,
    ) -> List[str]:
        lines: List[str] = []
        lines.append(f"{group.advertiser}")
        if group.page_profile_url:
            lines.append(f"Page: {group.page_profile_url}")
        lines.append(f"Duplicate count: {group.duplicate_count}")
        if group.creative_variants_count:
            lines.append(f"Creative variants: {group.creative_variants_count}")
        if group.days_active is not None:
            lines.append(f"Days active: {group.days_active}")
        if group.active_start_date or group.active_end_date:
            if group.active_start_date and group.active_end_date:
                period = f"{group.active_start_date} to {group.active_end_date}"
            elif group.active_start_date:
                period = f"Started {group.active_start_date}"
            else:
                period = f"Ended {group.active_end_date}"
            lines.append(f"Active period: {period}")
        if group.landing_domain:
            lines.append(f"Landing domain: {group.landing_domain}")
        elif group.landing_domains:
            lines.append(f"Landing domains: {', '.join(group.landing_domains)}")
        landing_domain_set = {
            normalize_domain_text(domain)
            for domain in group.landing_domains
            if normalize_domain_text(domain)
        }
        if group.search_domain and group.search_domain not in landing_domain_set:
            lines.append(f"Search domain: {group.search_domain}")
        if group.page_likes is not None:
            lines.append(f"Facebook Page likes (Meta): {group.page_likes:,}")
        if group.ig_followers is not None:
            lines.append(f"IG followers: {group.ig_followers:,}")
        if include_creative_text and (
            group.creative_text or group.creative_text_availability == CREATIVE_AVAILABILITY_PLACEHOLDER
        ):
            lines.append("")
            lines.append("Creative text:")
            lines.append(group.creative_text.strip() if group.creative_text else CREATIVE_PLACEHOLDER_NOTE)
        if include_creative_titles and (
            group.creative_titles or group.creative_titles_availability == CREATIVE_AVAILABILITY_PLACEHOLDER
        ):
            lines.append("")
            lines.append("Creative titles:")
            if group.creative_titles:
                lines.append("; ".join(group.creative_titles[:5]))
            else:
                lines.append(CREATIVE_PLACEHOLDER_NOTE)
        if include_media_line and group.media_url:
            lines.append("")
            lines.append(f"Media: {group.media_url}")
        if group.landing_page_urls:
            lines.append("")
            lines.append("Landing URLs:")
            for url in group.landing_page_urls:
                lines.append(f"- {url}")
        if include_variant_notes and group.grouped_notes:
            lines.append("")
            lines.append(f"Variant notes: {group.grouped_notes}")
        if group.ad_library_links:
            lines.append("")
            lines.append("Ad Library links:")
            for url in group.ad_library_links:
                lines.append(f"- {url}")
        if include_action_hint:
            action_hint_lines = self.group_action_hint_lines(group)
            if action_hint_lines:
                lines.append("")
                lines.extend(action_hint_lines)
        return lines

    def group_inline_commands(self, group: GroupedAdEntity) -> List[str]:
        commands: List[str] = []
        page_id = self.normalize_explicit_page_id(group.page_id)
        if page_id:
            commands.append(f"/ads page {page_id}")
        pivot_domains = self.group_pivot_domains(group)
        if len(pivot_domains) == 1:
            commands.append(f"/ads domain {pivot_domains[0]}")
        landing_url = group.landing_page_urls[0] if group.landing_page_urls else None
        if landing_url:
            commands.append(f"/ads inspect {landing_url}")
        return commands

    def group_action_hint_lines(self, group: GroupedAdEntity) -> List[str]:
        commands = self.group_inline_commands(group)
        if not commands:
            return []
        return [GROUP_ACTION_HINT_LABEL, *commands]

    def format_grouped_card(self, group: GroupedAdEntity, *, include_media_line: bool = True) -> str:
        render_attempts = [
            dict(include_action_hint=True, include_creative_text=True, include_creative_titles=True, include_variant_notes=True),
            dict(include_action_hint=True, include_creative_text=False, include_creative_titles=True, include_variant_notes=True),
            dict(include_action_hint=True, include_creative_text=False, include_creative_titles=False, include_variant_notes=True),
            dict(include_action_hint=True, include_creative_text=False, include_creative_titles=False, include_variant_notes=False),
            dict(include_action_hint=False, include_creative_text=False, include_creative_titles=False, include_variant_notes=False),
        ]
        for options in render_attempts:
            lines = self.build_grouped_card_lines(
                group,
                include_media_line=include_media_line,
                **options,
            )
            body = "\n".join(lines).strip()
            if len(body) <= MAX_CHAT_MESSAGE_LEN:
                return body

        # Final fallback: keep link targets and inline commands visible until the message limit is nearly reached.
        prefix_lines = [
            group.advertiser,
            f"Duplicate count: {group.duplicate_count}",
        ]
        if group.landing_domain:
            prefix_lines.append(f"Landing domain: {group.landing_domain}")
        elif group.landing_domains:
            prefix_lines.append(f"Landing domains: {', '.join(group.landing_domains)}")
        landing_domain_set = {
            normalize_domain_text(domain)
            for domain in group.landing_domains
            if normalize_domain_text(domain)
        }
        if group.search_domain and group.search_domain not in landing_domain_set:
            prefix_lines.append(f"Search domain: {group.search_domain}")
        action_hint_lines = self.group_action_hint_lines(group)
        if action_hint_lines:
            prefix_lines.extend(["", *action_hint_lines])
        prefix_lines.extend(["", "Ad Library links:"])
        prefix = "\n".join(prefix_lines) + "\n"
        remaining = MAX_CHAT_MESSAGE_LEN - len(prefix) - len("\nSome related links were omitted due to Telegram message length.")
        chosen_links: List[str] = []
        used = 0
        for url in group.ad_library_links:
            candidate = f"- {url}\n"
            if used + len(candidate) > remaining:
                break
            chosen_links.append(candidate.rstrip())
            used += len(candidate)
        suffix = ""
        omitted = len(group.ad_library_links) - len(chosen_links)
        if omitted > 0:
            suffix = f"\nSome related links were omitted due to Telegram message length. Omitted links: {omitted}"
        return prefix + "\n".join(chosen_links) + suffix

    def format_grouped_media_caption(self, group: GroupedAdEntity) -> str:
        lines = [
            truncate_text(group.advertiser, 80),
            f"Duplicates: {group.duplicate_count}",
        ]
        if group.landing_domain:
            lines.append(f"Domain: {group.landing_domain}")
        creative_hint = truncate_text(group.creative_text or "; ".join(group.creative_titles), 160)
        if creative_hint:
            lines.append(creative_hint)
        caption = "\n".join(lines).strip()
        return truncate_text(caption, MAX_MEDIA_CAPTION_LEN)

    def grouped_message_payload(self, search_session_id: str, group: GroupedAdEntity) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "text": self.format_grouped_card(group, include_media_line=True),
            "message_kind": "grouped_ad_card",
            "bind_session_id": search_session_id,
            "bind_group_key": group.group_key,
            "media_present_in_payload": False,
            "media_outcome": "no_media_in_payload",
            "text_fallback_used": False,
            "native_media_sent": False,
        }
        if group.media_url and group.media_kind in {"photo", "video"}:
            payload.update(
                {
                    "media_present_in_payload": True,
                    "media_url": group.media_url,
                    "media_kind": group.media_kind,
                    "media_caption": self.format_grouped_media_caption(group),
                    "native_media_text": self.format_grouped_card(group, include_media_line=False),
                    "media_outcome": None,
                }
            )
        return payload

    @staticmethod
    def extract_html_title(body_text: str) -> Optional[str]:
        match = re.search(r"<title[^>]*>(.*?)</title>", body_text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        return truncate_text(html_lib.unescape(re.sub(r"\s+", " ", match.group(1))).strip(), 200) or None

    @staticmethod
    def extract_meta_refresh_target(body_text: str, current_url: str) -> Optional[str]:
        match = re.search(
            r'<meta[^>]+http-equiv=["\']?refresh["\']?[^>]+content=["\'][^"\']*url=([^"\']+)["\']',
            body_text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        target = html_lib.unescape(match.group(1)).strip()
        if not target:
            return None
        return urllib.parse.urljoin(current_url, target)

    @staticmethod
    def detect_page_hints(body_text: Optional[str], headers: Dict[str, str]) -> Tuple[List[str], List[str], List[str]]:
        lower_body = (body_text or "").lower()
        lower_headers = {str(key).lower(): safe_text(value).lower() for key, value in headers.items()}
        technologies: set[str] = set()
        trackers: set[str] = set()
        notes: List[str] = []

        server = lower_headers.get("server", "")
        powered_by = lower_headers.get("x-powered-by", "")
        if "cloudflare" in server or "cf-ray" in lower_headers:
            technologies.add("Cloudflare")
        if "shopify" in lower_body or "cdn.shopify.com" in lower_body or "shopify" in powered_by:
            technologies.add("Shopify")
        if "wp-content" in lower_body or "wp-includes" in lower_body or "wordpress" in powered_by:
            technologies.add("WordPress")
        if "clickfunnels" in lower_body:
            technologies.add("ClickFunnels")
        if "unbounce" in lower_body:
            technologies.add("Unbounce")
        if "js.hs-scripts.com" in lower_body or "hubspot" in powered_by:
            technologies.add("HubSpot")

        if "connect.facebook.net" in lower_body or "fbq(" in lower_body:
            trackers.add("Meta Pixel")
        if "googletagmanager.com/gtm.js" in lower_body or "gtm-" in lower_body:
            trackers.add("Google Tag Manager")
        if "googletagmanager.com/gtag/js" in lower_body or "gtag(" in lower_body or "google-analytics.com" in lower_body:
            trackers.add("Google Analytics")
        if "analytics.tiktok.com" in lower_body or "ttq.track" in lower_body:
            trackers.add("TikTok Pixel")
        if "static.hotjar.com" in lower_body or "hj(" in lower_body:
            trackers.add("Hotjar")
        if "cdn.segment.com" in lower_body or "analytics.load(" in lower_body:
            trackers.add("Segment")
        if "munchkin.js" in lower_body:
            trackers.add("Marketo")

        if body_text and "http-equiv=\"refresh\"" in lower_body:
            notes.append("meta refresh detected")
        return sorted(technologies), sorted(trackers), notes

    @staticmethod
    def browser_state_from_page(page: Any) -> Dict[str, Any]:
        observed_url = safe_text(getattr(page, "url", None)).strip() or None
        observed_title: Optional[str] = None
        body_text_length = 0
        body_text_excerpt: Optional[str] = None
        element_count = 0
        ready_state: Optional[str] = None
        try:
            observed_title = truncate_text(page.title(), 200) or None
        except Exception:  # noqa: BLE001
            observed_title = None
        try:
            state = page.evaluate(
                """() => {
                    const body = document.body;
                    return {
                        readyState: document.readyState || null,
                        bodyTextLength: body ? ((body.innerText || '').trim().length) : 0,
                        bodyTextExcerpt: body ? ((body.innerText || '').trim().slice(0, 600)) : '',
                        elementCount: body ? body.querySelectorAll('*').length : 0,
                    };
                }"""
            ) or {}
            ready_state = safe_text(state.get("readyState")).strip() or None
            body_text_length = maybe_int(state.get("bodyTextLength")) or 0
            body_text_excerpt = text_excerpt(state.get("bodyTextExcerpt"))
            element_count = maybe_int(state.get("elementCount")) or 0
        except Exception:  # noqa: BLE001
            ready_state = None
        return {
            "final_url": observed_url,
            "page_title": observed_title,
            "ready_state": ready_state,
            "body_text_length": body_text_length,
            "body_text_excerpt": body_text_excerpt,
            "element_count": element_count,
        }

    @classmethod
    def browser_state_is_rendered_enough(cls, state: Dict[str, Any]) -> bool:
        final_url = safe_text(state.get("final_url")).strip()
        if final_url and final_url != "about:blank":
            if safe_text(state.get("page_title")).strip():
                return True
            if (maybe_int(state.get("body_text_length")) or 0) >= 20:
                return True
            if (maybe_int(state.get("element_count")) or 0) >= 10:
                return True
            if safe_text(state.get("ready_state")).strip().lower() in {"interactive", "complete"}:
                return True
        return False

    def browser_capture_attempt(
        self,
        target_url: str,
        screenshot_path: Path,
        *,
        wait_until: str,
        navigation_timeout_ms: int,
        render_grace_ms: int,
        network_idle_timeout_ms: int = 0,
    ) -> Dict[str, Any]:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import sync_playwright

        navigation_error: Optional[str] = None
        capture_error: Optional[str] = None
        observed_state: Dict[str, Any] = {
            "final_url": None,
            "page_title": None,
            "ready_state": None,
            "body_text_length": 0,
            "element_count": 0,
        }
        navigation_completed = False
        screenshot_captured = False

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={
                    "width": INSPECT_SCREENSHOT_VIEWPORT_WIDTH,
                    "height": INSPECT_SCREENSHOT_VIEWPORT_HEIGHT,
                },
                ignore_https_errors=True,
            )
            page = context.new_page()
            try:
                try:
                    page.goto(target_url, wait_until=wait_until, timeout=navigation_timeout_ms)
                    navigation_completed = True
                except Exception as exc:  # noqa: BLE001
                    navigation_error = f"{exc.__class__.__name__}: {exc}"
                if navigation_completed and network_idle_timeout_ms > 0:
                    try:
                        page.wait_for_load_state("networkidle", timeout=network_idle_timeout_ms)
                    except PlaywrightError:
                        pass
                if render_grace_ms > 0:
                    try:
                        page.wait_for_timeout(render_grace_ms)
                    except Exception:  # noqa: BLE001
                        pass
                observed_state = self.browser_state_from_page(page)
                rendered_enough = self.browser_state_is_rendered_enough(observed_state)
                if navigation_completed or rendered_enough:
                    try:
                        page.screenshot(path=str(screenshot_path), full_page=False)
                        screenshot_captured = True
                    except Exception as exc:  # noqa: BLE001
                        capture_error = f"{exc.__class__.__name__}: {exc}"
                else:
                    capture_error = None
            finally:
                context.close()
                browser.close()

        return {
            "navigation_completed": navigation_completed,
            "rendered_enough": self.browser_state_is_rendered_enough(observed_state),
            "screenshot_captured": screenshot_captured,
            "capture_error": capture_error,
            "navigation_error": navigation_error,
            **observed_state,
        }

    @staticmethod
    def best_browser_observation(*attempts: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        final_url: Optional[str] = None
        page_title: Optional[str] = None
        body_text_excerpt: Optional[str] = None
        for attempt in attempts:
            candidate_url = safe_text(attempt.get("final_url")).strip() or None
            candidate_title = safe_text(attempt.get("page_title")).strip() or None
            candidate_excerpt = text_excerpt(attempt.get("body_text_excerpt"))
            if candidate_url and not final_url:
                final_url = candidate_url
            if candidate_title and not page_title:
                page_title = candidate_title
            if candidate_excerpt and not body_text_excerpt:
                body_text_excerpt = candidate_excerpt
        return final_url, page_title, body_text_excerpt

    @staticmethod
    def combine_browser_attempt_reasons(*reasons: Optional[str]) -> str:
        unique = [reason for reason in reasons if reason]
        if not unique:
            return "browser navigation did not reach a renderable page"
        seen: List[str] = []
        for reason in unique:
            if reason not in seen:
                seen.append(reason)
        return " | ".join(seen)

    def browser_capture_support_status(self) -> Tuple[bool, Optional[str]]:
        try:
            import playwright.sync_api  # noqa: F401
            return True, None
        except Exception as exc:  # noqa: BLE001
            return False, f"Playwright import failed: {exc.__class__.__name__}: {exc}"

    def capture_inspect_screenshot(self, target_url: str) -> Dict[str, Any]:
        browser_capture_supported, browser_capture_reason = self.browser_capture_support_status()
        if not browser_capture_supported:
            return {
                "status": "unavailable",
                "reason": browser_capture_reason,
                "path": None,
                "final_url": None,
                "page_title": None,
                "body_text_excerpt": None,
            }

        screenshot_dir = self.make_temp_media_dir(prefix="inspect-")
        screenshot_path = screenshot_dir / f"inspect-{sha1_digest([target_url])[:12]}.png"
        try:
            primary_attempt = self.browser_capture_attempt(
                target_url,
                screenshot_path,
                wait_until="domcontentloaded",
                navigation_timeout_ms=INSPECT_SCREENSHOT_TIMEOUT_MS,
                render_grace_ms=0,
                network_idle_timeout_ms=INSPECT_SCREENSHOT_NETWORK_IDLE_TIMEOUT_MS,
            )
            if primary_attempt["screenshot_captured"]:
                return {
                    "status": "captured" if primary_attempt["navigation_completed"] else "partial",
                    "reason": None if primary_attempt["navigation_completed"] else (
                        primary_attempt["navigation_error"] or "partial browser render captured after incomplete navigation"
                    ),
                    "path": str(screenshot_path),
                    "final_url": primary_attempt.get("final_url"),
                    "page_title": primary_attempt.get("page_title"),
                    "body_text_excerpt": primary_attempt.get("body_text_excerpt"),
                }
            if primary_attempt["capture_error"] and (primary_attempt["navigation_completed"] or primary_attempt["rendered_enough"]):
                if screenshot_path.exists():
                    screenshot_path.unlink()
                shutil.rmtree(screenshot_dir, ignore_errors=True)
                return {
                    "status": "failed_capture",
                    "reason": self.combine_browser_attempt_reasons(
                        primary_attempt.get("navigation_error"),
                        primary_attempt.get("capture_error"),
                    ),
                    "path": None,
                    "final_url": primary_attempt.get("final_url"),
                    "page_title": primary_attempt.get("page_title"),
                    "body_text_excerpt": primary_attempt.get("body_text_excerpt"),
                }

            fallback_attempt = self.browser_capture_attempt(
                target_url,
                screenshot_path,
                wait_until="commit",
                navigation_timeout_ms=10000,
                render_grace_ms=1500,
            )
            observed_url, observed_title, observed_body_excerpt = self.best_browser_observation(primary_attempt, fallback_attempt)
            if fallback_attempt["screenshot_captured"]:
                return {
                    "status": "partial",
                    "reason": self.combine_browser_attempt_reasons(
                        primary_attempt.get("navigation_error"),
                        fallback_attempt.get("navigation_error"),
                    ),
                    "path": str(screenshot_path),
                    "final_url": fallback_attempt.get("final_url") or observed_url,
                    "page_title": fallback_attempt.get("page_title") or observed_title,
                    "body_text_excerpt": fallback_attempt.get("body_text_excerpt") or observed_body_excerpt,
                }
            if fallback_attempt["capture_error"] and (fallback_attempt["navigation_completed"] or fallback_attempt["rendered_enough"]):
                if screenshot_path.exists():
                    screenshot_path.unlink()
                shutil.rmtree(screenshot_dir, ignore_errors=True)
                return {
                    "status": "failed_capture",
                    "reason": self.combine_browser_attempt_reasons(
                        primary_attempt.get("navigation_error"),
                        fallback_attempt.get("navigation_error"),
                        fallback_attempt.get("capture_error"),
                    ),
                    "path": None,
                    "final_url": observed_url,
                    "page_title": observed_title,
                    "body_text_excerpt": observed_body_excerpt,
                }
            if screenshot_path.exists():
                screenshot_path.unlink()
            shutil.rmtree(screenshot_dir, ignore_errors=True)
            return {
                "status": "failed_navigation",
                "reason": self.combine_browser_attempt_reasons(
                    primary_attempt.get("navigation_error"),
                    fallback_attempt.get("navigation_error"),
                ),
                "path": None,
                "final_url": observed_url,
                "page_title": observed_title,
                "body_text_excerpt": observed_body_excerpt,
            }
        except Exception as exc:  # noqa: BLE001
            if screenshot_path.exists():
                screenshot_path.unlink()
            shutil.rmtree(screenshot_dir, ignore_errors=True)
            return {
                "status": "failed_navigation",
                "reason": f"{exc.__class__.__name__}: {exc}",
                "path": None,
                "final_url": None,
                "page_title": None,
                "body_text_excerpt": None,
            }

    @staticmethod
    def redirect_chain_summary(hops: Sequence[Dict[str, Any]]) -> str:
        if not hops:
            return "unavailable"
        summary_parts = []
        for hop in hops:
            status = safe_text(hop.get("status")).strip() or "?"
            target = safe_text(hop.get("location") or hop.get("meta_refresh_location") or hop.get("url")).strip()
            summary_parts.append(f"{status} -> {target}")
        return " | ".join(summary_parts)

    @staticmethod
    def normalize_inspect_compare_key(url: Optional[str]) -> Optional[Tuple[str, str, str]]:
        target = unwrap_meta_redirect(url)
        if not target:
            return None
        parsed = urllib.parse.urlparse(target)
        scheme = (parsed.scheme or "https").lower()
        netloc = (parsed.netloc or "").lower()
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/")
        return scheme, netloc, path

    @staticmethod
    def domain_relationship(source_host: Optional[str], target_host: Optional[str]) -> str:
        source = normalize_domain_text(source_host)
        target = normalize_domain_text(target_host)
        if not source or not target:
            return "unavailable"
        if source == target:
            return "same_host"
        if registrable_domain(source) and registrable_domain(source) == registrable_domain(target):
            return "same_registrable_domain_different_subdomain"
        return "cross_registrable_domain"

    @staticmethod
    def describe_domain_relationship(relationship: str) -> str:
        return {
            "same_host": "same host",
            "same_registrable_domain_different_subdomain": "same registrable domain, different subdomain",
            "cross_registrable_domain": "cross-registrable-domain",
            "unavailable": "unavailable",
        }.get(relationship, "unavailable")

    @classmethod
    def classify_pivot_domain_context(
        cls,
        *,
        pivot_domain: Optional[str],
        observed_hosts: Sequence[Optional[str]],
        bound_page_id: Optional[str],
    ) -> Tuple[str, str]:
        normalized_pivot = normalize_domain_text(pivot_domain)
        if not normalized_pivot or not safe_text(bound_page_id).strip():
            return (
                "unavailable",
                "unavailable - inspect was not grounded in a bound card/result",
            )

        relationships = [
            cls.domain_relationship(normalized_pivot, host)
            for host in ordered_unique_domains(observed_hosts)
        ]
        if not relationships:
            return (
                "unavailable",
                f"unavailable - suggested pivot domain {normalized_pivot} had no observed landing/final/browser hosts to compare",
            )

        related = {"same_host", "same_registrable_domain_different_subdomain"}
        if all(item in related for item in relationships):
            return (
                "tight",
                f"tight - suggested pivot domain {normalized_pivot} stays aligned with every observed landing/final/browser endpoint",
            )
        if any(item in related for item in relationships):
            return (
                "loose",
                f"loose - suggested pivot domain {normalized_pivot} matches some observed landing/final/browser endpoints but not all",
            )
        return (
            "cross-domain",
            f"cross-domain - suggested pivot domain {normalized_pivot} does not match the observed landing/final/browser registrable domains",
        )

    @classmethod
    def build_relationship_summary(
        cls,
        *,
        landing_host: Optional[str],
        final_host: Optional[str],
        browser_host: Optional[str],
        landing_to_final_relationship: str,
        delivery_divergence_hint: str,
        pivot_domain: Optional[str],
        pivot_domain_context: str,
        fetch_error: Optional[str],
    ) -> str:
        landing_label = landing_host or "unavailable"
        final_label = final_host or "unavailable"
        browser_label = browser_host or "unavailable"
        pivot_label = normalize_domain_text(pivot_domain) or "unavailable"

        if fetch_error:
            landing_clause = f"direct fetch did not resolve beyond {landing_label}"
        elif landing_to_final_relationship == "same_host":
            landing_clause = f"landing stayed on {final_label}"
        elif landing_to_final_relationship == "same_registrable_domain_different_subdomain":
            landing_clause = f"landing moved from {landing_label} to {final_label} within the same registrable domain"
        elif landing_to_final_relationship == "cross_registrable_domain":
            landing_clause = f"landing moved from {landing_label} to {final_label} across registrable domains"
        else:
            landing_clause = "landing-to-final relationship unavailable"

        if delivery_divergence_hint == "none":
            browser_clause = f"browser and direct fetch agree on {browser_label}"
        elif delivery_divergence_hint == "browser_url_differs_same_host":
            browser_clause = f"browser differs from direct fetch on the same host {browser_label}"
        elif delivery_divergence_hint == "browser_host_differs_same_registrable_domain":
            browser_clause = f"browser shifts to related host {browser_label} on the same registrable domain"
        elif delivery_divergence_hint == "browser_host_differs_cross_registrable_domain":
            browser_clause = f"browser diverges to {browser_label} across registrable domains"
        elif delivery_divergence_hint == "direct_fetch_failed_browser_loaded":
            browser_clause = f"browser still loaded {browser_label} after direct fetch failed"
        elif delivery_divergence_hint == "direct_final_unavailable_browser_loaded":
            browser_clause = f"browser still loaded {browser_label} while the direct final URL stayed unavailable"
        else:
            browser_clause = "browser comparison unavailable"

        if pivot_domain_context == "tight":
            pivot_clause = f"pivot {pivot_label} is tight with the current card"
        elif pivot_domain_context == "loose":
            pivot_clause = f"pivot {pivot_label} is loose with the current card"
        elif pivot_domain_context == "cross-domain":
            pivot_clause = f"pivot {pivot_label} is cross-domain versus the observed landing flow"
        else:
            pivot_clause = "no bound card context for pivot comparison"

        return "; ".join([landing_clause, browser_clause, pivot_clause])

    @classmethod
    def classify_redirect_interpretation(
        cls,
        *,
        landing_url: Optional[str],
        final_url: Optional[str],
        hops: Sequence[Dict[str, Any]],
        notes: Sequence[str],
        fetch_error: Optional[str],
        content_type: Optional[str],
    ) -> str:
        if fetch_error:
            return "direct fetch unavailable"

        if any("redirect loop detected" in note for note in notes):
            return "redirect loop detected"
        if any(note.startswith("redirect chain exceeded ") for note in notes):
            return f"redirect chain exceeded {REDIRECT_MAX_HOPS} hops"
        if not hops:
            return "unavailable"

        landing_host = extract_domain(landing_url)
        final_host = extract_domain(final_url)
        relationship = cls.domain_relationship(landing_host, final_host)
        has_meta_refresh = any(bool(hop.get("meta_refresh_location")) for hop in hops)
        landing_key = cls.normalize_inspect_compare_key(landing_url)
        final_key = cls.normalize_inspect_compare_key(final_url)

        if content_type and not content_type.lower().startswith(("text/html", "application/xhtml+xml", "text/plain")):
            if relationship == "cross_registrable_domain":
                return "cross-registrable-domain redirect to non-html response"
            if relationship == "same_registrable_domain_different_subdomain":
                return "same-registrable-domain redirect to non-html response"
            if landing_key and final_key and landing_key != final_key:
                return "same-host redirect to non-html response"
            return "terminal non-html response"

        if has_meta_refresh:
            if relationship == "cross_registrable_domain":
                return "cross-registrable-domain meta refresh"
            if relationship == "same_registrable_domain_different_subdomain":
                return "same-registrable-domain meta refresh"
            return "same-host meta refresh"

        if relationship == "cross_registrable_domain":
            return "cross-registrable-domain redirect"
        if relationship == "same_registrable_domain_different_subdomain":
            return "same-registrable-domain redirect"
        if landing_key and final_key and landing_key != final_key:
            return "same-host redirect"
        return "direct load"

    @classmethod
    def classify_delivery_divergence(
        cls,
        *,
        final_url: Optional[str],
        browser_final_url: Optional[str],
        fetch_error: Optional[str],
    ) -> Tuple[str, str]:
        if fetch_error and browser_final_url:
            return (
                "direct_fetch_failed_browser_loaded",
                "direct fetch failed but browser render still reached a page; hint only, not cloak detection",
            )
        if not browser_final_url:
            return (
                "browser_unavailable",
                "browser comparison unavailable in this inspect run",
            )
        if not final_url:
            return (
                "direct_final_unavailable_browser_loaded",
                "direct final URL was unavailable while browser render still reached a page; hint only, not cloak detection",
            )

        final_host = extract_domain(final_url)
        browser_host = extract_domain(browser_final_url)
        relationship = cls.domain_relationship(final_host, browser_host)
        if relationship == "cross_registrable_domain":
            return (
                "browser_host_differs_cross_registrable_domain",
                (
                    f"browser render ended on host {browser_host or 'unavailable'} while direct fetch ended on "
                    f"{final_host or 'unavailable'} across registrable domains; stronger divergence hint only, not cloak detection"
                ),
            )
        if relationship == "same_registrable_domain_different_subdomain":
            return (
                "browser_host_differs_same_registrable_domain",
                (
                    f"browser render ended on related host {browser_host or 'unavailable'} while direct fetch ended on "
                    f"{final_host or 'unavailable'} within the same registrable domain; often routing noise, hint only, not cloak detection"
                ),
            )
        if cls.normalize_inspect_compare_key(final_url) != cls.normalize_inspect_compare_key(browser_final_url):
            return (
                "browser_url_differs_same_host",
                "browser render ended on a different same-host path than direct fetch; often lower-signal routing noise, hint only, not cloak detection",
            )
        return (
            "none",
            "none observed between direct fetch and browser render",
        )

    @staticmethod
    def preferred_screenshot_signal_title(*candidates: Optional[str]) -> Optional[str]:
        for candidate in candidates:
            cleaned = text_excerpt(candidate, limit=100)
            if cleaned:
                return cleaned
        return None

    @classmethod
    def classify_screenshot_assessment(
        cls,
        *,
        final_status: Optional[int],
        final_url: Optional[str],
        browser_final_url: Optional[str],
        landing_page_title: Optional[str],
        browser_page_title: Optional[str],
        direct_body_excerpt: Optional[str],
        browser_body_excerpt: Optional[str],
    ) -> Tuple[str, Optional[str]]:
        title = cls.preferred_screenshot_signal_title(browser_page_title, landing_page_title)
        title_suffix = f' ("{title}")' if title else ""
        signal_text = " ".join(
            filter(
                None,
                [
                    safe_text(browser_page_title).lower(),
                    safe_text(landing_page_title).lower(),
                    safe_text(browser_body_excerpt).lower(),
                    safe_text(direct_body_excerpt).lower(),
                ],
            )
        )
        signal_urls = " ".join(filter(None, [safe_text(browser_final_url).lower(), safe_text(final_url).lower()]))

        challenge_text_markers = (
            "just a moment",
            "checking your browser",
            "verify you are human",
            "verify you are a human",
            "are you human",
            "security check",
            "captcha",
        )
        challenge_url_markers = ("cf-challenge", "__cf_chl", "challenge-platform", "captcha", "challenge=")
        blocked_text_markers = (
            "enable cookies",
            "access denied",
            "request blocked",
            "blocked request",
            "unsupported browser",
            "browser not supported",
            "forbidden",
        )
        error_text_markers = (
            "page not found",
            "something went wrong",
            "temporarily unavailable",
            "bad gateway",
            "service unavailable",
            "internal server error",
            "application error",
        )

        if any(marker in signal_urls for marker in challenge_url_markers) or any(
            marker in signal_text for marker in challenge_text_markers
        ):
            return (
                SCREENSHOT_ASSESSMENT_CHALLENGE,
                f"looks like a challenge or verification page{title_suffix}, not a normal landing-page capture",
            )

        if any(marker in signal_text for marker in blocked_text_markers):
            blocked_prefix = (
                "looks like a blocked or cookie-wall page"
                if "enable cookies" in signal_text
                else "looks like a blocked page"
            )
            return (
                SCREENSHOT_ASSESSMENT_BLOCKED,
                f"{blocked_prefix}{title_suffix}, not a normal landing-page capture",
            )

        if final_status is not None and final_status >= 400:
            return (
                SCREENSHOT_ASSESSMENT_ERROR_PAGE,
                f"error page (HTTP {final_status}){title_suffix}, not a normal landing-page capture",
            )

        if any(marker in signal_text for marker in error_text_markers):
            return (
                SCREENSHOT_ASSESSMENT_ERROR_PAGE,
                f"looks like an error page{title_suffix}, not a normal landing-page capture",
            )

        return SCREENSHOT_ASSESSMENT_NORMAL, None

    def direct_fetch_inspect_data(
        self,
        landing_url: str,
        *,
        timeout_sec: Optional[int] = None,
    ) -> Dict[str, Any]:
        current_url = landing_url
        final_url: Optional[str] = None
        final_status: Optional[int] = None
        final_headers: Dict[str, str] = {}
        final_body_text: Optional[str] = None
        fetch_error: Optional[str] = None
        notes: List[str] = []
        hops: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()

        for _ in range(REDIRECT_MAX_HOPS):
            seen_urls.add(current_url)
            try:
                status, body, headers = self._request(
                    "GET",
                    current_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    request_kind="inspect_fetch",
                    opener=self.direct_no_redirect_opener(),
                    timeout_sec=timeout_sec,
                )
            except AcquisitionError as exc:
                fetch_error = str(exc)
                if not notes:
                    notes.append("redirect resolution unavailable")
                break

            content_type = safe_text(headers.get("Content-Type") or headers.get("content-type")).split(";", 1)[0].strip() or None
            location = safe_text(headers.get("Location") or headers.get("location")).strip() or None
            body_text = None
            if body and content_type and content_type.lower().startswith(("text/html", "application/xhtml+xml", "text/plain")):
                body_text = body.decode("utf-8", "replace")

            hop: Dict[str, Any] = {
                "url": current_url,
                "status": status,
                "content_type": content_type,
            }

            if status in {301, 302, 303, 307, 308} and location:
                next_url = urllib.parse.urljoin(current_url, location)
                hop["location"] = next_url
                hops.append(hop)
                if next_url in seen_urls:
                    final_url = next_url
                    notes.append("redirect loop detected")
                    break
                current_url = next_url
                continue

            if body_text:
                meta_refresh_location = self.extract_meta_refresh_target(body_text, current_url)
                if meta_refresh_location:
                    hop["meta_refresh_location"] = meta_refresh_location
                    hops.append(hop)
                    notes.append("meta refresh detected")
                    if meta_refresh_location in seen_urls:
                        final_url = meta_refresh_location
                        final_status = status
                        final_headers = headers
                        final_body_text = body_text
                        notes.append("redirect loop detected")
                        break
                    current_url = meta_refresh_location
                    continue

            hops.append(hop)
            final_url = current_url
            final_status = status
            final_headers = headers
            final_body_text = body_text
            break
        else:
            final_url = current_url
            notes.append(f"redirect chain exceeded {REDIRECT_MAX_HOPS} hops")

        landing_title = self.extract_html_title(final_body_text or "")
        technology_hints, tracker_hints, detection_notes = self.detect_page_hints(final_body_text, final_headers)
        notes.extend(note for note in detection_notes if note not in notes)
        content_type = safe_text(final_headers.get("Content-Type") or final_headers.get("content-type")).split(";", 1)[0].strip() or None
        landing_host = extract_domain(landing_url)
        final_host = extract_domain(final_url)
        landing_registrable_domain = registrable_domain(landing_host)
        final_registrable_domain = registrable_domain(final_host)
        redirect_interpretation = self.classify_redirect_interpretation(
            landing_url=landing_url,
            final_url=final_url,
            hops=hops,
            notes=notes,
            fetch_error=fetch_error,
            content_type=content_type,
        )
        return {
            "landing_url": landing_url,
            "landing_host": landing_host,
            "landing_registrable_domain": landing_registrable_domain,
            "final_url": final_url,
            "final_host": final_host,
            "final_registrable_domain": final_registrable_domain,
            "final_status": final_status,
            "content_type": content_type,
            "landing_page_title": landing_title,
            "body_text_excerpt": text_excerpt(final_body_text),
            "redirect_chain": hops,
            "redirect_chain_summary": self.redirect_chain_summary(hops),
            "technology_hints": technology_hints,
            "tracker_hints": tracker_hints,
            "redirect_interpretation": redirect_interpretation,
            "notes": notes,
            "fetch_error": fetch_error,
        }

    def inspect_stack_target(
        self,
        landing_url: str,
        *,
        timeout_sec: int = STACK_HINT_REQUEST_TIMEOUT_SEC,
    ) -> Dict[str, Any]:
        report = self.direct_fetch_inspect_data(landing_url, timeout_sec=timeout_sec)
        report["stack_family"] = self.stack_family_signature(
            trackers=report.get("tracker_hints") or [],
            technologies=report.get("technology_hints") or [],
        )
        return report

    def inspect_delivery_target(
        self,
        landing_url: str,
        *,
        timeout_sec: int = STACK_HINT_REQUEST_TIMEOUT_SEC,
    ) -> Dict[str, Any]:
        report = self.direct_fetch_inspect_data(landing_url, timeout_sec=timeout_sec)
        report["delivery_family"] = self.delivery_family_signature(
            final_url=report.get("final_url"),
            final_host=report.get("final_host"),
        )
        report["redirect_family"] = self.redirect_family_signature(
            redirect_interpretation=report.get("redirect_interpretation"),
            hops=report.get("redirect_chain") or [],
            delivery_family=report.get("delivery_family"),
        )
        return report

    def inspect_landing_url(
        self,
        landing_url: str,
        *,
        pivot_domain: Optional[str] = None,
        bound_page_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        direct_report = self.direct_fetch_inspect_data(landing_url)
        final_url = direct_report.get("final_url")
        final_status = direct_report.get("final_status")
        content_type = direct_report.get("content_type")
        fetch_error = direct_report.get("fetch_error")
        browser_capture_target = final_url if final_url and final_status is not None and final_status < 400 else landing_url
        screenshot_capture = (
            self.capture_inspect_screenshot(browser_capture_target)
            if browser_capture_target
            else {
                "status": "unavailable",
                "reason": "Landing URL was unavailable for browser capture.",
                "path": None,
                "final_url": None,
                "page_title": None,
                "body_text_excerpt": None,
            }
        )
        landing_host = direct_report.get("landing_host")
        final_host = direct_report.get("final_host")
        browser_final_url = safe_text(screenshot_capture.get("final_url")).strip() or None
        browser_final_host = extract_domain(browser_final_url)
        landing_registrable_domain = direct_report.get("landing_registrable_domain")
        final_registrable_domain = direct_report.get("final_registrable_domain")
        browser_final_registrable_domain = registrable_domain(browser_final_host)
        suggested_pivot_domain = normalize_domain_text(pivot_domain)
        pivot_registrable_domain = registrable_domain(suggested_pivot_domain)
        landing_to_final_relationship = self.domain_relationship(landing_host, final_host)
        direct_to_browser_relationship = self.domain_relationship(final_host, browser_final_host)
        delivery_divergence_hint, delivery_divergence_note = self.classify_delivery_divergence(
            final_url=final_url,
            browser_final_url=browser_final_url,
            fetch_error=fetch_error,
        )
        pivot_domain_context, pivot_domain_context_note = self.classify_pivot_domain_context(
            pivot_domain=suggested_pivot_domain,
            observed_hosts=[landing_host, final_host, browser_final_host],
            bound_page_id=bound_page_id,
        )
        relationship_summary = self.build_relationship_summary(
            landing_host=landing_host,
            final_host=final_host,
            browser_host=browser_final_host,
            landing_to_final_relationship=landing_to_final_relationship,
            delivery_divergence_hint=delivery_divergence_hint,
            pivot_domain=suggested_pivot_domain,
            pivot_domain_context=pivot_domain_context,
            fetch_error=fetch_error,
        )
        screenshot_assessment_kind, screenshot_assessment_note = self.classify_screenshot_assessment(
            final_status=final_status,
            final_url=final_url,
            browser_final_url=browser_final_url,
            landing_page_title=direct_report.get("landing_page_title"),
            browser_page_title=screenshot_capture.get("page_title"),
            direct_body_excerpt=direct_report.get("body_text_excerpt"),
            browser_body_excerpt=screenshot_capture.get("body_text_excerpt"),
        )
        return {
            "landing_url": landing_url,
            "landing_host": landing_host,
            "landing_registrable_domain": landing_registrable_domain,
            "final_url": final_url,
            "final_host": final_host,
            "final_registrable_domain": final_registrable_domain,
            "final_status": final_status,
            "content_type": content_type,
            "landing_page_title": direct_report.get("landing_page_title"),
            "redirect_chain": direct_report.get("redirect_chain") or [],
            "redirect_chain_summary": direct_report.get("redirect_chain_summary") or "unavailable",
            "browser_final_url": browser_final_url,
            "browser_final_host": browser_final_host,
            "browser_final_registrable_domain": browser_final_registrable_domain,
            "browser_page_title": screenshot_capture.get("page_title"),
            "suggested_pivot_domain": suggested_pivot_domain,
            "pivot_registrable_domain": pivot_registrable_domain,
            "landing_to_final_relationship": self.describe_domain_relationship(landing_to_final_relationship),
            "direct_to_browser_relationship": self.describe_domain_relationship(direct_to_browser_relationship),
            "pivot_domain_context": pivot_domain_context,
            "pivot_domain_context_note": pivot_domain_context_note,
            "relationship_summary": relationship_summary,
            "redirect_interpretation": direct_report.get("redirect_interpretation") or "unavailable",
            "delivery_divergence_hint": delivery_divergence_hint,
            "delivery_divergence_note": delivery_divergence_note,
            "technology_hints": direct_report.get("technology_hints") or [],
            "tracker_hints": direct_report.get("tracker_hints") or [],
            "screenshot_status": screenshot_capture["status"],
            "screenshot_reason": screenshot_capture["reason"],
            "screenshot_path": screenshot_capture["path"],
            "screenshot_assessment_kind": screenshot_assessment_kind,
            "screenshot_assessment_note": screenshot_assessment_note,
            "notes": direct_report.get("notes") or [],
            "fetch_error": fetch_error,
        }

    @staticmethod
    def inspect_media_assessment_label(report: Dict[str, Any]) -> Optional[str]:
        assessment_kind = safe_text(report.get("screenshot_assessment_kind")).strip().lower()
        if assessment_kind == SCREENSHOT_ASSESSMENT_ERROR_PAGE:
            return "Error page evidence"
        if assessment_kind in {SCREENSHOT_ASSESSMENT_BLOCKED, SCREENSHOT_ASSESSMENT_CHALLENGE}:
            return "Blocked/challenge page evidence"
        return None

    @classmethod
    def inspect_bucket_context_lines(cls, report: Dict[str, Any]) -> List[str]:
        bucket_lines = [
            ("Pivot bucket LP summary", report.get("pivot_bucket_note")),
            ("Bucket overlap hints", report.get("pivot_bucket_overlap_note")),
            ("Bucket stack hints", report.get("pivot_bucket_stack_note")),
            ("Bucket delivery hints", report.get("pivot_bucket_delivery_note")),
            ("Bucket redirect hints", report.get("pivot_bucket_redirect_note")),
            ("Current card vs bucket", report.get("current_bucket_note")),
            ("Current card vs overlap family", report.get("current_overlap_note")),
            ("Current card vs stack family", report.get("current_stack_note")),
            ("Current card vs delivery family", report.get("current_delivery_note")),
            ("Current card vs redirect family", report.get("current_redirect_note")),
        ]
        notes = [safe_text(note).strip() or "unavailable" for _label, note in bucket_lines]
        if notes and all(note == INSPECT_NON_PIVOT_BUCKET_NOTE for note in notes):
            return [f"Pivot bucket context: {INSPECT_NON_PIVOT_BUCKET_CONTEXT_LINE}"]
        return [
            f"{label}: {truncate_text(safe_text(note).strip() or 'unavailable', 220)}"
            for label, note in bucket_lines
        ]

    @staticmethod
    def format_inspect_screenshot_lines(report: Dict[str, Any]) -> List[str]:
        screenshot_status = safe_text(report.get("screenshot_status")).strip() or "unavailable"
        if screenshot_status == "captured":
            lines = ["Screenshot: captured"]
        else:
            lines = [f"Screenshot: {screenshot_status} ({report.get('screenshot_reason') or 'unavailable'})"]
        assessment_note = safe_text(report.get("screenshot_assessment_note")).strip()
        if assessment_note:
            lines.append(f"Screenshot assessment: {assessment_note}")
        return lines

    def format_inspect_media_caption(self, advertiser_label: str, report: Dict[str, Any]) -> str:
        lines = [truncate_text(advertiser_label, 80)]
        assessment_label = self.inspect_media_assessment_label(report)
        if assessment_label:
            lines.append(assessment_label)
        title = truncate_text(report.get("landing_page_title") or report.get("final_url") or report.get("landing_url"), 160)
        if title:
            lines.append(title)
        return truncate_text("\n".join(lines).strip(), MAX_MEDIA_CAPTION_LEN)

    @staticmethod
    def join_report_lines(lines: Sequence[str]) -> str:
        return "\n".join(line for line in lines if safe_text(line).strip()).strip()

    def format_inspect_report_text(self, advertiser_label: str, report: Dict[str, Any]) -> str:
        prefix_lines = [
            f"Funnel inspect: {advertiser_label}",
            f"Landing URL: {report.get('landing_url') or 'unavailable'}",
            f"Landing host: {report.get('landing_host') or 'unavailable'}",
            f"Final URL (off-browser-replayed): {report.get('final_url') or 'unavailable'}",
            f"Final host (off-browser-replayed): {report.get('final_host') or 'unavailable'}",
            f"Browser URL (browser-observed): {report.get('browser_final_url') or 'unavailable'}",
            f"Browser host (browser-observed): {report.get('browser_final_host') or 'unavailable'}",
            f"Relationship summary: {report.get('relationship_summary') or 'unavailable'}",
            f"Landing -> final relationship: {report.get('landing_to_final_relationship') or 'unavailable'}",
            f"Direct -> browser relationship: {report.get('direct_to_browser_relationship') or 'unavailable'}",
            f"Pivot/domain context: {report.get('pivot_domain_context_note') or 'unavailable'}",
        ]
        prefix_lines.extend(self.format_inspect_screenshot_lines(report))
        prefix_lines.extend(self.inspect_bucket_context_lines(report))
        prefix_lines.extend(
            [
                f"Redirect interpretation: {report.get('redirect_interpretation') or 'unavailable'}",
                f"Delivery divergence hint: {report.get('delivery_divergence_note') or 'unavailable'}",
                f"Final status (off-browser-replayed): {report.get('final_status') if report.get('final_status') is not None else 'unavailable'}",
                f"Content type (off-browser-replayed): {report.get('content_type') or 'unavailable'}",
                f"Landing page title: {report.get('landing_page_title') or 'unavailable'}",
            ]
        )
        if report.get("fetch_error"):
            prefix_lines.append(f"Inspect diagnostic: {report['fetch_error']}")

        redirect_lines = ["Redirect chain:"]
        if report.get("redirect_chain"):
            for hop in report["redirect_chain"]:
                target = safe_text(hop.get("location") or hop.get("meta_refresh_location") or hop.get("url")).strip() or "unavailable"
                redirect_lines.append(f"- {hop.get('status', 'unavailable')} -> {target}")
        else:
            redirect_lines.append("- unavailable")

        optional_tail_lines = [
            "Technology hints (off-browser-replayed): "
            + (", ".join(report.get("technology_hints") or []) if report.get("technology_hints") else "unavailable")
        ]
        optional_tail_lines.append(
            "Tracker hints (off-browser-replayed): "
            + (", ".join(report.get("tracker_hints") or []) if report.get("tracker_hints") else "unavailable")
        )

        notes = list(report.get("notes") or [])
        if notes:
            optional_tail_lines.append("Notes:")
            for note in notes[:6]:
                optional_tail_lines.append(f"- {note}")

        trim_notice = "Inspect tail trimmed due to Telegram message length."
        full_lines = [*prefix_lines, *redirect_lines, *optional_tail_lines]
        body = self.join_report_lines(full_lines)
        if len(body) <= MAX_CHAT_MESSAGE_LEN:
            return body

        compact_tail = self.join_report_lines([*prefix_lines, *redirect_lines, trim_notice])
        if len(compact_tail) <= MAX_CHAT_MESSAGE_LEN:
            return compact_tail

        compact_prefix = self.join_report_lines([*prefix_lines, trim_notice])
        if len(compact_prefix) <= MAX_CHAT_MESSAGE_LEN:
            return compact_prefix

        reserved = len(trim_notice) + 1
        trimmed_prefix = truncate_text(
            self.join_report_lines(prefix_lines),
            max(1, MAX_CHAT_MESSAGE_LEN - reserved),
        )
        return self.join_report_lines([trimmed_prefix, trim_notice])

    def inspect_message_payload(self, advertiser_label: str, report: Dict[str, Any], message_text: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "text": message_text,
            "message_kind": "inspect_report",
            "disable_web_page_preview": True,
            "media_present_in_payload": False,
            "media_outcome": "no_media_in_payload",
            "text_fallback_used": False,
            "native_media_sent": False,
        }
        screenshot_path = safe_text(report.get("screenshot_path")).strip()
        if report.get("screenshot_status") in {"captured", "partial"} and screenshot_path:
            payload.update(
                {
                    "media_present_in_payload": True,
                    "media_kind": "photo",
                    "media_path": screenshot_path,
                    "media_caption": self.format_inspect_media_caption(advertiser_label, report),
                    "native_media_text": message_text,
                    "media_outcome": None,
                }
            )
        return payload

    def run_explicit_pivot(
        self,
        pivot_type: str,
        *,
        chat_id: str,
        user_id: str,
        pivot_query: str,
        date_from: Optional[str],
        date_to: Optional[str],
        geo: str = "US",
        source: str = "typed_command",
        exact_page_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        resolved_pivot_type = "page" if pivot_type == "advertiser" and exact_page_id else pivot_type
        page_ids = [exact_page_id] if exact_page_id else []
        search_keyword = "" if exact_page_id else pivot_query
        query_chain = [pivot_query]
        resolved_query = pivot_query
        fallback_used = False
        if resolved_pivot_type == "page":
            note = f'Page pivot: searching exact Facebook Ads Library page_id "{exact_page_id}" in USA.'
        elif pivot_type == "advertiser":
            note = f'Advertiser pivot: searching Facebook Ads Library by page label "{pivot_query}" in USA. This is a keyword pivot, not an exact page-id filter.'
        elif pivot_type == "domain":
            note = f'Domain pivot: searching Facebook Ads Library by domain keyword "{pivot_query}" in USA.'
        else:
            raise ValidationError(f"Unknown pivot type: {pivot_type}")

        def run_pivot_search(keyword: str) -> Dict[str, Any]:
            try:
                return self.run_search(
                    chat_id=chat_id,
                    user_id=user_id,
                    params=AdsSearchParams(
                        keyword=keyword,
                        date_from=date_from,
                        date_to=date_to,
                        geo=geo,
                        limit=10,
                        page_ids=page_ids,
                    ),
                )
            except AcquisitionError as exc:
                if pivot_type == "domain" and is_collation_query_failure(exc):
                    raise AcquisitionError(DOMAIN_PIVOT_DUPLICATE_EXPANSION_UNAVAILABLE_MESSAGE) from exc
                raise

        if resolved_pivot_type == "domain":
            query_chain = broaden_domain_query_candidates(pivot_query) or [pivot_query]
            result = run_pivot_search(query_chain[0])
            resolved_query = query_chain[0]
            if len((result.get("data") or {}).get("grouped_results") or []) == 0:
                for candidate in query_chain[1:]:
                    fallback_result = run_pivot_search(candidate)
                    resolved_query = candidate
                    fallback_used = True
                    result = fallback_result
                    if len((fallback_result.get("data") or {}).get("grouped_results") or []) > 0:
                        break
            if fallback_used:
                if len((result.get("data") or {}).get("grouped_results") or []) > 0:
                    note = (
                        f'Domain pivot note: exact search "{query_chain[0]}" returned no grouped ad cards, '
                        f'so the runtime retried broader Meta-searchable domain "{resolved_query}".'
                    )
                else:
                    note = (
                        f'Domain pivot note: exact search "{query_chain[0]}" returned no grouped ad cards. '
                        f'Broader retries also returned no grouped ad cards: {", ".join(query_chain[1:])}.'
                    )
        else:
            result = run_pivot_search(search_keyword)

        result["status"] = f"{resolved_pivot_type}_pivot_completed"
        result["messages"][0]["text"] = note + "\n" + safe_text(result["messages"][0].get("text")).strip()
        result["summary"] = result["messages"][0]["text"]
        result["data"]["pivot"] = {
            "pivot_surface": "page" if pivot_type == "advertiser" else pivot_type,
            "pivot_type": resolved_pivot_type,
            "query": pivot_query,
            "resolved_query": resolved_query,
            "query_chain": query_chain,
            "fallback_used": fallback_used,
            "exact_page_id": exact_page_id,
            "source": source,
            "source_search_session_id": None,
            "source_group_key": None,
            "source_advertiser": pivot_query if pivot_type == "advertiser" else None,
            "source_landing_domain": pivot_query if pivot_type == "domain" else None,
        }
        search_session_id = safe_text((((result.get("data") or {}).get("search_session")) or {}).get("search_session_id")).strip()
        refreshed_session = self.persist_pivot_context(search_session_id or None, result["data"]["pivot"])
        if refreshed_session is not None:
            result["data"]["search_session"] = refreshed_session.as_dict()
        self.attach_pivot_bucket_summary_to_result(result)
        return result

    def run_group_pivot(self, pivot_type: str, session: SearchSession, group: GroupedAdEntity) -> Dict[str, Any]:
        if pivot_type == "advertiser":
            pivot_query = truncate_text(group.advertiser, 200)
            exact_page_id = self.normalize_explicit_page_id(group.page_id)
        elif pivot_type == "domain":
            pivot_domains = self.group_pivot_domains(group)
            if len(pivot_domains) > 1:
                raise ValidationError(
                    "Domain pivot unavailable because this grouped ad maps to multiple searchable domains "
                    f"({', '.join(pivot_domains)}). Use page or inspect instead."
                )
            pivot_query = pivot_domains[0] if pivot_domains else ""
            exact_page_id = None
            if not pivot_query:
                raise ValidationError("Domain pivot unavailable because this grouped ad has no landing domain.")
        else:
            raise ValidationError(f"Unknown pivot type: {pivot_type}")

        result = self.run_explicit_pivot(
            pivot_type,
            chat_id=session.chat_id,
            user_id=session.user_id,
            pivot_query=pivot_query,
            date_from=session.date_from,
            date_to=session.date_to,
            geo=session.geo,
            source="bound_group_reply",
            exact_page_id=exact_page_id,
        )
        result["data"]["pivot"].update(
            {
                "source_search_session_id": session.search_session_id,
                "source_group_key": group.group_key,
                "source_advertiser": group.advertiser,
                "source_page_id": group.page_id,
                "source_landing_domain": group.landing_domain or (group.landing_domains[0] if group.landing_domains else None),
                "source_search_domain": group.search_domain or (group.search_domains[0] if group.search_domains else None),
                "source_pivot_domain": pivot_query if pivot_type == "domain" else None,
            }
        )
        search_session_id = safe_text((((result.get("data") or {}).get("search_session")) or {}).get("search_session_id")).strip()
        refreshed_session = self.persist_pivot_context(search_session_id or None, result["data"]["pivot"])
        if refreshed_session is not None:
            result["data"]["search_session"] = refreshed_session.as_dict()
        self.attach_pivot_bucket_summary_to_result(result)
        return result

    def inspect_target_url(
        self,
        landing_url: Optional[str],
        *,
        advertiser_label: str,
        search_session_id: Optional[str],
        group_key: Optional[str],
        landing_domain: Optional[str],
        suggested_pivot_domain: Optional[str],
        bound_page_id: Optional[str],
        pivot_bucket_summary: Optional[Dict[str, Any]],
        pivot_bucket_note: Optional[str],
        pivot_bucket_overlap_summary: Optional[Dict[str, Any]],
        pivot_bucket_overlap_note: Optional[str],
        pivot_bucket_stack_summary: Optional[Dict[str, Any]],
        pivot_bucket_stack_note: Optional[str],
        pivot_bucket_delivery_summary: Optional[Dict[str, Any]],
        pivot_bucket_delivery_note: Optional[str],
        pivot_bucket_redirect_summary: Optional[Dict[str, Any]],
        pivot_bucket_redirect_note: Optional[str],
        current_lp_cluster: Optional[Dict[str, Any]],
        current_bucket_position: Optional[str],
        current_bucket_note: Optional[str],
        current_overlap_family: Optional[Dict[str, Any]],
        current_overlap_position: Optional[str],
        current_overlap_note: Optional[str],
        current_stack_family: Optional[Dict[str, Any]],
        current_stack_position: Optional[str],
        current_stack_note: Optional[str],
        current_delivery_family: Optional[Dict[str, Any]],
        current_delivery_position: Optional[str],
        current_delivery_note: Optional[str],
        current_redirect_family: Optional[Dict[str, Any]],
        current_redirect_position: Optional[str],
        current_redirect_note: Optional[str],
        source: str,
    ) -> Dict[str, Any]:
        if not landing_url:
            report = {
                "landing_url": None,
                "landing_host": None,
                "landing_registrable_domain": None,
                "final_url": None,
                "final_host": None,
                "final_registrable_domain": None,
                "final_status": None,
                "content_type": None,
                "landing_page_title": None,
                "redirect_chain": [],
                "redirect_chain_summary": "unavailable",
                "browser_final_url": None,
                "browser_final_host": None,
                "browser_final_registrable_domain": None,
                "suggested_pivot_domain": normalize_domain_text(suggested_pivot_domain),
                "pivot_registrable_domain": registrable_domain(suggested_pivot_domain),
                "landing_to_final_relationship": "unavailable",
                "direct_to_browser_relationship": "unavailable",
                "pivot_domain_context": "unavailable",
                "pivot_domain_context_note": "unavailable - grouped ad did not expose a landing URL to inspect.",
                "relationship_summary": "landing URL unavailable; browser comparison unavailable; no bound card context for pivot comparison",
                "technology_hints": [],
                "tracker_hints": [],
                "screenshot_status": "unavailable",
                "screenshot_reason": "Grouped ad did not expose a landing URL to inspect.",
                "screenshot_path": None,
                "notes": ["landing URL unavailable on grouped card"],
                "redirect_interpretation": "direct fetch unavailable",
                "delivery_divergence_hint": "browser_unavailable",
                "delivery_divergence_note": "browser comparison unavailable in this inspect run",
                "fetch_error": "Grouped ad did not expose a landing URL to inspect.",
            }
        else:
            report = self.inspect_landing_url(
                landing_url,
                pivot_domain=suggested_pivot_domain,
                bound_page_id=bound_page_id,
            )

        report["pivot_bucket_summary"] = pivot_bucket_summary
        report["pivot_bucket_note"] = pivot_bucket_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["pivot_bucket_overlap_summary"] = pivot_bucket_overlap_summary
        report["pivot_bucket_overlap_note"] = pivot_bucket_overlap_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["pivot_bucket_stack_summary"] = pivot_bucket_stack_summary
        report["pivot_bucket_stack_note"] = pivot_bucket_stack_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["pivot_bucket_delivery_summary"] = pivot_bucket_delivery_summary
        report["pivot_bucket_delivery_note"] = pivot_bucket_delivery_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["pivot_bucket_redirect_summary"] = pivot_bucket_redirect_summary
        report["pivot_bucket_redirect_note"] = pivot_bucket_redirect_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["current_lp_cluster"] = current_lp_cluster
        report["current_bucket_position"] = current_bucket_position or "unavailable"
        report["current_bucket_note"] = current_bucket_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["current_overlap_family"] = current_overlap_family
        report["current_overlap_position"] = current_overlap_position or "unavailable"
        report["current_overlap_note"] = current_overlap_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["current_stack_family"] = current_stack_family
        report["current_stack_position"] = current_stack_position or "unavailable"
        report["current_stack_note"] = current_stack_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["current_delivery_family"] = current_delivery_family
        report["current_delivery_position"] = current_delivery_position or "unavailable"
        report["current_delivery_note"] = current_delivery_note or INSPECT_NON_PIVOT_BUCKET_NOTE
        report["current_redirect_family"] = current_redirect_family
        report["current_redirect_position"] = current_redirect_position or "unavailable"
        report["current_redirect_note"] = current_redirect_note or INSPECT_NON_PIVOT_BUCKET_NOTE

        message_text = self.format_inspect_report_text(advertiser_label, report)
        status = "inspect_completed" if not report.get("fetch_error") else "inspect_partial"
        return {
            "ok": True,
            "status": status,
            "summary": message_text,
            "data": {
                "search_session_id": search_session_id,
                "group_key": group_key,
                "advertiser": advertiser_label,
                "landing_domain": landing_domain,
                "suggested_pivot_domain": normalize_domain_text(suggested_pivot_domain),
                "source_page_id": bound_page_id,
                "source": source,
                "inspect_report": report,
            },
            "messages": [self.inspect_message_payload(advertiser_label, report, message_text)],
            "debug": self.action_debug(),
        }

    def inspect_group_funnel(self, session: SearchSession, group: GroupedAdEntity) -> Dict[str, Any]:
        landing_url = group.landing_page_urls[0] if group.landing_page_urls else None
        bucket_context = self.pivot_bucket_context_for_session_group(session, group)
        return self.inspect_target_url(
            landing_url,
            advertiser_label=group.advertiser,
            search_session_id=session.search_session_id,
            group_key=group.group_key,
            landing_domain=group.landing_domain or (group.landing_domains[0] if group.landing_domains else None),
            suggested_pivot_domain=self.group_suggested_pivot_domain(group),
            bound_page_id=self.normalize_explicit_page_id(group.page_id),
            pivot_bucket_summary=bucket_context["pivot_bucket_summary"],
            pivot_bucket_note=bucket_context["pivot_bucket_note"],
            pivot_bucket_overlap_summary=bucket_context["pivot_bucket_overlap_summary"],
            pivot_bucket_overlap_note=bucket_context["pivot_bucket_overlap_note"],
            pivot_bucket_stack_summary=bucket_context["pivot_bucket_stack_summary"],
            pivot_bucket_stack_note=bucket_context["pivot_bucket_stack_note"],
            pivot_bucket_delivery_summary=bucket_context["pivot_bucket_delivery_summary"],
            pivot_bucket_delivery_note=bucket_context["pivot_bucket_delivery_note"],
            pivot_bucket_redirect_summary=bucket_context["pivot_bucket_redirect_summary"],
            pivot_bucket_redirect_note=bucket_context["pivot_bucket_redirect_note"],
            current_lp_cluster=bucket_context["current_lp_cluster"],
            current_bucket_position=bucket_context["current_bucket_position"],
            current_bucket_note=bucket_context["current_bucket_note"],
            current_overlap_family=bucket_context["current_overlap_family"],
            current_overlap_position=bucket_context["current_overlap_position"],
            current_overlap_note=bucket_context["current_overlap_note"],
            current_stack_family=bucket_context["current_stack_family"],
            current_stack_position=bucket_context["current_stack_position"],
            current_stack_note=bucket_context["current_stack_note"],
            current_delivery_family=bucket_context["current_delivery_family"],
            current_delivery_position=bucket_context["current_delivery_position"],
            current_delivery_note=bucket_context["current_delivery_note"],
            current_redirect_family=bucket_context["current_redirect_family"],
            current_redirect_position=bucket_context["current_redirect_position"],
            current_redirect_note=bucket_context["current_redirect_note"],
            source="bound_group_reply",
        )

    # ----------------------- Reference comparison -----------------------

    @staticmethod
    def _token_set(value: str) -> set[str]:
        return {token for token in normalize_string(value).split() if token}

    @staticmethod
    def overlap_ratio(left: Iterable[str], right: Iterable[str]) -> float:
        a = {normalize_string(item) for item in left if normalize_string(item)}
        b = {normalize_string(item) for item in right if normalize_string(item)}
        if not a and not b:
            return 1.0
        if not a or not b:
            return 0.0
        return round(len(a & b) / len(a | b), 4)

    @classmethod
    def creative_similarity_score(cls, own_groups: Sequence[GroupedAdEntity], reference_rows: Sequence[Dict[str, Any]]) -> float:
        if not own_groups or not reference_rows:
            return 0.0
        reference_creatives = [cls._token_set(cls.reference_creative_text(row)) for row in reference_rows]
        scores: List[float] = []
        for group in own_groups:
            own_tokens = cls._token_set(group.creative_text or " ".join(group.creative_titles))
            if not own_tokens:
                continue
            best = 0.0
            for ref_tokens in reference_creatives:
                if not ref_tokens:
                    continue
                score = len(own_tokens & ref_tokens) / len(own_tokens | ref_tokens)
                best = max(best, score)
            scores.append(best)
        if not scores:
            return 0.0
        return round(sum(scores) / len(scores), 4)

    @staticmethod
    def reference_creative_text(row: Dict[str, Any]) -> str:
        return (
            safe_text(row.get("creative_text")).strip()
            or safe_text(row.get("creative_body")).strip()
            or safe_text(row.get("text")).strip()
            or safe_text(row.get("title")).strip()
        )

    @classmethod
    def normalize_reference_rows(cls, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            rows = payload
        else:
            rows = payload.get("results") or payload.get("items") or payload.get("data") or []
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            landing_urls = row.get("landing_urls") or row.get("links") or []
            if isinstance(landing_urls, str):
                landing_urls = [landing_urls]
            landing_domains = row.get("landing_domains") or []
            if isinstance(landing_domains, str):
                landing_domains = [landing_domains]
            normalized.append(
                {
                    "advertiser": safe_text(row.get("advertiser") or row.get("page") or row.get("page_name")).strip(),
                    "landing_domains": sorted(
                        {
                            normalize_domain_text(item)
                            for item in list(landing_urls) + list(landing_domains) + [row.get("landing_url"), row.get("landing_page_url")]
                            if normalize_domain_text(item)
                        }
                    ),
                    "creative_text": cls.reference_creative_text(row),
                    "duplicate_count": maybe_int(row.get("duplicate_count")) or 1,
                }
            )
        return normalized

    @classmethod
    def creative_similarity_notes(
        cls,
        own_groups: Sequence[GroupedAdEntity],
        reference_rows: Sequence[Dict[str, Any]],
        creative_similarity: float,
    ) -> str:
        own_with_text = sum(1 for group in own_groups if cls._token_set(group.creative_text or " ".join(group.creative_titles)))
        reference_with_text = sum(1 for row in reference_rows if cls._token_set(cls.reference_creative_text(row)))
        if not own_with_text or not reference_with_text:
            return "Creative similarity is 0.0 because one side lacked usable creative text."
        return (
            "Average best-match token overlap across "
            f"{own_with_text} own groups and {reference_with_text} reference rows: {creative_similarity:.4f}."
        )

    @staticmethod
    def duplicate_handling_notes(
        own_groups: Sequence[GroupedAdEntity],
        reference_rows: Sequence[Dict[str, Any]],
        *,
        reference_duplicate_notes: Optional[str] = None,
    ) -> str:
        own_duplicate_groups = sum(1 for group in own_groups if group.duplicate_count > 1)
        reference_duplicate_groups = sum(1 for row in reference_rows if (maybe_int(row.get("duplicate_count")) or 1) > 1)
        note = f"Own grouped duplicates: {own_duplicate_groups}; reference grouped duplicates: {reference_duplicate_groups}."
        if reference_duplicate_notes:
            return f"{note} {reference_duplicate_notes}".strip()
        return note

    @classmethod
    def grade_reference_comparison(
        cls,
        *,
        search_session_id: str,
        keyword: str,
        date_from: Optional[str],
        date_to: Optional[str],
        geo: str,
        own_groups: Sequence[GroupedAdEntity],
        reference_rows: Sequence[Dict[str, Any]],
        notes: Optional[Sequence[str]] = None,
        raw_reference_payload: Optional[Dict[str, Any]] = None,
        reference_duplicate_notes: Optional[str] = None,
    ) -> ReferenceComparisonReport:
        own_advertisers = [group.advertiser for group in own_groups]
        ref_advertisers = [safe_text(row.get("advertiser")).strip() for row in reference_rows]
        own_domains = sorted({domain for group in own_groups for domain in group.landing_domains})
        ref_domains = sorted(
            {
                normalize_domain_text(domain)
                for row in reference_rows
                for domain in row.get("landing_domains", [])
                if normalize_domain_text(domain)
            }
        )
        advertiser_overlap = cls.overlap_ratio(own_advertisers, ref_advertisers)
        landing_overlap = cls.overlap_ratio(own_domains, ref_domains)
        creative_similarity = cls.creative_similarity_score(own_groups, reference_rows)
        creative_notes = cls.creative_similarity_notes(own_groups, reference_rows, creative_similarity)
        duplicate_notes = cls.duplicate_handling_notes(
            own_groups,
            reference_rows,
            reference_duplicate_notes=reference_duplicate_notes,
        )

        if advertiser_overlap >= 0.60 and landing_overlap >= 0.60 and creative_similarity >= 0.45:
            verdict = "pass"
        elif advertiser_overlap >= 0.30 or landing_overlap >= 0.30 or creative_similarity >= 0.25:
            verdict = "partial"
        else:
            verdict = "fail"

        return ReferenceComparisonReport(
            search_session_id=search_session_id,
            keyword=keyword,
            date_from=date_from,
            date_to=date_to,
            geo=geo,
            verdict=verdict,
            advertiser_overlap=advertiser_overlap,
            landing_domain_overlap=landing_overlap,
            creative_similarity=creative_similarity,
            creative_similarity_notes=creative_notes,
            duplicate_handling_notes=duplicate_notes,
            own_result_count=len(own_groups),
            reference_result_count=len(reference_rows),
            own_advertisers=own_advertisers,
            reference_advertisers=ref_advertisers,
            own_landing_domains=own_domains,
            reference_landing_domains=ref_domains,
            notes=list(notes or []),
            raw_reference_payload=raw_reference_payload,
        )

    def compare_reference(
        self, session: SearchSession, own_groups: Optional[Sequence[GroupedAdEntity]] = None
    ) -> ReferenceComparisonReport:
        if not self.config.reference_base_url:
            raise ValidationError("Reference service base URL is not configured")
        own_groups = list(own_groups or self.all_groups(session.search_session_id, emitted_only=True, limit=10))
        payload = {
            "keyword": session.keyword,
            "date_from": session.date_from,
            "date_to": session.date_to,
            "geo": session.geo,
            "limit": 10,
        }
        body = compact_json(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "facebook-ads-agent/phase1",
        }
        if self.config.reference_token:
            headers["Authorization"] = f"Bearer {self.config.reference_token}"
        base = self.config.reference_base_url.rstrip("/")
        path = self.config.reference_search_path if self.config.reference_search_path.startswith("/") else "/" + self.config.reference_search_path
        status, raw_body, _ = self._request(
            "POST",
            base + path,
            headers=headers,
            body=body,
            request_kind="reference_compare",
        )
        if status >= 400:
            raise AcquisitionError(f"Reference service returned HTTP {status}")
        response = json.loads(raw_body.decode("utf-8", "replace"))
        reference_rows = self.normalize_reference_rows(response)
        report = self.grade_reference_comparison(
            search_session_id=session.search_session_id,
            keyword=session.keyword,
            date_from=session.date_from,
            date_to=session.date_to,
            geo=session.geo,
            own_groups=own_groups,
            reference_rows=reference_rows,
            notes=[],
            raw_reference_payload=response if isinstance(response, dict) else {"results": response},
        )
        self.update_session(session.search_session_id, comparison_json=compact_json(report.as_dict()))
        return report

    def action_debug(self, **extra: Any) -> Dict[str, Any]:
        debug = {"request_transport_summary": self.request_transport_summary()}
        debug.update(extra)
        return debug

    # ----------------------- Public actions -----------------------

    def search_display_query(self, params: AdsSearchParams) -> str:
        keyword = safe_text(params.keyword).strip()
        if keyword:
            return keyword
        page_ids = normalize_page_ids(params.page_ids)
        if len(page_ids) == 1:
            return page_ids[0]
        return "requested search"

    def run_search(
        self,
        *,
        chat_id: str,
        user_id: str,
        params: AdsSearchParams,
    ) -> Dict[str, Any]:
        self.cleanup_expired_sessions()
        session = self.create_session(chat_id, user_id, params)
        try:
            total_count, total_count_text, bootstrap_error = self.maybe_total_count(params)
            self.update_session(session.search_session_id, total_count=total_count, total_count_text=total_count_text)
            session = self.load_session(session.search_session_id)
            display_query = self.search_display_query(params)

            emitted_session, groups, diversity_debug = self.emit_next_groups(session, params.limit)
            if not groups:
                emitted_session = self.load_session(session.search_session_id)
                self.update_session(emitted_session.search_session_id, exhausted=True)
                return {
                    "ok": True,
                    "status": "no_results",
                    "summary": f'No Facebook Ads Library results found for "{display_query}" in USA.',
                    "data": {
                        "search_session": self.load_session(session.search_session_id).as_dict(),
                        "grouped_results": [],
                        "total_count": total_count,
                        "total_count_text": total_count_text or "unavailable",
                        "comparison_report": None,
                    },
                    "messages": [
                        {
                            "text": f'USA Facebook Ads search for "{display_query}" returned no grouped ad cards.'
                        }
                    ],
                    "debug": {
                        **self.action_debug(),
                        "bootstrap_error": bootstrap_error,
                        "comparison_error": None,
                        "diversity": diversity_debug,
                    },
                }

            summary_lines = [
                f'USA Facebook Ads search for "{display_query}" returned {len(groups)} grouped ad cards.',
                f"Total ads found: {total_count_text or 'not available from Meta for this run'}",
            ]
            if params.date_from or params.date_to:
                summary_lines.append(
                    f"Date range: {params.date_from or 'open'} to {params.date_to or 'open'}"
                )
            weak_match_note = safe_text(diversity_debug.get("weak_match_note")).strip()
            if weak_match_note:
                summary_lines.append(weak_match_note)
            messages: List[Dict[str, Any]] = [{"text": "\n".join(summary_lines)}]
            for group in groups:
                messages.append(self.grouped_message_payload(session.search_session_id, group))

            refreshed = self.load_session(session.search_session_id)
            needs_prompt = (
                not refreshed.exhausted
                or bool(self.un_emitted_groups(refreshed.search_session_id, 1))
                or self.has_pending_candidates(refreshed.search_session_id)
            )
            if needs_prompt:
                messages[0]["text"] = messages[0]["text"] + "\n" + SUMMARY_PROMPT_TEXT
                messages[0]["deferred_prompt_bind_session_id"] = refreshed.search_session_id
            else:
                self.update_session(refreshed.search_session_id, status="completed")

            return {
                "ok": True,
                "status": "search_completed",
                "summary": messages[0]["text"],
                "data": {
                    "search_session": self.load_session(session.search_session_id).as_dict(),
                    "grouped_results": [group.as_dict() for group in groups],
                    "total_count": total_count,
                    "total_count_text": total_count_text or "unavailable",
                    "comparison_report": None,
                },
                "messages": messages,
                "debug": {
                    **self.action_debug(),
                    "bootstrap_error": bootstrap_error,
                    "comparison_error": None,
                    "diversity": diversity_debug,
                },
            }
        except Exception as exc:
            self.persist_session_error(session.search_session_id, exc)
            raise

    def run_next_page(self, session: SearchSession, *, limit: int) -> Dict[str, Any]:
        self.cleanup_expired_sessions()
        try:
            session, groups, diversity_debug = self.emit_next_groups(session, limit)
            if not groups:
                self.update_session(session.search_session_id, exhausted=True, status="completed")
                return {
                    "ok": True,
                    "status": "exhausted",
                    "summary": "No further grouped ad cards are available for this search session.",
                    "data": {
                        "search_session": self.load_session(session.search_session_id).as_dict(),
                        "grouped_results": [],
                    },
                    "messages": [{"text": "No more grouped ad cards are available for this search session."}],
                    "debug": {
                        **self.action_debug(),
                        "diversity": diversity_debug,
                    },
                }

            messages: List[Dict[str, Any]] = []
            for group in groups:
                messages.append(self.grouped_message_payload(session.search_session_id, group))

            refreshed = self.load_session(session.search_session_id)
            needs_prompt = (
                not refreshed.exhausted
                or bool(self.un_emitted_groups(refreshed.search_session_id, 1))
                or self.has_pending_candidates(refreshed.search_session_id)
            )
            if needs_prompt:
                messages.append({"text": SUMMARY_PROMPT_TEXT, "bind_session_id": refreshed.search_session_id})
            else:
                self.update_session(refreshed.search_session_id, status="completed")

            return {
                "ok": True,
                "status": "next_page_completed",
                "summary": f'Returned {len(groups)} additional grouped ad cards for "{self.search_display_query(AdsSearchParams(keyword=session.keyword, date_from=session.date_from, date_to=session.date_to, geo=session.geo, page_ids=session.page_ids))}".',
                "data": {
                    "search_session": self.load_session(session.search_session_id).as_dict(),
                    "grouped_results": [group.as_dict() for group in groups],
                },
                "messages": messages,
                "debug": {
                    **self.action_debug(),
                    "diversity": diversity_debug,
                },
            }
        except Exception as exc:
            self.persist_session_error(session.search_session_id, exc)
            raise

    def health_check(self, params: AdsSearchParams) -> Dict[str, Any]:
        diagnostic = AcquisitionDiagnostic(
            keyword=params.keyword,
            geo=params.geo,
            date_from=params.date_from,
            date_to=params.date_to,
            bootstrap_ok=False,
            search_ok=False,
            details_ok=False,
            collation_ok=False,
            total_count_text=None,
            total_count=None,
            doc_ids=dict(self.config.doc_ids),
        )
        try:
            count, count_text, bootstrap_error = self.maybe_total_count(params)
            diagnostic.bootstrap_ok = bootstrap_error is None
            diagnostic.total_count = count
            diagnostic.total_count_text = count_text
            if bootstrap_error:
                diagnostic.errors.append(bootstrap_error)
        except Exception as exc:  # noqa: BLE001
            diagnostic.errors.append(f"bootstrap: {exc}")

        graphql_session_id = str(uuid.uuid4())
        try:
            records, _cursor, _has_next, _payload = self.search_page(
                params,
                graphql_session_id=graphql_session_id,
                cursor=None,
            )
            diagnostic.search_ok = bool(records)
            if not records:
                diagnostic.notes.append("Search replay returned zero results.")
            else:
                details, _ = self.get_ad_details_record(records[0].ad_archive_id, records[0].page_id, graphql_session_id)
                diagnostic.details_ok = bool(details.page_alias or details.page_likes is not None)
                duplicate_candidate = next((item for item in records if item.collation_count > 1 and item.collation_id), None)
                if duplicate_candidate:
                    related, _collation_payload, _aggregate_payload = self.get_collation_records(
                        duplicate_candidate, graphql_session_id
                    )
                    diagnostic.collation_ok = len(related) >= 1
                else:
                    diagnostic.notes.append("Search replay found no duplicate-heavy candidate on the first page.")
        except Exception as exc:  # noqa: BLE001
            diagnostic.errors.append(str(exc))

        return {
            "ok": not diagnostic.errors,
            "status": "health_check_completed" if not diagnostic.errors else "health_check_failed",
            "summary": (
                "Facebook Ads acquisition path is healthy."
                if not diagnostic.errors
                else "Facebook Ads acquisition path failed health check."
            ),
            "data": diagnostic.as_dict(),
            "debug": self.action_debug(),
        }

    # ----------------------- Command / hook actions -----------------------

    def parse_query_with_date_filters(self, raw_text: str, *, field_label: str) -> Tuple[str, Optional[str], Optional[str]]:
        text = (raw_text or "").strip()
        if not text:
            raise ValidationError('Usage: /ads "keyword" from=YYYY-MM-DD to=YYYY-MM-DD')
        keyword = text
        quoted = re.match(r'^"([^"]+)"(.*)$', text)
        tail = ""
        if quoted:
            keyword = quoted.group(1).strip()
            tail = quoted.group(2).strip()
        else:
            parts = re.split(r"\s+(?=(?:from|to)=)", text, maxsplit=1)
            keyword = parts[0].strip()
            tail = parts[1].strip() if len(parts) > 1 else ""
            if not keyword:
                raise ValidationError("Keyword is required")

        args = {"from": None, "to": None}
        for token in tail.split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            if key in args:
                args[key] = value.strip()

        date_from = ensure_date(args["from"], "date_from") if args["from"] else None
        date_to = ensure_date(args["to"], "date_to") if args["to"] else None
        if date_from and date_to and date_from > date_to:
            raise ValidationError("date_from must be earlier than or equal to date_to")
        if not keyword:
            raise ValidationError(f"{field_label} is required")
        return keyword, date_from, date_to

    def parse_search_command(self, raw_text: str) -> AdsSearchParams:
        text = (raw_text or "").strip()
        if not text:
            raise ValidationError('Usage: /ads "keyword" from=YYYY-MM-DD to=YYYY-MM-DD')
        if normalize_string(text) in {"next", "next 10"}:
            raise ValidationError("NEXT_ONLY")
        keyword, date_from, date_to = self.parse_query_with_date_filters(text, field_label="Keyword")
        keyword, relative_date_from, relative_date_to = normalize_conversational_search_keyword(
            keyword,
            allow_relative_dates=True,
        )
        if not date_from and not date_to:
            date_from, date_to = relative_date_from, relative_date_to
        if not keyword:
            raise ValidationError("Keyword is required")
        return AdsSearchParams(keyword=keyword, date_from=date_from, date_to=date_to)

    def parse_ads_command_action(self, raw_text: str) -> Dict[str, Any]:
        text = (raw_text or "").strip()
        if not text:
            raise ValidationError('Usage: /ads "keyword" from=YYYY-MM-DD to=YYYY-MM-DD')
        normalized = normalize_string(text)
        if normalized in {"next", "next 10"}:
            return {"kind": "next"}

        for prefix, kind, label in [
            ("page", "advertiser_pivot", "Advertiser/page query"),
            ("advertiser", "advertiser_pivot", "Advertiser/page query"),
            ("domain", "domain_pivot", "Landing domain"),
            ("inspect", "inspect", "Landing URL"),
        ]:
            if normalized == prefix or normalized.startswith(prefix + " "):
                value, date_from, date_to = self.parse_query_with_date_filters(text[len(prefix):].strip(), field_label=label)
                return {
                    "kind": kind,
                    "value": value,
                    "date_from": date_from,
                    "date_to": date_to,
                }

        return {"kind": "search", "params": self.parse_search_command(text)}

    def command_default_dates(self, chat_id: str, user_id: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            session = self.current_session_for_chat_user(chat_id, user_id)
        except ValidationError:
            return None, None
        return session.date_from, session.date_to

    def normalize_explicit_inspect_target(self, raw_target: str) -> str:
        target = safe_text(raw_target).strip()
        if not target:
            raise ValidationError("Landing URL is required for /ads inspect")
        if re.match(r"^[a-z][a-z0-9+.-]*://", target, flags=re.IGNORECASE):
            return target
        if re.match(r"^[A-Za-z0-9.-]+\.[A-Za-z]{2,}([/?#].*)?$", target):
            return "https://" + target
        raise ValidationError("Inspect requires a landing URL or domain-like target.")

    @staticmethod
    def normalize_explicit_page_id(raw_target: str) -> Optional[str]:
        target = safe_text(raw_target).strip()
        if re.fullmatch(r"\d+", target):
            return target
        return None

    @staticmethod
    def preview_label_quote(value: Any, *, fallback: str) -> str:
        text = truncate_text(safe_text(value).strip(), 160)
        if not text:
            text = fallback
        return f'"{text}"'

    def preview_search_task_label(self, params: AdsSearchParams) -> str:
        return f"search {self.preview_label_quote(self.search_display_query(params), fallback='requested search')}"

    def preview_next_page_task_label(self, session: SearchSession) -> str:
        params = AdsSearchParams(
            keyword=session.keyword,
            date_from=session.date_from,
            date_to=session.date_to,
            geo=session.geo,
            page_ids=session.page_ids,
        )
        return f"the next 10 grouped ads for {self.preview_label_quote(self.search_display_query(params), fallback='requested search')}"

    def preview_advertiser_pivot_task_label(self, pivot_query: str, *, exact_page_id: Optional[str] = None) -> str:
        if exact_page_id:
            return f"page pivot for {exact_page_id}"
        return f"advertiser pivot for {self.preview_label_quote(pivot_query, fallback='requested advertiser')}"

    @staticmethod
    def preview_domain_pivot_task_label(pivot_query: str) -> str:
        display = normalize_domain_text(pivot_query) or truncate_text(safe_text(pivot_query).strip(), 160) or "requested domain"
        return f"domain pivot for {display}"

    @staticmethod
    def preview_inspect_task_label(target: Any) -> str:
        target_text = safe_text(target).strip()
        display = normalize_domain_text(extract_domain(target_text)) or truncate_text(target_text, 160) or "the requested landing target"
        return f"inspect {display}"

    @staticmethod
    def preview_acceptance_result(task_class: str, *, task_label: Optional[str] = None) -> Dict[str, Any]:
        return {
            "ok": True,
            "status": "accepted_preview",
            "summary": f"Accepted Facebook Ads request preview for {task_class}.",
            "data": {
                "task_class": task_class,
                "task_label": task_label,
            },
        }

    def action_preview_run_ads_command(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        chat_id = normalize_chat_id(payload.get("chat_id"))
        user_id = safe_text(payload.get("user_id")).strip()
        args = safe_text(payload.get("text")).strip()
        if not chat_id or not user_id:
            raise ValidationError("chat_id and user_id are required")
        parsed = self.parse_ads_command_action(args)
        if parsed["kind"] == "next":
            session = self.latest_active_session(chat_id, user_id)
            return self.preview_acceptance_result("next_10", task_label=self.preview_next_page_task_label(session))
        if parsed["kind"] == "search":
            return self.preview_acceptance_result("search", task_label=self.preview_search_task_label(parsed["params"]))
        if parsed["kind"] in {"advertiser_pivot", "domain_pivot"}:
            exact_page_id = self.normalize_explicit_page_id(parsed["value"]) if parsed["kind"] == "advertiser_pivot" else None
            task_label = (
                self.preview_advertiser_pivot_task_label(parsed["value"], exact_page_id=exact_page_id)
                if parsed["kind"] == "advertiser_pivot"
                else self.preview_domain_pivot_task_label(parsed["value"])
            )
            return self.preview_acceptance_result("pivot", task_label=task_label)
        if parsed["kind"] == "inspect":
            target = self.normalize_explicit_inspect_target(parsed["value"])
            return self.preview_acceptance_result("inspect", task_label=self.preview_inspect_task_label(target))
        raise ValidationError(f"Unknown /ads command kind: {parsed['kind']}")

    def action_run_ads_command(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        chat_id = normalize_chat_id(payload.get("chat_id"))
        user_id = safe_text(payload.get("user_id")).strip()
        args = safe_text(payload.get("text")).strip()
        if not chat_id or not user_id:
            raise ValidationError("chat_id and user_id are required")
        parsed = self.parse_ads_command_action(args)
        if parsed["kind"] == "next":
            session = self.latest_active_session(chat_id, user_id)
            return self.run_next_page(session, limit=10)
        if parsed["kind"] == "search":
            params = parsed["params"]
            if not params.date_from and not params.date_to:
                default_from, default_to = default_command_date_range()
                params = dataclasses.replace(params, date_from=default_from, date_to=default_to)
            return self.run_search(chat_id=chat_id, user_id=user_id, params=params)

        default_from, default_to = self.command_default_dates(chat_id, user_id)
        date_from = parsed.get("date_from") or default_from
        date_to = parsed.get("date_to") or default_to
        if parsed["kind"] in {"advertiser_pivot", "domain_pivot"} and not date_from and not date_to:
            date_from, date_to = default_command_date_range()

        if parsed["kind"] == "advertiser_pivot":
            exact_page_id = self.normalize_explicit_page_id(parsed["value"])
            return self.run_explicit_pivot(
                "advertiser",
                chat_id=chat_id,
                user_id=user_id,
                pivot_query=truncate_text(parsed["value"], 200),
                date_from=date_from,
                date_to=date_to,
                source="typed_command",
                exact_page_id=exact_page_id,
            )
        if parsed["kind"] == "domain_pivot":
            return self.run_explicit_pivot(
                "domain",
                chat_id=chat_id,
                user_id=user_id,
                pivot_query=truncate_text(parsed["value"], 200),
                date_from=date_from,
                date_to=date_to,
                source="typed_command",
            )
        if parsed["kind"] == "inspect":
            target = self.normalize_explicit_inspect_target(parsed["value"])
            return self.inspect_target_url(
                target,
                advertiser_label=extract_domain(target) or target,
                search_session_id=None,
                group_key=None,
                landing_domain=extract_domain(target),
                suggested_pivot_domain=None,
                bound_page_id=None,
                pivot_bucket_summary=None,
                pivot_bucket_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                pivot_bucket_overlap_summary=None,
                pivot_bucket_overlap_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                pivot_bucket_stack_summary=None,
                pivot_bucket_stack_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                pivot_bucket_delivery_summary=None,
                pivot_bucket_delivery_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                pivot_bucket_redirect_summary=None,
                pivot_bucket_redirect_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                current_lp_cluster=None,
                current_bucket_position="unavailable",
                current_bucket_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                current_overlap_family=None,
                current_overlap_position="unavailable",
                current_overlap_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                current_stack_family=None,
                current_stack_position="unavailable",
                current_stack_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                current_delivery_family=None,
                current_delivery_position="unavailable",
                current_delivery_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                current_redirect_family=None,
                current_redirect_position="unavailable",
                current_redirect_note=INSPECT_NON_PIVOT_BUCKET_NOTE,
                source="typed_command",
            )
        raise ValidationError(f"Unknown /ads command kind: {parsed['kind']}")

    def action_preview_handle_reply(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        chat_id = normalize_chat_id(payload.get("chat_id"))
        user_id = safe_text(payload.get("user_id")).strip()
        reply_message_id = maybe_int(payload.get("reply_to_message_id"))
        if reply_message_id is None:
            reply_message_id = maybe_int(payload.get("reply_to_prompt_message_id"))
        reply_text = normalize_string(safe_text(payload.get("text")))
        if not chat_id or not user_id:
            raise ValidationError("chat_id and user_id are required")
        if reply_text in {"page", "advertiser", "domain", "inspect"}:
            if reply_message_id is None:
                if reply_text in {"page", "advertiser"}:
                    raise ValidationError("Advertiser pivot requires a direct reply to a grouped ad card.")
                if reply_text == "domain":
                    raise ValidationError("Domain pivot requires a direct reply to a grouped ad card.")
                raise ValidationError("Inspect requires a direct reply to a grouped ad card.")
            _session, group = self.bound_group_for_message(chat_id, user_id, reply_message_id)
            if reply_text in {"page", "advertiser"}:
                exact_page_id = self.normalize_explicit_page_id(group.page_id)
                return self.preview_acceptance_result(
                    "pivot",
                    task_label=self.preview_advertiser_pivot_task_label(group.advertiser, exact_page_id=exact_page_id),
                )
            if reply_text == "domain":
                pivot_domains = self.group_pivot_domains(group)
                if len(pivot_domains) > 1:
                    raise ValidationError(
                        "Domain pivot unavailable because this grouped ad maps to multiple searchable domains "
                        f"({', '.join(pivot_domains)}). Use page or inspect instead."
                    )
                if not pivot_domains:
                    raise ValidationError("Domain pivot unavailable because this grouped ad has no landing domain.")
                return self.preview_acceptance_result(
                    "pivot",
                    task_label=self.preview_domain_pivot_task_label(pivot_domains[0]),
                )
            if reply_text == "inspect":
                inspect_target = (
                    (group.landing_page_urls[0] if group.landing_page_urls else None)
                    or group.landing_domain
                    or (group.landing_domains[0] if group.landing_domains else None)
                    or group.search_domain
                    or (group.search_domains[0] if group.search_domains else None)
                    or group.advertiser
                )
                return self.preview_acceptance_result(
                    "inspect",
                    task_label=self.preview_inspect_task_label(inspect_target),
                )
        if reply_text in {"next 10", "next"}:
            if reply_message_id is not None:
                self.session_for_prompt(chat_id, user_id, reply_message_id)
            else:
                session = self.latest_active_session(chat_id, user_id)
                if session.prompt_message_id is None:
                    raise ValidationError("No active ads session is waiting for next 10.")
            session = self.session_for_prompt(chat_id, user_id, reply_message_id) if reply_message_id is not None else session
            return self.preview_acceptance_result("next_10", task_label=self.preview_next_page_task_label(session))
        return {
            "ok": True,
            "status": "ignored_reply_preview",
            "summary": "Reply did not match the pagination, pivot, or inspect contract.",
            "data": {
                "task_class": None,
            },
        }

    def action_handle_reply(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        chat_id = normalize_chat_id(payload.get("chat_id"))
        user_id = safe_text(payload.get("user_id")).strip()
        reply_message_id = maybe_int(payload.get("reply_to_message_id"))
        if reply_message_id is None:
            reply_message_id = maybe_int(payload.get("reply_to_prompt_message_id"))
        reply_text = normalize_string(safe_text(payload.get("text")))
        if not chat_id or not user_id:
            raise ValidationError("chat_id and user_id are required")
        if reply_text not in {"next 10", "next"}:
            if reply_text in {"page", "advertiser"}:
                if reply_message_id is None:
                    raise ValidationError("Advertiser pivot requires a direct reply to a grouped ad card.")
                session, group = self.bound_group_for_message(chat_id, user_id, reply_message_id)
                return self.run_group_pivot("advertiser", session, group)
            if reply_text == "domain":
                if reply_message_id is None:
                    raise ValidationError("Domain pivot requires a direct reply to a grouped ad card.")
                session, group = self.bound_group_for_message(chat_id, user_id, reply_message_id)
                return self.run_group_pivot("domain", session, group)
            if reply_text == "inspect":
                if reply_message_id is None:
                    raise ValidationError("Inspect requires a direct reply to a grouped ad card.")
                session, group = self.bound_group_for_message(chat_id, user_id, reply_message_id)
                return self.inspect_group_funnel(session, group)
            return {
                "ok": True,
                "status": "ignored_reply",
                "summary": "Reply did not match the pagination, pivot, or inspect contract.",
                "messages": [],
            }
        if reply_message_id is not None:
            session = self.session_for_prompt(chat_id, user_id, reply_message_id)
        else:
            session = self.latest_active_session(chat_id, user_id)
            if session.prompt_message_id is None:
                raise ValidationError("No active ads session is waiting for next 10.")
        return self.run_next_page(session, limit=10)

    # ----------------------- Action dispatcher -----------------------

    def dispatch(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.reset_request_clients()
        self.transport_tracker.reset()
        if action == "preview_run_ads_command":
            return self.action_preview_run_ads_command(payload)
        if action == "preview_handle_reply":
            return self.action_preview_handle_reply(payload)
        if action == "run_ads_command":
            return self.action_run_ads_command(payload)
        if action == "handle_reply":
            return self.action_handle_reply(payload)
        if action == "bind_session_prompt":
            search_session_id = safe_text(payload.get("search_session_id")).strip()
            prompt_message_id = maybe_int(payload.get("prompt_message_id"))
            if not search_session_id or prompt_message_id is None:
                raise ValidationError("search_session_id and prompt_message_id are required")
            self.bind_session_prompt(search_session_id, prompt_message_id)
            return {
                "ok": True,
                "status": "prompt_bound",
                "summary": "Bound prompt message id to search session.",
            }
        if action == "bind_group_message":
            search_session_id = safe_text(payload.get("search_session_id")).strip()
            group_key = safe_text(payload.get("group_key")).strip()
            message_id = maybe_int(payload.get("message_id"))
            if not search_session_id or not group_key or message_id is None:
                raise ValidationError("search_session_id, group_key, and message_id are required")
            self.bind_group_message(search_session_id, group_key, message_id)
            return {
                "ok": True,
                "status": "group_message_bound",
                "summary": "Bound grouped ad card message id to search session.",
            }
        if action == "search_ads":
            params = AdsSearchParams(
                keyword=safe_text(payload.get("keyword")).strip(),
                date_from=ensure_date(payload.get("date_from"), "date_from") if payload.get("date_from") else None,
                date_to=ensure_date(payload.get("date_to"), "date_to") if payload.get("date_to") else None,
                geo=safe_text(payload.get("geo")).strip() or "US",
                limit=maybe_int(payload.get("limit")) or 10,
            )
            if params.geo != "US":
                raise ValidationError("This MVP only supports geo=US")
            chat_id, user_id = self.tool_session_scope(payload)
            return self.run_search(chat_id=chat_id, user_id=user_id, params=params)
        if action == "get_next_page":
            session_id = safe_text(payload.get("search_session_id")).strip()
            limit = maybe_int(payload.get("limit")) or 10
            session = self.load_session(session_id)
            return self.run_next_page(session, limit=limit)
        if action == "get_ad_details":
            ad_archive_id = safe_text(payload.get("ad_archive_id")).strip()
            page_id = safe_text(payload.get("page_id")).strip()
            graphql_session_id = safe_text(payload.get("graphql_session_id")).strip() or str(uuid.uuid4())
            details, details_payload = self.get_ad_details_record(ad_archive_id, page_id, graphql_session_id)
            return {
                "ok": True,
                "status": "details_completed",
                "summary": f"Fetched detailed ad metadata for {ad_archive_id}.",
                "data": {
                    "details": details.as_dict(),
                    "diagnostic_request": details_payload.get("_request"),
                },
            }
        if action == "format_grouped_ad_card":
            entity = payload.get("grouped_ad_entity")
            if not isinstance(entity, dict):
                raise ValidationError("grouped_ad_entity object is required")
            group = GroupedAdEntity(**entity)
            card_text = self.format_grouped_card(
                group,
                include_media_line=bool(payload.get("include_media_line", True)),
            )
            return {
                "ok": True,
                "status": "card_formatted",
                "summary": "Formatted grouped ad card.",
                "data": {
                    "card_text": card_text,
                },
            }
        if action == "inspect_group_funnel":
            search_session_id = safe_text(payload.get("search_session_id")).strip()
            group_key = safe_text(payload.get("group_key")).strip()
            if not search_session_id or not group_key:
                raise ValidationError("search_session_id and group_key are required")
            session = self.load_session(search_session_id)
            group = self.load_group(search_session_id, group_key)
            return self.inspect_group_funnel(session, group)
        if action == "ads_health_check":
            params = AdsSearchParams(
                keyword=safe_text(payload.get("keyword")).strip() or "shopify",
                date_from=ensure_date(payload.get("date_from"), "date_from") if payload.get("date_from") else None,
                date_to=ensure_date(payload.get("date_to"), "date_to") if payload.get("date_to") else None,
            )
            return self.health_check(params)
        if action == "compare_reference_results":
            session = self.load_session(safe_text(payload.get("search_session_id")).strip())
            report = self.compare_reference(session)
            return {
                "ok": True,
                "status": "reference_compared",
                "summary": f"Reference comparison verdict: {report.verdict}.",
                "data": report.as_dict(),
            }
        raise ValidationError(f"Unknown action: {action}")


def success_result(data: Dict[str, Any]) -> Dict[str, Any]:
    data.setdefault("ok", True)
    return data


def error_result(exc: Exception, *, debug: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    retryable = isinstance(exc, AcquisitionError)
    message = str(exc).strip() or exc.__class__.__name__
    result = {
        "ok": False,
        "status": "error",
        "summary": message,
        "error": {
            "code": exc.__class__.__name__,
            "message": message,
            "retryable": retryable,
            "operator_action": (
                "Refresh proof-gate artifacts and doc_id overrides, then rerun ads_health_check."
                if retryable
                else None
            ),
        },
    }
    if debug:
        result["debug"] = debug
    return result


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tool-action", required=True)
    parser.add_argument("--tool-payload-json", default="{}")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        payload = json.loads(args.tool_payload_json)
        if not isinstance(payload, dict):
            raise ValidationError("tool payload must be a JSON object")
    except json.JSONDecodeError as exc:
        payload = {}
        result = error_result(ValidationError(f"Invalid JSON payload: {exc}"))
        print(pretty_json(result))
        return 1

    runtime = FacebookAdsRuntime()
    try:
        result = success_result(runtime.dispatch(args.tool_action, payload))
        print(pretty_json(result))
        return 0
    except Exception as exc:  # noqa: BLE001 - deterministic error envelope
        print(pretty_json(error_result(exc, debug={"request_transport_summary": runtime.request_transport_summary()})))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
