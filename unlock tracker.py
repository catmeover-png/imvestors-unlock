#!/usr/bin/env python3
"""
LMTS investor unlocks tracker (Sablier Lockup on Base) -> Google Sheets.

NOTE: 0xc19a09a66887017f603e5df420ed3cb9a5c07c0a ("Investors Unlock") is the
shared SablierLockup contract on Base - it holds vesting streams for 100+
different tokens. This script keeps only streams whose underlying token is
LMTS (and, optionally, only streams created by SENDER_FILTER addresses).

Every stream is an ERC-721 NFT; its current owner is the recipient (investor)
who can claim.

Pipeline:
  1. eth_getLogs on the Lockup contract, one pass, three topics:
       Transfer (ERC-721)        -> all stream ids + current recipient
       WithdrawFromLockupStream  -> claims   (token topic == LMTS)
       CancelLockupStream        -> cancels  (token topic == LMTS)
  2. Multicall3 getUnderlyingToken(id) for every stream -> keep LMTS
  3. Multicall3 full state of LMTS streams (amounts, times, cliff,
     granularity, tranches / segments)
  4. Multicall3 LMTS balanceOf(recipient)
  5. Unlock schedule re-computed locally (Sablier VestingMath) for
     next unlock date/amount and 7d/30d/90d projections. The local
     "streamed now" is compared with on-chain streamedAmountOf (calc_drift).

Reads:   Labels, _UnlockState
Writes:  Unlock_Investors, Unlock_Streams, Unlock_Upcoming, Unlock_Claims,
         Unlock_Summary, _UnlockState

Required env:
  BASESCAN_API_KEY              Alchemy API key (name kept for compatibility)
  UNLOCK_SHEET_ID / GOOGLE_SHEET_ID / GSHEET_ID
  GOOGLE_SERVICE_ACCOUNT_JSON

Optional env:
  LOCKUP_ADDRESS        default 0xc19a09a66887017f603e5df420ed3cb9a5c07c0a
  LMTS_TOKEN            default 0x9eadbe35f3ee3bf3e28180070c429298a1b02f93
  SENDER_FILTER         default ""  comma-separated stream creators to keep
  LOCKUP_DEPLOY_BLOCK   default auto (binary search, cached in _UnlockState)
  LABELS_SHEET          default Labels
  UPCOMING_DAYS         default 60
  CLAIMS_MAX_ROWS       default 20000
  ALCHEMY_BASE_URL      default https://base-mainnet.g.alchemy.com/v2
  LOG_CHUNK             default 100000
  MULTICALL_SIZE        default 200
  CALL_BATCH            default 20
  RATE_LIMIT_RPS        default 4
  CONFIRMATIONS         default 5
  MULTICALL_ADDRESS     default 0xcA11bde05977b3631167028862bE2a173976CA11
"""

import os
import json
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Any

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

getcontext().prec = 60

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("unlocks")


# =========================
# CONFIG
# =========================

ALCHEMY_API_KEY = os.getenv("BASESCAN_API_KEY", "").strip()
ALCHEMY_BASE_URL = os.getenv("ALCHEMY_BASE_URL", "https://base-mainnet.g.alchemy.com/v2").strip()

GOOGLE_SHEET_ID = (
    os.getenv("UNLOCK_SHEET_ID", "").strip()
    or os.getenv("GOOGLE_SHEET_ID", "").strip()
    or os.getenv("GSHEET_ID", "").strip()
)
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

LOCKUP_ADDRESS = os.getenv(
    "LOCKUP_ADDRESS", "0xc19a09a66887017f603e5df420ed3cb9a5c07c0a"
).strip().lower()
LMTS_TOKEN = os.getenv(
    "LMTS_TOKEN", "0x9eadbe35f3ee3bf3e28180070c429298a1b02f93"
).strip().lower()
MULTICALL_ADDRESS = os.getenv(
    "MULTICALL_ADDRESS", "0xcA11bde05977b3631167028862bE2a173976CA11"
).strip().lower()

SENDER_FILTER = {
    a.strip().lower() for a in os.getenv("SENDER_FILTER", "").split(",") if a.strip()
}
LOCKUP_DEPLOY_BLOCK = int(os.getenv("LOCKUP_DEPLOY_BLOCK", "0") or 0)
LABELS_SHEET = os.getenv("LABELS_SHEET", "Labels")
RETAIL_LABEL = os.getenv("RETAIL_LABEL", "Retail")
UPCOMING_DAYS = int(os.getenv("UPCOMING_DAYS", "60"))
CLAIMS_MAX_ROWS = int(os.getenv("CLAIMS_MAX_ROWS", "20000"))
LOG_CHUNK = int(os.getenv("LOG_CHUNK", "100000"))
MULTICALL_SIZE = int(os.getenv("MULTICALL_SIZE", "200"))
CALL_BATCH = int(os.getenv("CALL_BATCH", "20"))
RATE_LIMIT_RPS = int(os.getenv("RATE_LIMIT_RPS", "4"))
CONFIRMATIONS = int(os.getenv("CONFIRMATIONS", "5"))

BASESCAN_TX = "https://basescan.org/tx/"
BASESCAN_ADDR = "https://basescan.org/address/"
BASESCAN_NFT = f"https://basescan.org/nft/{LOCKUP_ADDRESS}/"

DAY = 86400
STEP_MIN_SEC = 3600          # LL granularity >= 1h is treated as discrete steps
MAX_UPCOMING_POINTS = 200    # per stream, safety cap

# --- selectors (keccak-verified, all present in the Lockup dispatch table) ---
SEL_GET_UNDERLYING_TOKEN = "0xa4775772"
SEL_GET_SENDER = "0xb971302a"
SEL_GET_DEPOSITED = "0xa80fc071"
SEL_GET_WITHDRAWN = "0xd511609f"
SEL_GET_REFUNDED = "0xd4dbd20b"
SEL_STREAMED_AMOUNT_OF = "0x4869e12d"
SEL_WITHDRAWABLE_AMOUNT_OF = "0xd975dfed"
SEL_GET_START_TIME = "0xbc2be1be"
SEL_GET_END_TIME = "0x9067b677"
SEL_STATUS_OF = "0xad35efd4"
SEL_IS_CANCELABLE = "0x4857501f"
SEL_IS_TRANSFERABLE = "0xb2564569"
SEL_GET_CLIFF_TIME = "0x780a82c8"
SEL_GET_GRANULARITY = "0x5c61ce95"
SEL_GET_UNLOCK_AMOUNTS = "0xdf2a848c"
SEL_GET_TRANCHES = "0x7f5799f9"
SEL_GET_SEGMENTS = "0xb637b865"
SEL_GET_PRICE_GATED = "0x2fb2b3a8"
SEL_GET_LOCKUP_MODEL = "0xe6c417eb"
SEL_AGGREGATE_AMOUNT = "0xec01da3b"
SEL_NEXT_STREAM_ID = "0x1e99d569"
SEL_BALANCE_OF = "0x70a08231"
SEL_DECIMALS = "0x313ce567"
SEL_SYMBOL = "0x95d89b41"
SEL_AGGREGATE3 = "0x82ad56cb"

# --- event topics (keccak-verified) ---
TOPIC_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOPIC_WITHDRAW = "0x40b88e5c41c5a97ffb7b6ef88a0a2d505aa0c634cf8a0275cb236ea7dd87ed4d"
TOPIC_CANCEL = "0x5edb27d6c1a327513b90a792050debf074b7194444885e3144d4decc5caaaa50"

ZERO_ADDR = "0x" + "0" * 40

STATUS_NAMES = {0: "PENDING", 1: "STREAMING", 2: "SETTLED", 3: "CANCELED", 4: "DEPLETED"}
STATUS_CANCELED = 3
FINISHED_STATUSES = {2, 3, 4}
MODEL_FALLBACK = {0: "LL", 1: "LD", 2: "LT", 3: "LPG"}

# order matters: index = position in per-stream multicall result
STREAM_FIELDS = (
    ("sender", SEL_GET_SENDER),
    ("deposited", SEL_GET_DEPOSITED),
    ("withdrawn", SEL_GET_WITHDRAWN),
    ("refunded", SEL_GET_REFUNDED),
    ("streamed", SEL_STREAMED_AMOUNT_OF),
    ("withdrawable", SEL_WITHDRAWABLE_AMOUNT_OF),
    ("start", SEL_GET_START_TIME),
    ("end", SEL_GET_END_TIME),
    ("status", SEL_STATUS_OF),
    ("cancelable", SEL_IS_CANCELABLE),
    ("transferable", SEL_IS_TRANSFERABLE),
    ("model_raw", SEL_GET_LOCKUP_MODEL),
    ("cliff", SEL_GET_CLIFF_TIME),              # LL only
    ("granularity", SEL_GET_GRANULARITY),       # LL only
    ("unlock_amounts", SEL_GET_UNLOCK_AMOUNTS), # LL only
    ("tranches", SEL_GET_TRANCHES),             # LT only
    ("segments", SEL_GET_SEGMENTS),             # LD only
    ("price_gated", SEL_GET_PRICE_GATED),       # LPG only
)

STATE_SHEET = "_UnlockState"
SHEET_INVESTORS = "Unlock_Investors"
SHEET_STREAMS = "Unlock_Streams"
SHEET_UPCOMING = "Unlock_Upcoming"
SHEET_CLAIMS = "Unlock_Claims"
SHEET_SUMMARY = "Unlock_Summary"


# =========================
# MODELS
# =========================

@dataclass
class Stream:
    id: int
    recipient: str = ""
    burned: bool = False
    minted_block: int = 0
    sender: str = ""
    model: str = ""
    status: int = -1
    cancelable: bool = False
    transferable: bool = False
    deposited: int = 0        # raw units
    withdrawn: int = 0
    refunded: int = 0
    streamed: int = 0
    withdrawable: int = 0
    start: int = 0
    end: int = 0
    cliff: int = 0
    granularity: int = 0
    unlock_start: int = 0
    unlock_cliff: int = 0
    tranches: list[tuple[int, int]] = field(default_factory=list)        # (amount, ts)
    segments: list[tuple[int, int, int]] = field(default_factory=list)   # (amount, exp_raw, ts)


@dataclass
class LogEvent:
    block: int
    log_index: int
    tx_hash: str
    kind: str          # CLAIM | CANCEL
    stream_id: int
    to: str
    amount: int        # raw
    ts: int = 0


class RpcError(RuntimeError):
    pass


# =========================
# HELPERS
# =========================

def norm_addr(a: Any) -> str:
    return str(a or "").strip().lower()


def is_addr(a: str) -> bool:
    return a.startswith("0x") and len(a) == 42


def hex_to_int(v: Any) -> int:
    if v is None or v == "":
        return 0
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if s in {"0x", ""}:
        return 0
    return int(s, 16) if s.lower().startswith("0x") else int(s)


def to_hex(n: int) -> str:
    return hex(int(n))


def ts_to_utc(ts: int, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime(fmt)


def now_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def dec_str(v: Any, places: int = 6) -> str:
    if v is None:
        return ""
    if isinstance(v, Decimal):
        q = Decimal(1).scaleb(-places)
        s = format(v.quantize(q), "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s if s and s != "-0" else "0"
    return str(v)


def topic_to_addr(topic: str) -> str:
    return "0x" + str(topic)[-40:].lower()


def addr_topic(address: str) -> str:
    return "0x" + norm_addr(address)[2:].rjust(64, "0")


def _word(n: int) -> str:
    return hex(int(n))[2:].rjust(64, "0")


def encode_uint_call(selector: str, n: int) -> str:
    return selector + _word(n)


def encode_addr_call(selector: str, address: str) -> str:
    return selector + norm_addr(address)[2:].rjust(64, "0")


def human_days(seconds: int) -> str:
    if seconds <= 0:
        return "now"
    d = seconds / DAY
    return f"{d:.1f}d" if d < 10 else f"{int(d)}d"


# =========================
# ABI ENCODING / DECODING
# =========================

def _pad_bytes(hexstr: str) -> str:
    h = hexstr[2:] if hexstr.startswith("0x") else hexstr
    if len(h) % 64:
        h += "0" * (64 - len(h) % 64)
    return h


def encode_aggregate3(calls: list[tuple[str, bool, str]]) -> str:
    """calls: [(target, allow_failure, calldata_hex)] -> calldata hex."""
    n = len(calls)
    tuples: list[str] = []
    for target, allow, data in calls:
        raw = data[2:] if data.startswith("0x") else data
        tuples.append(
            _word(int(target, 16))
            + _word(1 if allow else 0)
            + _word(0x60)
            + _word(len(raw) // 2)
            + _pad_bytes(raw)
        )
    offsets: list[str] = []
    cursor = n * 32
    for t in tuples:
        offsets.append(_word(cursor))
        cursor += len(t) // 2
    return SEL_AGGREGATE3 + _word(0x20) + _word(n) + "".join(offsets) + "".join(tuples)


def decode_aggregate3(ret_hex: str) -> list[tuple[bool, str]]:
    """-> [(success, returndata_hex)]"""
    h = ret_hex[2:] if ret_hex.startswith("0x") else ret_hex
    if not h:
        raise ValueError("empty multicall return")
    b = bytes.fromhex(h)

    def word(off: int) -> int:
        return int.from_bytes(b[off:off + 32], "big")

    arr = word(0)
    n = word(arr)
    base = arr + 32
    out: list[tuple[bool, str]] = []
    for i in range(n):
        s = base + word(base + i * 32)
        success = bool(word(s))
        data_off = s + word(s + 32)
        ln = word(data_off)
        out.append((success, "0x" + b[data_off + 32: data_off + 32 + ln].hex()))
    return out


def _words(ret_hex: str) -> list[int]:
    h = ret_hex[2:] if ret_hex.startswith("0x") else ret_hex
    return [int(h[i:i + 64], 16) for i in range(0, len(h) - len(h) % 64, 64)]


def decode_address(ret_hex: str) -> str:
    h = ret_hex[2:] if ret_hex.startswith("0x") else ret_hex
    return "0x" + h[-40:].lower() if len(h) >= 40 else ""


def decode_static_tuple_array(ret_hex: str, width: int) -> list[tuple[int, ...]]:
    """Return data of a function returning `T[]` where T is a static tuple of `width` words."""
    w = _words(ret_hex)
    if len(w) < 2:
        return []
    start = w[0] // 32
    n = w[start]
    body = w[start + 1:]
    return [tuple(body[i * width:(i + 1) * width]) for i in range(n)]


def decode_string(ret_hex: str) -> str:
    h = ret_hex[2:] if ret_hex.startswith("0x") else ret_hex
    try:
        b = bytes.fromhex(h)
        off = int.from_bytes(b[0:32], "big")
        ln = int.from_bytes(b[off:off + 32], "big")
        return b[off + 32: off + 32 + ln].decode("utf-8", "replace")
    except Exception:
        return ""


# =========================
# RPC
# =========================

class RateLimiter:
    def __init__(self, rps: int):
        self.min_interval = 1.0 / max(int(rps), 1)
        self.last = 0.0

    def wait(self):
        delta = time.monotonic() - self.last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self.last = time.monotonic()


_limiter = RateLimiter(RATE_LIMIT_RPS)
_req_id = 0

RATE_LIMIT_MARKERS = (
    "rate limit", "too many", "compute unit", "throughput",
    "capacity", "exceeded", "429",
)


def _next_id() -> int:
    global _req_id
    _req_id += 1
    return _req_id


def _is_rate_limited(err: Any) -> bool:
    if not isinstance(err, dict):
        return False
    if err.get("code") in (429, -32005, -32029):
        return True
    msg = str(err.get("message", "")).lower()
    return any(m in msg for m in RATE_LIMIT_MARKERS)


def _rpc_url() -> str:
    if not ALCHEMY_API_KEY:
        raise RuntimeError("BASESCAN_API_KEY is missing (should contain the Alchemy key)")
    return f"{ALCHEMY_BASE_URL.rstrip('/')}/{ALCHEMY_API_KEY}"


def _post(payload: Any, max_retries: int = 8) -> Any:
    """POST with retries on HTTP 429/5xx AND on rate-limit errors inside the body."""
    url = _rpc_url()
    backoff = 1.0

    for attempt in range(max_retries):
        _limiter.wait()
        try:
            r = requests.post(url, json=payload, timeout=120,
                              headers={"Content-Type": "application/json"})
        except requests.RequestException as e:
            log.warning("network error: %s (attempt %s)", e, attempt + 1)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        if r.status_code == 429 or r.status_code >= 500:
            log.warning("HTTP %s, sleeping %.1fs", r.status_code, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        r.raise_for_status()

        try:
            data = r.json()
        except ValueError as e:
            log.warning("bad JSON: %s", e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        items = data if isinstance(data, list) else [data]
        if any(_is_rate_limited(it.get("error")) for it in items if isinstance(it, dict)):
            log.warning("RPC rate limit inside response, sleeping %.1fs", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        return data

    raise RuntimeError("RPC failed after retries (rate limited)")


def rpc(method: str, params: list) -> Any:
    data = _post({"jsonrpc": "2.0", "id": _next_id(), "method": method, "params": params})
    if isinstance(data, dict) and data.get("error"):
        raise RpcError(str(data["error"]))
    return data.get("result") if isinstance(data, dict) else None


def rpc_batch_tolerant(calls: list[tuple[str, list]]) -> list[tuple[bool, Any]]:
    """JSON-RPC batch where a failing item (e.g. a revert) does not abort the batch."""
    if not calls:
        return []
    payload, order = [], {}
    for i, (method, params) in enumerate(calls):
        rid = _next_id()
        order[rid] = i
        payload.append({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})

    data = _post(payload)
    if isinstance(data, dict):
        data = [data]

    out: list[tuple[bool, Any]] = [(False, None)] * len(calls)
    for item in data:
        idx = order.get(item.get("id"))
        if idx is None:
            continue
        if item.get("error"):
            out[idx] = (False, None)
        else:
            out[idx] = (True, item.get("result"))
    return out


def eth_call(to: str, data: str, block: str = "latest") -> str:
    return rpc("eth_call", [{"to": to, "data": data}, block]) or "0x"


def call_uint(to: str, data: str, block: str = "latest") -> int:
    return hex_to_int(eth_call(to, data, block))


def latest_block() -> int:
    return hex_to_int(rpc("eth_blockNumber", []))


def block_timestamp(n: int) -> int:
    b = rpc("eth_getBlockByNumber", [to_hex(n), False]) or {}
    return hex_to_int(b.get("timestamp", "0x0"))


def multicall(calls: list[tuple[str, str]], block: str) -> list[tuple[bool, str]]:
    """calls: [(target, calldata)] -> [(ok, returndata)], all with allowFailure=True.

    Falls back to a tolerant plain JSON-RPC batch if Multicall3 fails.
    """
    out: list[tuple[bool, str]] = []
    use_multicall = True

    for i in range(0, len(calls), MULTICALL_SIZE):
        chunk = calls[i:i + MULTICALL_SIZE]

        if use_multicall:
            try:
                raw = eth_call(MULTICALL_ADDRESS,
                               encode_aggregate3([(t, True, d) for t, d in chunk]), block)
                decoded = decode_aggregate3(raw)
                if len(decoded) != len(chunk):
                    raise ValueError(f"multicall returned {len(decoded)} of {len(chunk)}")
                out.extend(decoded)
                continue
            except (RpcError, ValueError, IndexError) as e:
                log.warning("multicall failed (%s) - falling back to plain batching", e)
                use_multicall = False

        for j in range(0, len(chunk), CALL_BATCH):
            sub = chunk[j:j + CALL_BATCH]
            res = rpc_batch_tolerant(
                [("eth_call", [{"to": t, "data": d}, block]) for t, d in sub]
            )
            out.extend((ok, r or "0x") for ok, r in res)

    return out


# =========================
# DEPLOY BLOCK
# =========================

def find_deploy_block(address: str, hi: int) -> int:
    lo = 0
    log.info("searching deploy block for %s ...", address)
    while lo < hi:
        mid = (lo + hi) // 2
        code = rpc("eth_getCode", [address, to_hex(mid)]) or "0x"
        if len(code) > 2:
            hi = mid
        else:
            lo = mid + 1
    log.info("deploy block = %s", lo)
    return lo


# =========================
# LOGS
# =========================

def get_logs_chunked(address: str, topics: list, from_block: int, to_block: int) -> list[dict]:
    out: list[dict] = []
    start = from_block
    window = max(LOG_CHUNK, 1)

    while start <= to_block:
        end = min(start + window - 1, to_block)
        params = {
            "address": address,
            "fromBlock": to_hex(start),
            "toBlock": to_hex(end),
            "topics": topics,
        }
        try:
            res = rpc("eth_getLogs", [params]) or []
        except RpcError as e:
            msg = str(e).lower()
            if window > 1 and any(k in msg for k in
                                  ("more than", "limit", "range", "too large",
                                   "exceed", "timeout", "response size")):
                window = max(window // 4, 1)
                log.warning("shrinking log window to %s blocks", window)
                continue
            raise

        out.extend(res)
        log.info("logs %s-%s: +%s (total %s)", start, end, len(res), len(out))
        start = end + 1

        if len(res) < 2000 and window < LOG_CHUNK:
            window = min(window * 2, LOG_CHUNK)

    return out


def parse_logs(raw_logs: list[dict]) -> tuple[dict[int, Stream], list[LogEvent]]:
    """-> (streams with recipient/mint info, LMTS claim + cancel events)."""
    lmts_topic = addr_topic(LMTS_TOKEN)
    streams: dict[int, Stream] = {}
    events: list[LogEvent] = []

    raw_logs = sorted(raw_logs, key=lambda lg: (hex_to_int(lg.get("blockNumber")),
                                                hex_to_int(lg.get("logIndex"))))
    for lg in raw_logs:
        topics = [str(t).lower() for t in (lg.get("topics") or [])]
        if not topics:
            continue
        t0 = topics[0]
        block = hex_to_int(lg.get("blockNumber"))

        if t0 == TOPIC_TRANSFER and len(topics) == 4:
            sid = hex_to_int(topics[3])
            frm, to = topic_to_addr(topics[1]), topic_to_addr(topics[2])
            s = streams.get(sid)
            if s is None:
                s = Stream(id=sid)
                streams[sid] = s
            if frm == ZERO_ADDR:
                s.minted_block = block
            if to == ZERO_ADDR:
                s.burned = True            # keep last owner as recipient
            else:
                s.recipient = to
            continue

        if t0 == TOPIC_WITHDRAW and len(topics) == 4:
            if topics[3] != lmts_topic:
                continue
            events.append(LogEvent(
                block=block,
                log_index=hex_to_int(lg.get("logIndex")),
                tx_hash=str(lg.get("transactionHash", "")).lower(),
                kind="CLAIM",
                stream_id=hex_to_int(topics[1]),
                to=topic_to_addr(topics[2]),
                amount=hex_to_int(lg.get("data")),
            ))
            continue

        if t0 == TOPIC_CANCEL and len(topics) == 4:
            if topics[3] != lmts_topic:
                continue
            w = _words(lg.get("data") or "0x")
            if len(w) < 3:
                continue
            events.append(LogEvent(
                block=block,
                log_index=hex_to_int(lg.get("logIndex")),
                tx_hash=str(lg.get("transactionHash", "")).lower(),
                kind="CANCEL",
                stream_id=w[0],
                to=topic_to_addr(topics[1]),   # refund goes to sender
                amount=w[1],                    # senderAmount (refunded)
            ))

    return streams, events


# =========================
# TIMESTAMPS
# =========================

class BlockClock:
    """Interpolates block -> timestamp from two anchors (Base: fixed 2s blocks)."""

    def __init__(self, b1: int, t1: int, b2: int, t2: int):
        self.b1, self.t1 = b1, t1
        self.slope = (t2 - t1) / (b2 - b1) if b2 > b1 else 2.0
        self.exact: dict[int, int] = {b1: t1, b2: t2}

    def get(self, block: int) -> int:
        if block in self.exact:
            return self.exact[block]
        return int(self.t1 + (block - self.b1) * self.slope)

    def fetch_exact(self, blocks: list[int]) -> None:
        todo = sorted({b for b in blocks if b not in self.exact})
        if not todo:
            return
        log.info("exact timestamps for %s blocks", len(todo))
        for i in range(0, len(todo), CALL_BATCH):
            chunk = todo[i:i + CALL_BATCH]
            res = rpc_batch_tolerant([("eth_getBlockByNumber", [to_hex(b), False]) for b in chunk])
            for b, (ok, r) in zip(chunk, res):
                if ok and r:
                    self.exact[b] = hex_to_int(r.get("timestamp", "0x0"))


# =========================
# STREAM STATE
# =========================

def filter_lmts(streams: dict[int, Stream], block: str) -> dict[int, Stream]:
    ids = sorted(streams)
    res = multicall([(LOCKUP_ADDRESS, encode_uint_call(SEL_GET_UNDERLYING_TOKEN, sid))
                     for sid in ids], block)
    keep = {}
    for sid, (ok, data) in zip(ids, res):
        if ok and decode_address(data) == LMTS_TOKEN:
            keep[sid] = streams[sid]
    log.info("LMTS streams: %s of %s total", len(keep), len(ids))
    return keep


def load_stream_state(streams: dict[int, Stream], block: str) -> None:
    ids = sorted(streams)
    per = len(STREAM_FIELDS)
    calls = [(LOCKUP_ADDRESS, encode_uint_call(sel, sid))
             for sid in ids for _, sel in STREAM_FIELDS]
    res = multicall(calls, block)

    for n, sid in enumerate(ids):
        s = streams[sid]
        r = {name: res[n * per + k] for k, (name, _) in enumerate(STREAM_FIELDS)}

        def u(name: str) -> int:
            ok, d = r[name]
            return hex_to_int(d) if ok else 0

        s.sender = decode_address(r["sender"][1]) if r["sender"][0] else ""
        s.deposited = u("deposited")
        s.withdrawn = u("withdrawn")
        s.refunded = u("refunded")
        s.streamed = u("streamed")
        s.withdrawable = u("withdrawable")
        s.start = u("start")
        s.end = u("end")
        s.status = u("status") if r["status"][0] else -1
        s.cancelable = bool(u("cancelable"))
        s.transferable = bool(u("transferable"))

        # model: detect by which model-specific getter succeeded
        if r["tranches"][0]:
            s.model = "LT"
            s.tranches = [(a, t) for a, t in decode_static_tuple_array(r["tranches"][1], 2)]
        elif r["segments"][0]:
            s.model = "LD"
            s.segments = [(a, e, t) for a, e, t in decode_static_tuple_array(r["segments"][1], 3)]
        elif r["price_gated"][0]:
            s.model = "LPG"
        elif r["cliff"][0]:
            s.model = "LL"
        else:
            s.model = MODEL_FALLBACK.get(u("model_raw"), f"model_{u('model_raw')}")

        if s.model == "LL":
            s.cliff = u("cliff")
            s.granularity = u("granularity")
            ok, d = r["unlock_amounts"]
            if ok:
                w = _words(d)
                if len(w) >= 2:
                    s.unlock_start, s.unlock_cliff = w[0], w[1]

        if (n + 1) % 100 == 0:
            log.info("stream state %s/%s", n + 1, len(ids))


def read_balances(addresses: list[str], block: str) -> dict[str, int]:
    res = multicall([(LMTS_TOKEN, encode_addr_call(SEL_BALANCE_OF, a)) for a in addresses], block)
    return {a: (hex_to_int(d) if ok else 0) for a, (ok, d) in zip(addresses, res)}


# =========================
# VESTING MATH (mirrors Sablier VestingMath)
# =========================

def streamed_at(s: Stream, t: int) -> int:
    """Streamed (unlocked) amount at timestamp t, raw units."""
    if s.status == STATUS_CANCELED:
        return s.deposited - s.refunded           # frozen at cancel time
    if s.start > t:
        return 0
    if t >= s.end:
        return s.deposited

    if s.model == "LT":
        return min(sum(a for a, ts in s.tranches if ts <= t), s.deposited)

    if s.model == "LL":
        if s.cliff > t:
            return s.unlock_start
        unlocked = s.unlock_start + s.unlock_cliff
        if unlocked >= s.deposited:
            return s.deposited
        anchor = s.cliff if s.cliff else s.start
        total = s.end - anchor
        if total <= 0:
            return s.deposited
        elapsed = t - anchor
        gran = s.granularity if s.granularity > 1 else 1
        elapsed = (elapsed // gran) * gran
        return min(unlocked + (s.deposited - unlocked) * elapsed // total, s.deposited)

    if s.model == "LD":
        prev_amounts, prev_ts = 0, s.start
        for amount, exp_raw, seg_ts in s.segments:
            if seg_ts <= t:
                prev_amounts += amount
                prev_ts = seg_ts
                continue
            dur = seg_ts - prev_ts
            if dur <= 0:
                return min(prev_amounts, s.deposited)
            frac = (t - prev_ts) / dur
            exp = exp_raw / 1e18 if exp_raw else 1.0
            cur = int(Decimal(amount) * Decimal(frac ** exp))
            return min(prev_amounts + cur, s.deposited)
        return s.deposited

    # LPG / unknown: can't predict the price; keep the on-chain value until end
    return s.streamed


def unlock_points(s: Stream, now: int, horizon_end: int) -> list[tuple[int, str]]:
    """Timestamps in (now, horizon_end] where something unlocks, with a type label."""
    if s.status in FINISHED_STATUSES or now >= s.end:
        return []

    pts: dict[int, str] = {}

    def add(ts: int, kind: str):
        if now < ts <= horizon_end and ts not in pts:
            pts[ts] = kind

    if s.model == "LT":
        for a, ts in s.tranches:
            if a > 0:
                add(ts, "tranche")
    elif s.model == "LL":
        if s.unlock_start:
            add(s.start, "start unlock")
        if s.cliff:
            add(s.cliff, "cliff")
        anchor = s.cliff or s.start
        if s.granularity >= STEP_MIN_SEC:
            k = max((now - anchor) // s.granularity + 1, 1)
            while len(pts) < MAX_UPCOMING_POINTS:
                ts = anchor + k * s.granularity
                if ts > horizon_end or ts >= s.end:
                    break
                add(ts, "step")
                k += 1
        else:
            t = max(now, anchor)
            while len(pts) < MAX_UPCOMING_POINTS:
                t += 7 * DAY
                if t > horizon_end or t >= s.end:
                    break
                add(t, "linear (7d accrual)")
    elif s.model == "LD":
        for _, _, ts in s.segments:
            add(ts, "segment end")
        t = max(now, s.start)
        while len(pts) < MAX_UPCOMING_POINTS:
            t += 7 * DAY
            if t > horizon_end or t >= s.end:
                break
            add(t, "dynamic (7d accrual)")

    add(s.end, "end" if s.model != "LPG" else "end (price-gated)")
    return sorted(pts.items())


def next_unlock(s: Stream, now: int) -> tuple[int, int, str]:
    """-> (ts, raw_amount, kind) of the next unlock event; (0, 0, '') if none."""
    if s.status in FINISHED_STATUSES or now >= s.end:
        return 0, 0, ""

    cur = streamed_at(s, now)

    if s.model == "LL" and now < s.start and s.unlock_start:
        return s.start, s.unlock_start, "start unlock"

    # continuous linear/dynamic: next 24h accrual (or when streaming begins)
    continuous = (s.model == "LL" and s.granularity < STEP_MIN_SEC) or s.model == "LD"
    if continuous:
        anchor = (s.cliff or s.start) if s.model == "LL" else s.start
        if now >= anchor:
            ts = min(now + DAY, s.end)
            return ts, streamed_at(s, ts) - cur, "continuous (24h)"
        jump = streamed_at(s, anchor) - cur
        if jump > 0:
            return anchor, jump, "cliff" if s.cliff and s.cliff == anchor else "start unlock"
        ts = min(anchor + DAY, s.end)
        return anchor, streamed_at(s, ts) - cur, "streaming starts (first 24h)"

    for ts, kind in unlock_points(s, now, s.end):
        amt = streamed_at(s, ts) - cur
        if amt > 0:
            return ts, amt, kind
    return 0, 0, ""


# =========================
# GOOGLE SHEETS
# =========================

def open_sheet():
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("UNLOCK_SHEET_ID / GOOGLE_SHEET_ID is missing")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is missing")
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)


def ensure_ws(ss, title: str, rows: int = 1000, cols: int = 26):
    try:
        return ss.worksheet(title)
    except WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def write_sheet(ss, title: str, header: list[str], rows: list[list], chunk: int = 2000):
    need_rows = max(len(rows) + 10, 100)
    need_cols = max(len(header), 10)
    ws = ensure_ws(ss, title, rows=need_rows, cols=need_cols)

    if ws.row_count < need_rows or ws.col_count < need_cols:
        ws.resize(rows=max(ws.row_count, need_rows), cols=max(ws.col_count, need_cols))

    ws.clear()
    matrix = [header] + rows

    for start in range(0, len(matrix), chunk):
        part = matrix[start:start + chunk]
        for attempt in range(5):
            try:
                ws.update(values=part, range_name=f"A{start + 1}",
                          value_input_option="USER_ENTERED")
                break
            except APIError as e:
                if attempt == 4:
                    raise
                wait = 2 ** attempt
                log.warning("sheets write retry in %ss: %s", wait, e)
                time.sleep(wait)
        time.sleep(0.3)

    log.info("wrote %s rows -> %s", len(rows), title)


def read_state(ss) -> dict[str, str]:
    ws = ensure_ws(ss, STATE_SHEET, rows=50, cols=4)
    state: dict[str, str] = {}
    for row in ws.get_all_values():
        if len(row) >= 2 and row[0].strip():
            state[row[0].strip().lower()] = row[1].strip()
    return state


def write_state(ss, state: dict[str, str]):
    write_sheet(ss, STATE_SHEET, ["key", "value"],
                [[k, v] for k, v in sorted(state.items())])


def read_labels(ss) -> dict[str, str]:
    """Labels sheet: address | label | notes. Created empty if missing."""
    try:
        ws = ss.worksheet(LABELS_SHEET)
    except WorksheetNotFound:
        ws = ss.add_worksheet(title=LABELS_SHEET, rows=500, cols=5)
        ws.update(values=[["address", "label", "notes"]], range_name="A1")
        log.info("created empty %s sheet", LABELS_SHEET)
        return {}

    values = ws.get_all_values()
    if not values:
        return {}

    headers = [h.strip().lower() for h in values[0]]

    def find_col(*names) -> int:
        for n in names:
            if n in headers:
                return headers.index(n)
        return -1

    addr_col = find_col("address", "wallet", "wallet_address", "adress")
    label_col = find_col("label", "name", "investor", "title")
    if addr_col == -1:
        addr_col, label_col = 0, 1

    out: dict[str, str] = {}
    for row in values[1:]:
        addr = norm_addr(row[addr_col]) if addr_col < len(row) else ""
        if not is_addr(addr):
            continue
        label = str(row[label_col]).strip() if 0 <= label_col < len(row) else ""
        if label:
            out[addr] = label
    return out


# =========================
# MAIN
# =========================

def main():
    ss = open_sheet()
    state = read_state(ss)

    head = latest_block()
    target_block = max(head - CONFIRMATIONS, 0)
    block_hex = to_hex(target_block)
    log.info("head=%s target_block=%s", head, target_block)

    decimals = call_uint(LMTS_TOKEN, SEL_DECIMALS, block_hex) or 18
    symbol = decode_string(eth_call(LMTS_TOKEN, SEL_SYMBOL, block_hex)) or "LMTS"
    scale = Decimal(10) ** decimals

    def tok(raw: int) -> Decimal:
        return Decimal(raw) / scale

    deploy_block = LOCKUP_DEPLOY_BLOCK or int(state.get("deploy_block") or 0)
    if not deploy_block:
        deploy_block = find_deploy_block(LOCKUP_ADDRESS, target_block)
    state["deploy_block"] = str(deploy_block)

    # ---------- logs ----------
    raw = get_logs_chunked(
        LOCKUP_ADDRESS,
        [[TOPIC_TRANSFER, TOPIC_WITHDRAW, TOPIC_CANCEL]],
        deploy_block, target_block,
    )
    all_streams, events = parse_logs(raw)
    next_stream_id = call_uint(LOCKUP_ADDRESS, SEL_NEXT_STREAM_ID, block_hex)
    log.info("streams on contract (all tokens): %s | nextStreamId=%s",
             len(all_streams), next_stream_id)

    clock = BlockClock(deploy_block, block_timestamp(deploy_block),
                       target_block, block_timestamp(target_block))
    now_ts = clock.get(target_block)

    # ---------- stream state ----------
    lmts_streams = filter_lmts(all_streams, block_hex)
    if not lmts_streams:
        log.warning("no LMTS streams found - check LMTS_TOKEN")
        return
    load_stream_state(lmts_streams, block_hex)

    # aggregate check uses ALL LMTS streams (before sender filter)
    agg_onchain = call_uint(LOCKUP_ADDRESS, encode_addr_call(SEL_AGGREGATE_AMOUNT, LMTS_TOKEN), block_hex)
    agg_calc = sum(s.deposited - s.withdrawn - s.refunded for s in lmts_streams.values())
    lockup_balance = call_uint(LMTS_TOKEN, encode_addr_call(SEL_BALANCE_OF, LOCKUP_ADDRESS), block_hex)

    sender_totals: dict[str, list[int]] = {}
    for s in lmts_streams.values():
        st = sender_totals.setdefault(s.sender, [0, 0])
        st[0] += 1
        st[1] += s.deposited - s.refunded

    streams = {sid: s for sid, s in lmts_streams.items()
               if not SENDER_FILTER or s.sender in SENDER_FILTER}
    log.info("streams after sender filter: %s", len(streams))

    labels = read_labels(ss)
    log.info("labels loaded: %s", len(labels))

    def label_of(addr: str) -> str:
        return labels.get(addr, RETAIL_LABEL)

    recipients = sorted({s.recipient for s in streams.values() if s.recipient})
    balances = read_balances(recipients, block_hex)

    # ---------- claims ----------
    events = [e for e in events if e.stream_id in streams]
    last_seen_block = int(state.get("last_scanned_block") or 0)
    new_events = [e for e in events if e.block > last_seen_block] if last_seen_block else []
    clock.fetch_exact([e.block for e in new_events])
    for e in events:
        e.ts = clock.get(e.block)

    claims_by_rcpt: dict[str, list[LogEvent]] = {}
    for e in events:
        if e.kind == "CLAIM":
            claims_by_rcpt.setdefault(streams[e.stream_id].recipient, []).append(e)

    # ---------- per stream calc ----------
    h7, h30, h90 = now_ts + 7 * DAY, now_ts + 30 * DAY, now_ts + 90 * DAY
    calc: dict[int, dict] = {}
    max_drift = 0
    for s in streams.values():
        cur = streamed_at(s, now_ts)
        drift = s.streamed - cur if s.model in ("LL", "LT", "LD") and s.status != STATUS_CANCELED else 0
        max_drift = max(max_drift, abs(drift))
        nts, namt, nkind = next_unlock(s, now_ts)
        calc[s.id] = {
            "cur": cur,
            "drift": drift,
            "next_ts": nts, "next_amt": namt, "next_kind": nkind,
            "u7": max(streamed_at(s, h7) - cur, 0),
            "u30": max(streamed_at(s, h30) - cur, 0),
            "u90": max(streamed_at(s, h90) - cur, 0),
        }

    # =====================
    # STREAMS SHEET
    # =====================
    streams_header = [
        "stream_id", "label", "recipient", "sender", "model", "status", "cancelable",
        "allocated_lmts", "refunded_lmts", "claimed_lmts", "unlocked_lmts", "claimable_now",
        "locked_lmts", "vested_pct", "start_utc", "cliff_utc", "end_utc", "granularity",
        "tranches", "next_unlock_utc", "next_unlock_lmts", "next_unlock_type",
        "calc_drift", "link",
    ]
    stream_rows = []
    for s in sorted(streams.values(), key=lambda x: x.deposited - x.refunded, reverse=True):
        c = calc[s.id]
        alloc = s.deposited - s.refunded
        stream_rows.append([
            s.id,
            label_of(s.recipient),
            s.recipient,
            s.sender,
            s.model,
            STATUS_NAMES.get(s.status, str(s.status)) + (" (burned)" if s.burned else ""),
            "YES" if s.cancelable else "",
            dec_str(tok(alloc)),
            dec_str(tok(s.refunded)),
            dec_str(tok(s.withdrawn)),
            dec_str(tok(s.streamed)),
            dec_str(tok(s.withdrawable)),
            dec_str(tok(max(alloc - s.streamed, 0))),
            dec_str(Decimal(s.streamed) / Decimal(alloc) * 100 if alloc else Decimal(0), 2),
            ts_to_utc(s.start),
            ts_to_utc(s.cliff),
            ts_to_utc(s.end),
            human_days(s.granularity) if s.granularity >= STEP_MIN_SEC else ("continuous" if s.model == "LL" else ""),
            len(s.tranches) if s.tranches else "",
            ts_to_utc(c["next_ts"]),
            dec_str(tok(c["next_amt"])) if c["next_ts"] else "",
            c["next_kind"],
            dec_str(tok(c["drift"])),
            f"{BASESCAN_NFT}{s.id}",
        ])
    write_sheet(ss, SHEET_STREAMS, streams_header, stream_rows)

    # =====================
    # INVESTORS SHEET
    # =====================
    by_rcpt: dict[str, list[Stream]] = {}
    for s in streams.values():
        by_rcpt.setdefault(s.recipient, []).append(s)

    inv_header = [
        "rank", "label", "group", "address", "streams",
        "allocated_lmts", "claimed_lmts", "claimable_now", "locked_lmts", "vested_pct",
        "wallet_balance_lmts", "balance_minus_claimed", "claims_to_other_addr",
        "next_unlock_utc", "days_to_next", "next_unlock_lmts", "next_unlock_type",
        "unlock_next_7d", "unlock_next_30d", "unlock_next_90d",
        "unlock_start_utc", "unlock_end_utc", "unlock_end_year",
        "last_claim_utc", "claims_count", "status", "link",
    ]

    inv_data = []
    for addr, lst in by_rcpt.items():
        alloc = sum(s.deposited - s.refunded for s in lst)
        claimed = sum(s.withdrawn for s in lst)
        unlocked = sum(s.streamed for s in lst)
        claimable = sum(s.withdrawable for s in lst)
        bal = balances.get(addr, 0)

        nexts = [(calc[s.id]["next_ts"], calc[s.id]["next_amt"], calc[s.id]["next_kind"])
                 for s in lst if calc[s.id]["next_ts"]]
        if nexts:
            nts = min(n[0] for n in nexts)
            same = [n for n in nexts if n[0] == nts]
            namt = sum(n[1] for n in same)
            nkind = ", ".join(sorted({n[2] for n in same}))
        else:
            nts, namt, nkind = 0, 0, ""

        active = [s for s in lst if s.status not in FINISHED_STATUSES]
        statuses = sorted({STATUS_NAMES.get(s.status, "?") for s in lst})
        claims = claims_by_rcpt.get(addr, [])
        end_ts = max((s.end for s in lst), default=0)

        inv_data.append((alloc, [
            0,
            label_of(addr),
            "KNOWN" if addr in labels else "RETAIL",
            addr,
            len(lst),
            dec_str(tok(alloc)),
            dec_str(tok(claimed)),
            dec_str(tok(claimable)),
            dec_str(tok(max(alloc - unlocked, 0))),
            dec_str(Decimal(unlocked) / Decimal(alloc) * 100 if alloc else Decimal(0), 2),
            dec_str(tok(bal)),
            dec_str(tok(bal - claimed)),
            "YES" if any(e.to != addr for e in claims) else "",
            ts_to_utc(nts),
            dec_str(Decimal(nts - now_ts) / DAY, 1) if nts else "",
            dec_str(tok(namt)) if nts else "",
            nkind,
            dec_str(tok(sum(calc[s.id]["u7"] for s in lst))),
            dec_str(tok(sum(calc[s.id]["u30"] for s in lst))),
            dec_str(tok(sum(calc[s.id]["u90"] for s in lst))),
            ts_to_utc(min((s.start for s in lst), default=0), "%Y-%m-%d"),
            ts_to_utc(end_ts, "%Y-%m-%d"),
            ts_to_utc(end_ts, "%Y"),
            ts_to_utc(max((e.ts for e in claims), default=0)),
            len(claims),
            "ACTIVE" if active else "/".join(statuses),
            f"{BASESCAN_ADDR}{addr}",
        ]))

    inv_data.sort(key=lambda x: x[0], reverse=True)
    inv_rows = []
    for i, (_, row) in enumerate(inv_data, start=1):
        row[0] = i
        inv_rows.append(row)
    write_sheet(ss, SHEET_INVESTORS, inv_header, inv_rows)

    # =====================
    # UPCOMING SHEET
    # =====================
    horizon_end = now_ts + UPCOMING_DAYS * DAY
    up_header = ["unlock_utc", "days_from_now", "label", "recipient", "stream_id",
                 "amount_lmts", "type", "model", "claimable_now_lmts"]
    up_rows = []
    for s in streams.values():
        prev = calc[s.id]["cur"]
        for ts, kind in unlock_points(s, now_ts, horizon_end):
            v = streamed_at(s, ts)
            amt = v - prev
            prev = v
            if amt <= 0:
                continue
            up_rows.append((ts, [
                ts_to_utc(ts),
                dec_str(Decimal(ts - now_ts) / DAY, 1),
                label_of(s.recipient),
                s.recipient,
                s.id,
                dec_str(tok(amt)),
                kind,
                s.model,
                dec_str(tok(s.withdrawable)),
            ]))
    up_rows.sort(key=lambda x: x[0])
    write_sheet(ss, SHEET_UPCOMING, up_header, [r for _, r in up_rows])

    # =====================
    # CLAIMS SHEET (full rewrite: labels stay current; NEW = since last run)
    # =====================
    claims_header = ["new", "block_time_utc", "block", "action", "stream_id", "label",
                     "recipient", "to", "amount_lmts", "recipient_balance_now",
                     "tx_hash", "link"]
    claim_rows = []
    new_keys = {(e.tx_hash, e.log_index) for e in new_events}
    for e in sorted(events, key=lambda x: (x.block, x.log_index), reverse=True)[:CLAIMS_MAX_ROWS]:
        rcpt = streams[e.stream_id].recipient
        claim_rows.append([
            "NEW" if (e.tx_hash, e.log_index) in new_keys else "",
            ts_to_utc(e.ts),
            e.block,
            e.kind,
            e.stream_id,
            label_of(rcpt),
            rcpt,
            e.to,
            dec_str(tok(e.amount)),
            dec_str(tok(balances.get(rcpt, 0))),
            e.tx_hash,
            f"{BASESCAN_TX}{e.tx_hash}",
        ])
    write_sheet(ss, SHEET_CLAIMS, claims_header, claim_rows)

    # =====================
    # SUMMARY
    # =====================
    tot_alloc = sum(s.deposited - s.refunded for s in streams.values())
    tot_claimed = sum(s.withdrawn for s in streams.values())
    tot_unlocked = sum(s.streamed for s in streams.values())
    tot_claimable = sum(s.withdrawable for s in streams.values())
    tot_refunded = sum(s.refunded for s in streams.values())
    tot_bal = sum(balances.values())
    known = [a for a in by_rcpt if a in labels]
    new_claims = [e for e in new_events if e.kind == "CLAIM"]

    agg_diff = agg_onchain - agg_calc
    summary = [
        ["generated_at_utc", now_utc()],
        ["block", target_block],
        ["block_time_utc", ts_to_utc(now_ts)],
        ["lockup_contract", LOCKUP_ADDRESS],
        ["token", f"{symbol} {LMTS_TOKEN}"],
        ["sender_filter", ", ".join(sorted(SENDER_FILTER)) or "(none - all LMTS streams)"],
        ["", ""],
        ["streams_tracked", len(streams)],
        ["streams_active", sum(1 for s in streams.values() if s.status not in FINISHED_STATUSES)],
        ["investors (unique recipients)", len(by_rcpt)],
        ["investors_labeled", len(known)],
        ["investors_retail", len(by_rcpt) - len(known)],
        ["", ""],
        ["allocated_total", dec_str(tok(tot_alloc))],
        ["unlocked_total", dec_str(tok(tot_unlocked))],
        ["claimed_total", dec_str(tok(tot_claimed))],
        ["claimable_now_total", dec_str(tok(tot_claimable))],
        ["locked_total", dec_str(tok(max(tot_alloc - tot_unlocked, 0)))],
        ["refunded_total (cancels)", dec_str(tok(tot_refunded))],
        ["vested_pct", dec_str(Decimal(tot_unlocked) / Decimal(tot_alloc) * 100 if tot_alloc else Decimal(0), 2)],
        ["recipients_wallet_balance_total", dec_str(tok(tot_bal))],
        ["", ""],
        ["unlock_next_7d", dec_str(tok(sum(c["u7"] for c in calc.values())))],
        ["unlock_next_30d", dec_str(tok(sum(c["u30"] for c in calc.values())))],
        ["unlock_next_90d", dec_str(tok(sum(c["u90"] for c in calc.values())))],
        ["claims_since_last_run", len(new_claims)],
        ["claimed_since_last_run", dec_str(tok(sum(e.amount for e in new_claims)))],
        ["", ""],
        ["--- LMTS STREAMS BY SENDER (all, before filter) ---", ""],
    ]
    for snd, (cnt, amt) in sorted(sender_totals.items(), key=lambda x: x[1][1], reverse=True):
        summary.append([snd or "(unknown)", f"{cnt} streams | {dec_str(tok(amt))} {symbol}"])

    summary += [
        ["", ""],
        ["--- SELF-CHECK ---", ""],
        ["streams_seen_in_logs vs nextStreamId-1", f"{len(all_streams)} vs {max(next_stream_id - 1, 0)}"],
        ["check_1_all_streams_indexed",
         "OK" if len(all_streams) == max(next_stream_id - 1, 0) else "REVIEW"],
        ["aggregateAmount(LMTS) onchain", dec_str(tok(agg_onchain))],
        ["sum(deposited-withdrawn-refunded)", dec_str(tok(agg_calc))],
        ["check_2_aggregate_matches", "OK" if agg_diff == 0 else f"MISMATCH {dec_str(tok(agg_diff))}"],
        ["lockup_LMTS_balance", dec_str(tok(lockup_balance))],
        ["check_3_balance_covers_aggregate", "OK" if lockup_balance >= agg_onchain else "REVIEW"],
        ["max_calc_drift (local math vs streamedAmountOf)", dec_str(tok(max_drift))],
        ["check_4_vesting_math", "OK" if max_drift <= 10 ** max(decimals - 6, 0) else "REVIEW"],
        ["scanned_from_block", deploy_block],
        ["log_events_scanned", len(raw)],
    ]
    write_sheet(ss, SHEET_SUMMARY, ["metric", "value"], summary)

    state["last_scanned_block"] = str(target_block)
    state["last_run_utc"] = now_utc()
    state["last_allocated_total"] = dec_str(tok(tot_alloc))
    state["last_claimed_total"] = dec_str(tok(tot_claimed))
    write_state(ss, state)

    log.info("DONE | streams=%s investors=%s allocated=%s claimed=%s claimable=%s new_claims=%s",
             len(streams), len(by_rcpt), dec_str(tok(tot_alloc)), dec_str(tok(tot_claimed)),
             dec_str(tok(tot_claimable)), len(new_claims))


if __name__ == "__main__":
    main()
