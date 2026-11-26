"""
bot.py
------
টেলিগ্রাম ভিডিও ডাউনলোডার বট — অ্যাডভান্সড, ফাস্ট, অপ্টিমাইজড, লিক-সেফ।

Facebook, Instagram, TikTok, YouTube সহ yt-dlp সাপোর্টেড ১৮০০+ প্ল্যাটফর্মের
লিংক পেস্ট করলে ভিডিও / অডিও (MP3) / সাউন্ড-ছাড়া ভিডিও, quality সিলেকশন সহ
লাইভ প্রোগ্রেস বার দেখিয়ে ডাউনলোড করে পাঠিয়ে দেয়।

মূল ফিচার/অপ্টিমাইজেশন:
    - লাইভ প্রোগ্রেস বার: ডাউনলোড % + স্পিড + ETA (cloud mode এ আপলোড %ও দেখায়)
    - cookies.txt সাপোর্ট (লগইন-প্রয়োজন/এজ-রেস্ট্রিক্টেড কনটেন্টের জন্য)
    - aria2c দিয়ে multi-connection (multi-threaded) দ্রুত ডাউনলোড
    - সব ffmpeg/ffprobe কল -loglevel error -nostats দিয়ে চলে, তাই বড়/লম্বা
      ভিডিওতে বিশাল stderr বাফার জমে RAM leak হয় না
    - ফাইল আপলোড path/chunked-stream দিয়ে হয় (RAM এ পুরো ফাইল একসাথে পড়া হয় না)
    - প্রতি ৫ মিনিটে ব্যাকগ্রাউন্ড maintenance job: expired link cache purge,
      stale ফাইল ডিলিট, gc.collect(), মেমোরি ব্যবহার লগ
    - ডাউনলোড শুরুর আগে ডিস্ক স্পেস চেক — কম থাকলে আগে ক্লিনআপ, তারপরও কম
      থাকলে নিরাপদে বন্ধ করে ইউজারকে জানানো (ক্র্যাশ না করে)
    - ভিডিও টাইটেল ক্যাপশন হিসেবে যোগ হয়

Environment Variables:
    BOT_TOKEN            (required) BotFather থেকে পাওয়া টোকেন
    RENDER_EXTERNAL_URL  (optional) Render এর পাবলিক URL — keep-alive এর জন্য
    TELEGRAM_API_ID      (optional) my.telegram.org থেকে পাওয়া api_id — দিলে Local Bot API
                          সার্ভার ব্যবহার হবে, ফাইল লিমিট ৫০MB থেকে বেড়ে ~২GB হয়ে যাবে
    TELEGRAM_API_HASH    (optional) my.telegram.org থেকে পাওয়া api_hash (উপরেরটার সাথে জোড়ায়)
    LOCAL_BOT_API_URL    (optional) Local Bot API সার্ভারের ঠিকানা (default http://127.0.0.1:8081)
    COOKIES_FILE         (optional) yt-dlp কুকিজ ফাইলের পাথ (private/এজ-রেস্ট্রিক্টেড কনটেন্টের জন্য)
    MAX_CONCURRENT_DL    (optional) একসাথে সর্বোচ্চ কয়টা ডাউনলোড চলবে (default 2)
    MAX_DL_THREADS       (optional) aria2c/fragment ডাউনলোডে কয়টা কানেকশন (default 8)
    MIN_FREE_DISK_MB     (optional) ডাউনলোড শুরুর জন্য ন্যূনতম ফ্রি ডিস্ক (default 500)
"""

from __future__ import annotations

import os
import re
import gc
import sys
import time
import math
import uuid
import shutil
import json
import html
import base64
import struct
import hashlib
import socket
import signal
import copy
import random
import collections
import http.cookiejar
import logging
import asyncio
import threading
import contextlib
import subprocess
import types
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin

import requests
from requests.adapters import HTTPAdapter
import yt_dlp
from yt_dlp.utils import DownloadError, std_headers

try:
    import uvloop  # type: ignore[import-not-found]
    uvloop.install()
    UVLOOP_AVAILABLE = True
except Exception:
    UVLOOP_AVAILABLE = False

try:
    from yt_dlp.networking.impersonate import ImpersonateTarget
except Exception:
    ImpersonateTarget = None  # type: ignore[assignment]

try:
    from curl_cffi import requests as cffi_requests
except Exception:
    cffi_requests = None  # type: ignore[assignment]

try:
    from telethon import TelegramClient, helpers as tl_helpers
    from telethon.crypto import AuthKey
    from telethon.sessions import StringSession
    from telethon.network import MTProtoSender
    try:
        from telethon.network.connection import ConnectionTcpAbridged
    except Exception:
        ConnectionTcpAbridged = None  # type: ignore[assignment]
    from telethon.tl.alltlobjects import LAYER as TL_LAYER
    from telethon.tl.functions import InvokeWithLayerRequest, InvokeWithoutUpdatesRequest, PingRequest
    from telethon.tl.functions.help import GetConfigRequest
    from telethon.tl.functions.auth import (
        ExportAuthorizationRequest,
        ImportAuthorizationRequest,
        ImportBotAuthorizationRequest,
    )
    from telethon.tl.functions.messages import GetMessagesRequest
    from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
    from telethon.tl.types import (
        DocumentAttributeAudio,
        DocumentAttributeVideo,
        InputFile,
        InputFileBig,
        InputMessageID,
    )
    TELETHON_AVAILABLE = True
except Exception:
    TELETHON_AVAILABLE = False

try:
    import cryptg as _cryptg  # noqa: F401
    CRYPTG_AVAILABLE = True
except Exception:
    CRYPTG_AVAILABLE = False
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyParameters
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from keepalive import register_telegram_webhook, start_keepalive

# ---------------------------------------------------------------------------
# কনফিগারেশন
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)  # ভার্বোজ HTTP লগ কমানো
logger = logging.getLogger("bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
COOKIES_FILE = os.environ.get("COOKIES_FILE", "").strip()
if not COOKIES_FILE:
    if os.environ.get("COOKIES_DATA", "").strip():
        try:
            Path("cookies.txt").write_text(os.environ["COOKIES_DATA"].strip(), encoding="utf-8")
            COOKIES_FILE = "cookies.txt"
        except Exception as e:
            logger.warning("Failed to write COOKIES_DATA: %s", e)
    elif Path("cookies.txt").exists():
        COOKIES_FILE = "cookies.txt"
COOKIES_FILE = COOKIES_FILE or None

# yt-dlp ব্যবহারের পর কুকিজ ফাইলে সেশন-আপডেট লিখতে চেষ্টা করে; Render এর Secret
# Files (/etc/secrets/...) read-only মাউন্ট করা থাকে বলে সরাসরি ওখানে লিখতে গেলে
# crash হয় ([Errno 30] Read-only file system)। তাই startup এ COOKIES_FILE কে এই
# writable পাথে কপি করে ACTIVE_COOKIES_FILE সেট করা হয় — yt-dlp সবসময় এটাই ব্যবহার করে।
COOKIES_RUNTIME_PATH = Path("cookies_runtime.txt")
ACTIVE_COOKIES_FILE: Optional[str] = None
YOUTUBE_COOKIES_EXPIRED: bool = False
# PO Token Provider (bgutil-pot) — একই কন্টেইনারে বান্ডল করা আছে (Dockerfile/
# start.sh দেখুন), 127.0.0.1:4416 এ চলে। ডিফল্টে এটাই ব্যবহার হয়; চাইলে env var
# দিয়ে বাইরের কোনো POT provider ও পয়েন্ট করা যায়। সন্দেহ হলে (যেমন "Failed to
# extract any player response" জাতীয় এরর) সম্পূর্ণ বন্ধ করে পরীক্ষা করতে
# DISABLE_POT_PROVIDER=1 সেট করুন — এটাই একমাত্র নিশ্চিত বন্ধ করার উপায়
# (POT_PROVIDER_URL খালি রাখলে start.sh স্বয়ংক্রিয়ভাবে লোকাল সার্ভারের URL
# দিয়ে আবার ভরে দেয়, তাই সেটা দিয়ে বন্ধ করা যায় না)।
DISABLE_POT_PROVIDER = os.environ.get("DISABLE_POT_PROVIDER", "").strip().lower() in ("1", "true", "yes")
POT_PROVIDER_URL = (
    None if DISABLE_POT_PROVIDER
    else os.environ.get("POT_PROVIDER_URL", "http://127.0.0.1:4416").strip().rstrip("/") or None
)
# Render এর ডেটাসেন্টার IP ইউটিউবের বট-ডিটেকশনে বেশি ধরা পড়ে (বাসার/মোবাইল IP
# তুলনামূলক নিরাপদ)। রেসিডেনশিয়াল/মোবাইল proxy থাকলে এখানে দিন, যেমন:
# http://user:pass@proxy-host:port অথবা socks5://user:pass@proxy-host:port
PROXY_URL = os.environ.get("PROXY_URL", "").strip() or None
OWNER_ID = os.environ.get("OWNER_ID", "8753024194").strip()
RAILWAY_API_TOKEN = os.environ.get("RAILWAY_API_TOKEN", "").strip()
RAILWAY_PROJECT_ID = os.environ.get("RAILWAY_PROJECT_ID", "").strip()
RAILWAY_ENVIRONMENT_ID = os.environ.get("RAILWAY_ENVIRONMENT_ID", "").strip()
RAILWAY_SERVICE_ID = os.environ.get("RAILWAY_SERVICE_ID", "").strip()
RENDER_API_KEY = os.environ.get("RENDER_API_KEY", "").strip()
RENDER_SERVICE_ID = os.environ.get("RENDER_SERVICE_ID", "").strip()
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DL", "1"))
MAX_QUEUE_SLOTS = max(1, int(os.environ.get("MAX_QUEUE_SLOTS", "5")))
MAX_DL_THREADS = int(os.environ.get("MAX_DL_THREADS", "28"))
MAX_HTTP_RANGE_THREADS = int(os.environ.get("MAX_HTTP_RANGE_THREADS", "24"))
MAX_UL_THREADS = int(os.environ.get("MAX_UL_THREADS", "32"))
UPLOAD_PIPELINE_DEPTH = int(os.environ.get("UPLOAD_PIPELINE_DEPTH", "5"))
MIN_FREE_DISK_MB = int(os.environ.get("MIN_FREE_DISK_MB", "500"))

TELEGRAM_API_ID = os.environ.get("TELEGRAM_API_ID", "").strip() or os.environ.get("API_ID", "").strip()
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip() or os.environ.get("API_HASH", "").strip()
TELETHON_SESSION_ENV = os.environ.get("TELETHON_SESSION", "").strip()
TELETHON_SESSION_FILE = Path("telethon_session.txt")
TELETHON_CLIENT: Optional["TelegramClient"] = None
MTPROTO_UPLOAD_ENABLED = bool(TELETHON_AVAILABLE and TELEGRAM_API_ID and TELEGRAM_API_HASH)
LOCAL_BOT_API_URL = os.environ.get("LOCAL_BOT_API_URL", "http://127.0.0.1:8081").rstrip("/")
LOCAL_API_ENABLED = bool(
    TELEGRAM_API_ID
    and TELEGRAM_API_HASH
    and os.environ.get("ENABLE_LOCAL_BOT_API", "").strip() in ("1", "true", "yes")
    and shutil.which("telegram-bot-api") is not None
)

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# MTProto / Local Bot API থাকলে সিঙ্গেল-ফাইল আপলোড লিমিট ৫০MB থেকে বেড়ে ~২GB হয়ে যায়
if LOCAL_API_ENABLED or MTPROTO_UPLOAD_ENABLED:
    MAX_TELEGRAM_FILE_SIZE = 1_950 * 1024 * 1024  # ২০০০MB সীমার একটু নিচে
else:
    MAX_TELEGRAM_FILE_SIZE = 49 * 1024 * 1024  # ক্লাউড টেলিগ্রাম বট API এর ৫০MB সীমার একটু নিচে

SPLIT_TARGET_SIZE = int(MAX_TELEGRAM_FILE_SIZE * 0.92)  # প্রতিটা পার্টের টার্গেট সাইজ (মার্জিনসহ)
COMPRESS_TARGET_SIZE = int(MAX_TELEGRAM_FILE_SIZE * 0.93)  # split ব্যর্থ হলে fallback কমপ্রেশনের টার্গেট
COMPRESS_MAX_ATTEMPTS = 3  # প্রথম চেষ্টায় সাইজ না মিললে আরও ছোট bitrate এ পুনরায় চেষ্টা
FFMPEG_TIMEOUT_SECONDS = 1800  # খুব বড় ভিডিও প্রসেস আটকে না থাকার জন্য সেফটি টাইমআউট
LINK_TTL_SECONDS = 7 * 86400  # ৭ দিন পর্যন্ত বাটনের লিংক ক্যাশ সচল থাকবে (+ ডিস্ক/ক্লাউড ও রিপ্লাই অটো-রিকভারি)
INFO_CACHE_TTL_SECONDS = 240  # ৪ মিনিট এক্সট্র্যাক্টেড স্ট্রিম ক্যাশ (ডাবল API কল এড়িয়ে তাৎক্ষণিক ডাউনলোড শুরুর জন্য)
QUALITY_OPTIONS_LIMIT = 6  # কতগুলো resolution বাটন দেখানো হবে
STALE_FILE_AGE_SECONDS = 3600  # ১ ঘন্টার বেশি পুরনো টেম্প ফাইল সেফটি-নেট হিসেবে মোছা হয় (ACTIVE_FILE_IDS এ থাকা ফাইল বাদে, নিচে দেখুন)
MAINTENANCE_INTERVAL_SECONDS = 240  # প্রতি ৪ মিনিটে কিপ-অ্যালাইভ ও ক্লিনআপ জব চলবে (৩০ মিনিটের টোকেন ৬ বার রিট্রাই সুযোগ পাবে)
PROGRESS_MIN_INTERVAL = 1.4  # সেকেন্ড — সিঙ্গেল-ফ্লাইট রিয়েল-টাইম অ্যানিমেশন (৫-স্লট ১-বাই-১ কিউতে সম্পূর্ণ ফ্লাড-সেফ)
PROGRESS_MIN_PERCENT_DELTA = 0.5  # % — সূক্ষ্ম সাব-ব্লক স্মুথ প্রোগ্রেস আপডেট
INDETERMINATE_TICK_SECONDS = 10.0  # local-mode আপলোডে elapsed-time ইন্ডিকেটর কত ঘনঘন আপডেট হবে

URL_REGEX = re.compile(r"https?://\S+")
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
# যে file_id গুলোর ডাউনলোড/প্রসেসিং/আপলোড এখনো চলছে বা রিজিউম কিউতে আছে — এগুলোর ফাইল যত পুরনোই
# মনে হোক, maintenance job বা স্টার্টআপ ক্লিনআপ কখনো মুছবে না।
ACTIVE_FILE_IDS: set[str] = set()
CHAT_FLOOD_UNTIL: dict[int, float] = {}

_LIBC_MALLOC_TRIM: Optional[Callable[[int], int]] = None
try:
    import ctypes

    _libc = ctypes.CDLL("libc.so.6")
    if hasattr(_libc, "malloc_trim"):
        _LIBC_MALLOC_TRIM = _libc.malloc_trim
except Exception:
    _LIBC_MALLOC_TRIM = None


def _trim_process_memory() -> None:
    """Python GC ও Linux glibc malloc_trim(0) চালিয়ে মুক্ত হওয়া মেমোরি সাথে সাথে OS/cgroup-কে ফিরিয়ে দেয়।"""
    with contextlib.suppress(Exception):
        gc.collect()
    if _LIBC_MALLOC_TRIM is not None:
        with contextlib.suppress(Exception):
            _LIBC_MALLOC_TRIM(0)


def _drop_file_page_cache(fd: int, offset: int = 0, length: int = 0, sync_first: bool = False) -> None:
    """Linux cgroup v2-তে বড় ফাইলের পেজ-ক্যাশ জমে কন্টেইনার OOM রিস্টার্ট হওয়া প্রতিরোধ করে।"""
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        return
    with contextlib.suppress(OSError):
        if sync_first and hasattr(os, "fdatasync"):
            with contextlib.suppress(OSError):
                os.fdatasync(fd)  # type: ignore[attr-defined]
        os.posix_fadvise(fd, offset, length, os.POSIX_FADV_DONTNEED)  # type: ignore[attr-defined]


def _drop_path_page_cache(path: Path, sync_first: bool = False) -> None:
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED") or not path.exists():
        return
    with contextlib.suppress(OSError):
        mode = os.O_RDWR if sync_first else os.O_RDONLY
        fd = os.open(str(path), mode)
        try:
            _drop_file_page_cache(fd, 0, 0, sync_first=sync_first)
        finally:
            os.close(fd)


def _note_chat_flood(chat_id: int, retry_after_sec: float) -> None:
    wait_s = max(1.0, float(retry_after_sec))
    CHAT_FLOOD_UNTIL[chat_id] = max(CHAT_FLOOD_UNTIL.get(chat_id, 0.0), time.time() + wait_s + 1.5)


def _chat_flood_remaining(chat_id: int) -> float:
    rem = CHAT_FLOOD_UNTIL.get(chat_id, 0.0) - time.time()
    if rem <= 0:
        CHAT_FLOOD_UNTIL.pop(chat_id, None)
        return 0.0
    return rem


def _extract_flood_seconds(err: BaseException) -> int:
    sec = int(getattr(err, "seconds", 0) or getattr(err, "retry_after", 0) or 0)
    if sec > 0:
        return sec
    m = re.search(r"(?:wait of|retry after)\s+(\d+)\s*s", str(err), re.IGNORECASE)
    return int(m.group(1)) if m else 0


# ---------------------------------------------------------------------------
# লিংক ক্যাশ (callback_data এর ৬৪-বাইট সীমার জন্য পূর্ণ URL এর বদলে short_id পাঠানো হয়)
# TTL পার হওয়া এন্ট্রি lazy-delete ছাড়াও maintenance job থেকে সুইপ হয়, যাতে
# ক্লিক না করা লিংক জমে RAM leak না হয়।
# ---------------------------------------------------------------------------
LINKS_CACHE_FILE = DOWNLOAD_DIR / "_links_cache.json"
_LINKS_CLOUD_SYNC_TASK: Optional[asyncio.Task] = None
_LAST_SYNCED_LINKS_JSON: Optional[str] = None


@dataclass
class _CacheEntry:
    url: str
    source_message_id: Optional[int] = None
    created_at: float = field(default_factory=time.time)

    @property
    def expired(self) -> bool:
        return time.time() - self.created_at > LINK_TTL_SECONDS


class LinkCache:
    def __init__(self) -> None:
        self._store: dict[str, _CacheEntry] = {}

    def put(self, url: str, source_message_id: Optional[int] = None, short_id: Optional[str] = None) -> str:
        sid = short_id or uuid.uuid4().hex[:8]
        self._store[sid] = _CacheEntry(url=url, source_message_id=source_message_id)
        if len(self._store) > 1000:
            oldest = sorted(self._store.items(), key=lambda kv: kv[1].created_at)[:250]
            for old_k, _ in oldest:
                self._store.pop(old_k, None)
        _persist_links_cache()
        return sid

    def get(self, short_id: str) -> Optional[str]:
        entry = self.get_entry(short_id)
        return entry.url if entry is not None else None

    def get_entry(self, short_id: str) -> Optional[_CacheEntry]:
        entry = self._store.get(short_id)
        if entry is None:
            return None
        if entry.expired:
            self._store.pop(short_id, None)
            return None
        return entry

    def purge_expired(self) -> int:
        expired_ids = [sid for sid, entry in self._store.items() if entry.expired]
        for sid in expired_ids:
            self._store.pop(sid, None)
        if expired_ids:
            _persist_links_cache()
        return len(expired_ids)


@dataclass
class _BatchEntry:
    urls: list[str]
    source_message_id: Optional[int] = None
    created_at: float = field(default_factory=time.time)

    @property
    def expired(self) -> bool:
        return time.time() - self.created_at > LINK_TTL_SECONDS


class BatchCache:
    def __init__(self) -> None:
        self._store: dict[str, _BatchEntry] = {}

    def put(self, urls: list[str], source_message_id: Optional[int] = None, batch_id: Optional[str] = None) -> str:
        bid = batch_id or uuid.uuid4().hex[:8]
        self._store[bid] = _BatchEntry(urls=urls, source_message_id=source_message_id)
        if len(self._store) > 200:
            oldest = sorted(self._store.items(), key=lambda kv: kv[1].created_at)[:50]
            for old_k, _ in oldest:
                self._store.pop(old_k, None)
        _persist_links_cache()
        return bid

    def get(self, batch_id: str) -> Optional[list[str]]:
        entry = self.get_entry(batch_id)
        return entry.urls if entry is not None else None

    def get_entry(self, batch_id: str) -> Optional[_BatchEntry]:
        entry = self._store.get(batch_id)
        if entry is None:
            return None
        if entry.expired:
            self._store.pop(batch_id, None)
            return None
        return entry

    def purge_expired(self) -> int:
        expired_ids = [bid for bid, entry in self._store.items() if entry.expired]
        for bid in expired_ids:
            self._store.pop(bid, None)
        if expired_ids:
            _persist_links_cache()
        return len(expired_ids)


def _reply_params(message_id: Optional[int]) -> Optional[ReplyParameters]:
    if message_id and int(message_id) > 0:
        return ReplyParameters(message_id=int(message_id), allow_sending_without_reply=True)
    return None


# ---------------------------------------------------------------------------
# ডুপ্লিকেট লিংক ডিটেকশন ও পারসিস্টেন্ট ডাউনলোড হিস্ট্রি
# ---------------------------------------------------------------------------
DOWNLOAD_HISTORY_FILE = Path("download_history.json")
DOWNLOAD_HISTORY: dict[str, dict] = {}
_HISTORY_SYNC_TASK: Optional[asyncio.Task] = None
_LAST_SYNCED_HISTORY_JSON: Optional[str] = None


def _canonical_history_key(stored_url: str) -> str:
    """যেকোনো প্ল্যাটফর্মের লিংককে একটি স্থায়ী ইউনিক কী-তে রূপান্তর করে যাতে একই ভিডিও দ্বিতীয়বার চেনা যায়।"""
    u, _ = _unpack_stored_url(stored_url)
    u = u.strip()
    if not u:
        return ""
    try:
        parsed = urlparse(u)
        host = (parsed.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = parsed.path or ""

        # ১. Aylo / site-ma.* মেম্বার পোর্টাল (scene ID ভিত্তিক)
        if host.startswith("site-ma.") or any(host.endswith(d) for d in ("brazzers.com", "spicevids.com", "realitykings.com", "mofos.com", "babes.com", "twistys.com", "digitalplayground.com", "wicked.com", "bangbros.com")):
            m_sc = re.search(r"/(?:scene|video/(?:view/)?|movie|serie)/(\d+)", path, re.IGNORECASE)
            if not m_sc:
                m_sc = re.search(r"/video/[A-Za-z0-9_-]+/(\d+)", path, re.IGNORECASE)
            if m_sc:
                parts_h = host.split(".")
                bdom = ".".join(parts_h[-2:]) if len(parts_h) >= 2 else host
                return f"aylo:{bdom}:{m_sc.group(1)}"

        # ২. YouTube (video ID ভিত্তিক)
        if host == "youtu.be":
            vid = path.strip("/").split("/")[0]
            if vid:
                return f"yt:{vid}"
        if host.endswith("youtube.com"):
            qs = parse_qs(parsed.query)
            if qs.get("v") and qs["v"][0]:
                return f"yt:{qs['v'][0]}"
            m_yt = re.search(r"/(?:shorts|live|embed)/([A-Za-z0-9_-]{6,})", path)
            if m_yt:
                return f"yt:{m_yt.group(1)}"

        # ৩. Facebook / Instagram / TikTok শর্ট আইডি
        if "facebook.com" in host or host == "fb.watch":
            m_fb = re.search(r"/(?:reel|videos)/(\d+)", path)
            if m_fb:
                return f"fb:{m_fb.group(1)}"
            qs = parse_qs(parsed.query)
            if qs.get("v") and qs["v"][0]:
                return f"fb:{qs['v'][0]}"
        if "instagram.com" in host:
            m_ig = re.search(r"/(?:reel|reels|p|tv)/([A-Za-z0-9_-]+)", path)
            if m_ig:
                return f"ig:{m_ig.group(1)}"
        if "tiktok.com" in host:
            m_tt = re.search(r"/video/(\d+)", path)
            if m_tt:
                return f"tt:{m_tt.group(1)}"

        # ৪. সাধারণ / CDN / .m3u8 লিংক — ক্ষণস্থায়ী সিগনেচার ও ট্র্যাকিং প্যারামিটার বাদ দিয়ে পাথ নরমালাইজ
        qs_raw = parse_qs(parsed.query, keep_blank_values=False)
        VOLATILE_KEYS = {
            "validto", "expires", "exp", "e", "hash", "token", "sig", "signature", "ip", "s", "hdnea",
            "rdid", "share_url", "mibextid", "fbclid", "ref", "refsrc", "igsh", "si", "feature",
        }
        kept_q = {
            k: v for k, v in qs_raw.items()
            if k.lower() not in VOLATILE_KEYS and not k.lower().startswith("utm_")
        }
        clean_q = urlencode(sorted(kept_q.items()), doseq=True)
        norm_path = path.rstrip("/") or "/"
        return f"url:{host}{norm_path}" + (f"?{clean_q}" if clean_q else "")
    except Exception:
        return u.lower().rstrip("/")


def _load_download_history_on_boot() -> None:
    raw = ""
    if DOWNLOAD_HISTORY_FILE.exists():
        with contextlib.suppress(Exception):
            raw = DOWNLOAD_HISTORY_FILE.read_text(encoding="utf-8").strip()
    if not raw:
        raw = os.environ.get("DOWNLOAD_HISTORY", "").strip()
    if not raw:
        return
    with contextlib.suppress(Exception):
        data = json.loads(raw)
        if isinstance(data, dict):
            DOWNLOAD_HISTORY.clear()
            for k, v in data.items():
                if isinstance(k, str) and isinstance(v, dict):
                    DOWNLOAD_HISTORY[k] = v
            logger.info("📚 ডাউনলোড হিস্ট্রি লোড হয়েছে: %dটি পূর্ববর্তী ভিডিও রেকর্ড সচল।", len(DOWNLOAD_HISTORY))


def _serialize_download_history() -> str:
    if len(DOWNLOAD_HISTORY) > 1000:
        sorted_items = sorted(DOWNLOAD_HISTORY.items(), key=lambda kv: float(kv[1].get("ts") or 0), reverse=True)[:1000]
        DOWNLOAD_HISTORY.clear()
        DOWNLOAD_HISTORY.update(dict(sorted_items))
    return json.dumps(DOWNLOAD_HISTORY, ensure_ascii=False, separators=(",", ":"))


def _write_history_disk_sync(serialized: str) -> None:
    tmp = DOWNLOAD_HISTORY_FILE.with_suffix(".tmp")
    with contextlib.suppress(OSError):
        tmp.write_text(serialized, encoding="utf-8")
        tmp.replace(DOWNLOAD_HISTORY_FILE)


async def _debounced_history_cloud_sync() -> None:
    global _LAST_SYNCED_HISTORY_JSON
    await asyncio.sleep(3.0)
    serialized = _serialize_download_history()
    if len(serialized) > 28000:
        sorted_items = sorted(DOWNLOAD_HISTORY.items(), key=lambda kv: float(kv[1].get("ts") or 0), reverse=True)[:260]
        serialized = json.dumps(dict(sorted_items), ensure_ascii=False, separators=(",", ":"))
    if serialized == _LAST_SYNCED_HISTORY_JSON:
        return
    _LAST_SYNCED_HISTORY_JSON = serialized
    os.environ["DOWNLOAD_HISTORY"] = serialized
    await asyncio.get_running_loop().run_in_executor(None, _sync_cloud_env_var, "DOWNLOAD_HISTORY", serialized)


def _record_completed_download(chat_id: int, stored_url: str, title: Optional[str], mode: str, quality: str) -> None:
    global _HISTORY_SYNC_TASK
    ckey = _canonical_history_key(stored_url)
    if not ckey:
        return
    DOWNLOAD_HISTORY[f"{chat_id}:{ckey}"] = {
        "title": (title or "")[:120],
        "ts": round(time.time(), 1),
        "mode": mode,
        "quality": quality,
    }
    try:
        serialized = _serialize_download_history()
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, _write_history_disk_sync, serialized)
        if _HISTORY_SYNC_TASK is None or _HISTORY_SYNC_TASK.done():
            _HISTORY_SYNC_TASK = loop.create_task(_debounced_history_cloud_sync())
    except Exception as e:
        logger.debug("Download history save warning: %s", e)


def _lookup_previous_download(chat_id: int, stored_url: str) -> Optional[dict]:
    ckey = _canonical_history_key(stored_url)
    if not ckey:
        return None
    # বর্তমানে চলমান বা অপেক্ষমাণ কিউতে একই লিংক আছে কিনা চেক
    active_list = ([CURRENT_RUNNING_JOB] if (CURRENT_RUNNING_JOB and not CURRENT_RUNNING_JOB.is_cancelled()) else []) + [
        w for w in WAITING_QUEUE if not w.is_cancelled()
    ]
    for aj in active_list:
        if aj.chat_id == chat_id and _canonical_history_key(aj.url) == ckey:
            return {"title": aj.title or "", "in_queue": True}
    rec = DOWNLOAD_HISTORY.get(f"{chat_id}:{ckey}")
    if rec is not None:
        return {**rec, "in_queue": False}
    return None


class DownloadCancelledError(BaseException):
    """ইউজার ডাউনলোড বা আপলোড ক্যানসেল করলে তাৎক্ষণিকভাবে সব থ্রেড ও রিট্রাই থামানোর সিগন্যাল।"""


def _cancel_kb(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ ডাউনলোড বাতিল করুন (Cancel)", callback_data=f"cancel|{job_id}")]]
    )


LINKS = LinkCache()
BATCHES = BatchCache()
EXTRACT_INFO_CACHE: dict[str, tuple[float, dict]] = {}


def _serialize_links_cache(max_links: int = 2000, max_batches: int = 400) -> str:
    l_items = sorted(LINKS._store.items(), key=lambda kv: kv[1].created_at, reverse=True)[:max_links]
    b_items = sorted(BATCHES._store.items(), key=lambda kv: kv[1].created_at, reverse=True)[:max_batches]
    payload = {
        "links": {
            sid: {"u": e.url, "m": e.source_message_id, "t": round(e.created_at, 1)}
            for sid, e in l_items
            if not e.expired
        },
        "batches": {
            bid: {"u": b.urls, "m": b.source_message_id, "t": round(b.created_at, 1)}
            for bid, b in b_items
            if not b.expired
        },
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


async def _debounced_links_cloud_sync() -> None:
    global _LAST_SYNCED_LINKS_JSON
    await asyncio.sleep(2.0)
    serialized = _serialize_links_cache(max_links=180, max_batches=30)
    if len(serialized) > 28000:
        serialized = _serialize_links_cache(max_links=90, max_batches=15)
    if serialized == _LAST_SYNCED_LINKS_JSON:
        return
    _LAST_SYNCED_LINKS_JSON = serialized
    os.environ["PERSISTED_LINKS"] = serialized
    await asyncio.get_running_loop().run_in_executor(None, _sync_cloud_env_var, "PERSISTED_LINKS", serialized)


def _persist_links_cache() -> None:
    global _LINKS_CLOUD_SYNC_TASK
    try:
        disk_json = _serialize_links_cache()
        tmp = LINKS_CACHE_FILE.with_suffix(".tmp")
        with contextlib.suppress(OSError):
            tmp.write_text(disk_json, encoding="utf-8")
            tmp.replace(LINKS_CACHE_FILE)
        with contextlib.suppress(RuntimeError):
            loop = asyncio.get_running_loop()
            if _LINKS_CLOUD_SYNC_TASK is None or _LINKS_CLOUD_SYNC_TASK.done():
                _LINKS_CLOUD_SYNC_TASK = loop.create_task(_debounced_links_cloud_sync())
    except Exception as e:
        logger.debug("Links cache persist warning: %s", e)


def _load_links_cache_on_boot() -> None:
    sources: list[str] = []
    if LINKS_CACHE_FILE.exists():
        with contextlib.suppress(Exception):
            s_disk = LINKS_CACHE_FILE.read_text(encoding="utf-8").strip()
            if s_disk:
                sources.append(s_disk)
    s_env = os.environ.get("PERSISTED_LINKS", "").strip()
    if s_env:
        sources.append(s_env)

    now = time.time()
    loaded_l = 0
    loaded_b = 0
    for raw in reversed(sources):
        with contextlib.suppress(Exception):
            data = json.loads(raw)
            if not isinstance(data, dict):
                continue
            for sid, obj in (data.get("links") or {}).items():
                if isinstance(sid, str) and isinstance(obj, dict) and obj.get("u"):
                    ts = float(obj.get("t") or now)
                    if now - ts <= LINK_TTL_SECONDS:
                        LINKS._store[sid] = _CacheEntry(
                            url=str(obj["u"]),
                            source_message_id=int(obj["m"]) if obj.get("m") else None,
                            created_at=ts,
                        )
                        loaded_l += 1
            for bid, obj in (data.get("batches") or {}).items():
                if isinstance(bid, str) and isinstance(obj, dict) and isinstance(obj.get("u"), list):
                    ts = float(obj.get("t") or now)
                    if now - ts <= LINK_TTL_SECONDS:
                        BATCHES._store[bid] = _BatchEntry(
                            urls=[str(x) for x in obj["u"] if x],
                            source_message_id=int(obj["m"]) if obj.get("m") else None,
                            created_at=ts,
                        )
                        loaded_b += 1
    if LINKS._store or BATCHES._store:
        logger.info("🔗 লিংক ক্যাশ লোড হয়েছে: %dটি সিঙ্গেল লিংক ও %dটি বাল্ক ব্যাচ সচল।", len(LINKS._store), len(BATCHES._store))


def _get_cached_extract_info(key: str) -> Optional[dict]:
    item = EXTRACT_INFO_CACHE.get(key)
    if not item:
        return None
    ts, data = item
    if time.time() - ts > INFO_CACHE_TTL_SECONDS:
        EXTRACT_INFO_CACHE.pop(key, None)
        return None
    return data


def _put_cached_extract_info(key: str, data: dict) -> None:
    if data and isinstance(data, dict):
        now = time.time()
        EXTRACT_INFO_CACHE[key] = (now, data)
        if len(EXTRACT_INFO_CACHE) > 32:
            _purge_expired_extract_info()
            if len(EXTRACT_INFO_CACHE) > 32:
                oldest_keys = sorted(EXTRACT_INFO_CACHE.items(), key=lambda kv: kv[1][0])[: len(EXTRACT_INFO_CACHE) - 24]
                for old_k, _ in oldest_keys:
                    EXTRACT_INFO_CACHE.pop(old_k, None)


def _purge_expired_extract_info() -> None:
    now = time.time()
    stale = [k for k, (ts, _) in EXTRACT_INFO_CACHE.items() if now - ts > INFO_CACHE_TTL_SECONDS]
    for k in stale:
        EXTRACT_INFO_CACHE.pop(k, None)


# ---------------------------------------------------------------------------
# ফরম্যাটিং হেল্পার — সাব-ব্লক হাই-প্রিসিশন প্রোগ্রেস বার, সাইজ, ETA
# ---------------------------------------------------------------------------
_BAR_SUBBLOCKS = ("", "▏", "▎", "▍", "▌", "▋", "▊", "▉")
_ANIM_FRAMES = ("⚡", "🚀", "🔥", "💨")


def _format_bar(percent: float, width: int = 14) -> str:
    percent = max(0.0, min(100.0, percent))
    exact = width * (percent / 100.0)
    full = int(exact)
    if full >= width:
        return "█" * width
    rem_idx = min(7, max(0, int((exact - full) * 8)))
    sub = _BAR_SUBBLOCKS[rem_idx]
    if sub:
        return ("█" * full) + sub + ("░" * (width - full - 1))
    return ("█" * full) + ("░" * (width - full))


def _format_wave_bar(frame_idx: int, width: int = 14) -> str:
    seg = 4
    pos = frame_idx % width
    chars = ["░"] * width
    for i in range(seg):
        chars[(pos + i) % width] = "█" if i in (1, 2) else "▓"
    return "".join(chars)


def _format_size(n: Optional[float]) -> str:
    if not n or n <= 0:
        return "?"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def _format_eta(seconds: Optional[float]) -> str:
    if seconds is None or seconds < 0:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"


# ---------------------------------------------------------------------------
# রিয়েল-টাইম সিঙ্গেল-ফ্লাইট প্রোগ্রেস অ্যানিমেশন ইঞ্জিন (ProgressTracker v2)
# - থ্রেড-সেফ রোলিং উইন্ডো EWMA লাইভ স্পিড (রিজিউম চেকপয়েন্টেও ১০০% নিখুঁত স্পিড)
# - জিরো কো-রুটিন পাইলআপ ও জিরো আউট-অফ-অর্ডার এডিট (coalesced single-flight pump)
# - ফেজ শেষ হলে close() কলের মাধ্যমে পুরনো মেসেজ ওভাররাইট প্রতিরোধ
# ---------------------------------------------------------------------------
class ProgressTracker:
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        bot,
        chat_id: int,
        message_id: int,
        label: str,
        reply_markup: Optional[InlineKeyboardMarkup] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.loop = loop
        self.bot = bot
        self.chat_id = chat_id
        self.message_id = message_id
        self.label = label
        self.reply_markup = reply_markup
        self.cancel_check = cancel_check
        self._lock = threading.Lock()
        self._closed = False
        self._latest_state: Optional[tuple[int, int, float, Optional[float]]] = None
        self._samples: collections.deque[tuple[float, int]] = collections.deque(maxlen=24)
        self._ewma_speed: float = 0.0
        self._last_edit_mono: float = time.monotonic() - (PROGRESS_MIN_INTERVAL - 0.35)
        self._last_percent: float = -100.0
        self._last_rendered_text: str = ""
        self._edit_count: int = 0
        self._frame_idx: int = 0
        self._wake_event: Optional[asyncio.Event] = None
        self._pump_task: Optional[asyncio.Task] = None

    def report(self, downloaded: int, total: int, speed: Optional[float] = None, eta: Optional[float] = None) -> None:
        if self._closed or (self.cancel_check and self.cancel_check()):
            return
        if downloaded <= 0 and (not total or total <= 0):
            return
        now_m = time.monotonic()
        with self._lock:
            if self._closed:
                return
            if downloaded >= 0:
                if self._samples and downloaded < self._samples[-1][1]:
                    self._samples.clear()
                    self._ewma_speed = 0.0
                if (
                    not self._samples
                    or (now_m - self._samples[-1][0]) >= 0.12
                    or (total and downloaded >= total)
                ):
                    self._samples.append((now_m, downloaded))
                while len(self._samples) > 2 and (now_m - self._samples[0][0]) > 2.6:
                    self._samples.popleft()
                if len(self._samples) >= 2:
                    dt = now_m - self._samples[0][0]
                    db = downloaded - self._samples[0][1]
                    if dt >= 0.18 and db >= 0:
                        inst = db / dt
                        self._ewma_speed = inst if self._ewma_speed <= 0.0 else (0.72 * inst + 0.28 * self._ewma_speed)
            eff_spd = self._ewma_speed if self._ewma_speed > 0.0 else float(speed or 0.0)
            if total and total > downloaded and eff_spd > 0.0:
                eff_eta: Optional[float] = (total - downloaded) / eff_spd
            elif total and downloaded >= total:
                eff_eta = 0.0
            else:
                eff_eta = eta
            self._latest_state = (downloaded, total, eff_spd, eff_eta)

        with contextlib.suppress(RuntimeError):
            self.loop.call_soon_threadsafe(self._notify_loop)

    def _notify_loop(self) -> None:
        if self._closed:
            return
        if self._wake_event is None:
            self._wake_event = asyncio.Event()
        self._wake_event.set()
        if self._pump_task is None or self._pump_task.done():
            self._pump_task = self.loop.create_task(self._pump_loop())

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        with contextlib.suppress(RuntimeError):
            self.loop.call_soon_threadsafe(self._stop_on_loop)

    def _stop_on_loop(self) -> None:
        if self._wake_event is not None:
            self._wake_event.set()
        if self._pump_task is not None and not self._pump_task.done():
            self._pump_task.cancel()

    def _build_card_html(self, downloaded: int, total: int, spd: float, eta: Optional[float]) -> str:
        icon = _ANIM_FRAMES[self._frame_idx % len(_ANIM_FRAMES)]
        self._frame_idx += 1
        clean_lbl = html.escape(self.label.lstrip("⚡ ").strip())
        if total and total > 0:
            percent = max(0.0, min(100.0, downloaded / total * 100.0))
            lines = [
                f"{icon} <b>{clean_lbl}...</b> <code>{percent:5.1f}%</code>",
                f"<code>[{_format_bar(percent)}]</code>",
                f"📦 <code>{_format_size(downloaded)} / {_format_size(total)}</code>",
            ]
        else:
            lines = [
                f"{icon} <b>{clean_lbl}...</b>",
                f"<code>[{_format_wave_bar(self._frame_idx)}]</code>",
                f"📦 <code>{_format_size(downloaded)}</code>",
            ]
        meta_parts: list[str] = []
        if spd and spd > 0:
            meta_parts.append(f"🚀 <code>{_format_size(spd)}/s</code>")
        if eta is not None:
            meta_parts.append(f"⏳ বাকি: <code>{_format_eta(eta)}</code>")
        if meta_parts:
            lines.append(" • ".join(meta_parts))
        return "\n".join(lines)

    async def _pump_loop(self) -> None:
        idle_rounds = 0
        try:
            while not self._closed:
                if self.cancel_check and self.cancel_check():
                    return
                assert self._wake_event is not None
                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=2.5)
                    idle_rounds = 0
                except asyncio.TimeoutError:
                    idle_rounds += 1
                    if idle_rounds >= 5:
                        return
                    continue

                if self._closed or (self.cancel_check and self.cancel_check()):
                    return

                min_iv = PROGRESS_MIN_INTERVAL if self._edit_count < 14 else 2.1
                now_m = time.monotonic()
                with self._lock:
                    snap = self._latest_state
                if snap is None:
                    self._wake_event.clear()
                    continue

                dl_0, tot_0, _, _ = snap
                is_done_0 = bool(tot_0 and dl_0 >= tot_0)
                wait_iv = (self._last_edit_mono + min_iv) - now_m
                if wait_iv > 0.02 and not (is_done_0 and self._last_percent < 99.3 and (now_m - self._last_edit_mono) >= 0.7):
                    await asyncio.sleep(min(wait_iv, 2.2))
                    if self._closed or (self.cancel_check and self.cancel_check()):
                        return

                self._wake_event.clear()
                with self._lock:
                    state = self._latest_state
                if state is None:
                    continue

                downloaded, total, spd, eta = state
                if _chat_flood_remaining(self.chat_id) > 0:
                    await asyncio.sleep(1.0)
                    continue

                percent = (downloaded / total * 100.0) if total else 0.0
                is_done = bool(total and downloaded >= total)
                now_m2 = time.monotonic()
                if not is_done and total > 0:
                    if abs(percent - self._last_percent) < PROGRESS_MIN_PERCENT_DELTA and (now_m2 - self._last_edit_mono) < 3.2:
                        continue
                elif is_done and self._last_percent >= 99.6:
                    continue

                text = self._build_card_html(downloaded, total, spd, eta)
                if text == self._last_rendered_text:
                    continue
                self._last_rendered_text = text
                self._last_edit_mono = now_m2
                self._last_percent = percent
                self._edit_count += 1
                await self._safe_edit(text)
        except asyncio.CancelledError:
            return

    async def _safe_edit(self, text: str) -> None:
        if self._closed or (self.cancel_check and self.cancel_check()):
            return
        if _chat_flood_remaining(self.chat_id) > 0:
            return
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=text,
                parse_mode="HTML",
                reply_markup=self.reply_markup,
            )
        except RetryAfter as ra:
            sec = _extract_flood_seconds(ra) or 5
            _note_chat_flood(self.chat_id, sec)
            logger.warning("Progress edit flood cooldown (%ds) — সাময়িকভাবে প্রোগ্রেস মেসেজ এডিট বিরত থাকবে।", sec)
        except TelegramError as e:
            sec = _extract_flood_seconds(e)
            if sec > 0:
                _note_chat_flood(self.chat_id, sec)
            elif "not modified" not in str(e).lower():
                logger.debug("progress edit failed: %s", e)


def _make_ytdlp_progress_hook(
    tracker: ProgressTracker,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> Callable[[dict], None]:
    def hook(d: dict) -> None:
        if cancel_check and cancel_check():
            raise DownloadCancelledError("ডাউনলোড বাতিল করা হয়েছে")
        if d.get("status") != "downloading":
            return
        downloaded = d.get("downloaded_bytes") or 0
        total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
        tracker.report(downloaded, total, speed=d.get("speed"), eta=d.get("eta"))
    return hook


class _ProgressFile:
    """
    একটা ফাইলকে wrap করে chunk-by-chunk read এর সাথে progress callback জোড়া লাগায়।
    RAM এ পুরো ফাইল একসাথে লোড হয় না (httpx যতটুকু চায় ততটুকুই read হয়), তাই বড়
    ফাইলেও মেমোরি ব্যবহার বাউন্ডেড থাকে। অন্য সব অ্যাট্রিবিউট (seek/tell ইত্যাদি)
    আসল ফাইল অবজেক্টে ডেলিগেট হয়।
    """

    def __init__(
        self,
        path: Path,
        total: int,
        on_read: Callable[[int, int], None],
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        self._f = open(path, "rb")
        self.name = path.name
        self._total = total
        self._read_bytes = 0
        self._on_read = on_read
        self._cancel_check = cancel_check

    def read(self, size: int = -1) -> bytes:
        if self._cancel_check and self._cancel_check():
            raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
        chunk = self._f.read(size)
        self._read_bytes += len(chunk)
        try:
            self._on_read(self._read_bytes, self._total)
        except Exception:
            pass  # প্রোগ্রেস-রিপোর্টিং কখনোই আপলোড ভাঙবে না
        return chunk

    def __getattr__(self, item):
        return getattr(self._f, item)

    def close(self) -> None:
        self._f.close()


async def _run_indeterminate_ticker(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    label: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> asyncio.Task:
    """
    Local Bot API মোডে সত্যিকারের বাইট-লেভেল আপলোড % জানা সম্ভব না (ফাইল path
    দিয়ে সরাসরি লোকাল সার্ভার আপলোড করে) — তাই এখানে elapsed-time সহ একটা
    animated indicator দেখানো হয়, যাতে ইউজার বুঝতে পারে কাজ চলছে, ফেক % না দেখিয়ে।
    """
    frames = ["📤", "⬆️", "🔼", "⬆️"]

    async def ticker() -> None:
        i = 0
        start = time.time()
        while True:
            if cancel_check and cancel_check():
                return
            elapsed = int(time.time() - start)
            text = f"{frames[i % len(frames)]} {label}... (চলছে {_format_eta(elapsed)})"
            with contextlib.suppress(TelegramError):
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    reply_markup=reply_markup,
                )
            i += 1
            await asyncio.sleep(INDETERMINATE_TICK_SECONDS)

    return asyncio.create_task(ticker())


async def _cancel_task(task: asyncio.Task) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# টুল-অ্যাভেইলেবিলিটি — এনভায়রনমেন্টে aria2c/ffmpeg না থাকলেও যাতে crash না করে,
# শুধু সংশ্লিষ্ট ফিচারটা silently কম-অপ্টিমাইজড মোডে চলে যায়
# ---------------------------------------------------------------------------
ARIA2C_AVAILABLE = shutil.which("aria2c") is not None
FFMPEG_AVAILABLE = shutil.which("ffmpeg") is not None
FFPROBE_AVAILABLE = shutil.which("ffprobe") is not None


# ---------------------------------------------------------------------------
# yt-dlp হেল্পার — aria2c দিয়ে multi-connection ফাস্ট ডাউনলোড + কুকিজ সাপোর্ট
# ---------------------------------------------------------------------------
# player_client নিয়ে বারবার হাতে-টিউন করা client list (tv/android_sdkless/
# web_safari ইত্যাদি) yt-dlp এর নিজস্ব actively-maintained ডিফল্টের সাথে না মিলে
# উল্টো সমস্যা তৈরি করছিল। yt-dlp এখন নিজে থেকেই স্মার্ট ডিফল্ট বেছে নেয়
# (unauthenticated: ios+mweb; cookies সহ: web_creator+mweb) এবং ইউটিউব বদলালে
# yt-dlp মেইনটেইনাররাই এটা আপডেট রাখেন — তাই আমাদের নিজস্ব override দিয়ে এটা
# "override" করার চেষ্টা না করে প্রথম চেষ্টায় কোনো override ছাড়াই (None) যাওয়া
# হয়, যাতে yt-dlp এর সবচেয়ে current লজিক কাজ করে। শুধু সেটা ব্যর্থ হলে fallback
# হিসেবে mweb সহ একটা বিস্তৃত ম্যানুয়াল list চেষ্টা হয় (mweb আগে আমাদের list এ
# ছিলই না, যেটা একটা গুরুত্বপূর্ণ miss ছিল)।
PLAYER_CLIENTS_FALLBACK = ["mweb", "tv", "web_safari", "web", "android_sdkless"]
MIN_GOOD_HEIGHT = 480  # এর নিচে হলে "সীমিত quality" ধরে নিয়ে আরেকটা client combo চেষ্টা করা হয়


class _YdlLogger:
    """
    yt-dlp কে দেওয়া হয় যাতে quiet=True থাকা সত্ত্বেও ভেতরের warning/error
    মেসেজগুলো (যেমন "Some X client formats skipped, missing URL", "PO Token
    Providers: ...") আমাদের নিজস্ব Render লগে দেখা যায় — এটাই ভবিষ্যতে কোনো
    ভিডিও ব্যর্থ হলে exact কারণ বোঝার একমাত্র উপায়।
    """

    def debug(self, msg: str) -> None:
        if msg.startswith("[debug] "):
            return  # অতিরিক্ত-ভার্বোজ internal debug লাইন বাদ, শুধু গুরুত্বপূর্ণটা রাখা
        logger.info("yt-dlp: %s", msg)

    def info(self, msg: str) -> None:
        pass

    def warning(self, msg: str) -> None:
        global YOUTUBE_COOKIES_EXPIRED
        logger.warning("yt-dlp: %s", msg)
        low = msg.lower()
        if "cookies are no longer valid" in low or "rotated in the browser" in low:
            if not YOUTUBE_COOKIES_EXPIRED:
                YOUTUBE_COOKIES_EXPIRED = True
                logger.warning(
                    "⚠️ এক্সপায়ার্ড YouTube কুকিজ শনাক্ত হয়েছে — নতুন কুকিজ আপলোড না হওয়া পর্যন্ত "
                    "YouTube কুকিজ স্বয়ংক্রিয়ভাবে স্কিপ করা হবে।"
                )

    def error(self, msg: str) -> None:
        logger.error("yt-dlp: %s", msg)


def _cookies_usable_for_url(url: str) -> bool:
    """কুকিজ ফাইল আছে কিনা এবং টার্গেট ডোমেইনের জন্য প্রযোজ্য ও সচল কিনা চেক করে।"""
    if not ACTIVE_COOKIES_FILE or not os.path.exists(ACTIVE_COOKIES_FILE):
        return False
    try:
        if os.path.getsize(ACTIVE_COOKIES_FILE) == 0:
            return False
        content = Path(ACTIVE_COOKIES_FILE).read_text(encoding="utf-8", errors="ignore").lower()
    except Exception:
        return False

    if _is_youtube_url(url):
        if YOUTUBE_COOKIES_EXPIRED:
            return False
        return "youtube.com" in content or "google.com" in content

    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    parts = host.split(".")
    base_domain = ".".join(parts[-2:]) if len(parts) >= 2 else host
    return bool(base_domain and base_domain in content)


def _base_ydl_opts(
    use_cookies: bool = True,
    player_clients: Optional[list[str]] = None,
    is_youtube: bool = False,
    url: Optional[str] = None,
    impersonate_chrome: bool = False,
    custom_referer: Optional[str] = None,
) -> dict:
    opts: dict = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "noprogress": True,
        "cachedir": False,
        "retries": 12,
        "fragment_retries": 15,
        "file_access_retries": 10,
        "skip_unavailable_fragments": False,
        "retry_sleep_functions": {
            "http": lambda n: min(20.0, 1.5 * (1.5 ** max(0, n - 1))),
            "fragment": lambda n: min(20.0, 1.5 * (1.5 ** max(0, n - 1))),
            "file_access": lambda n: min(5.0, 0.5 * n),
        },
        "continuedl": True,
        "restrictfilenames": True,
        "socket_timeout": 30,
        "concurrent_fragment_downloads": min(MAX_DL_THREADS, 8),
        "http_chunk_size": 10 * 1024 * 1024,
        "buffersize": 512 * 1024,
        "geo_bypass": True,
        "postprocessor_args": {"default": ["-loglevel", "error", "-nostats", "-threads", "2"]},
        "extractor_args": {},
        "logger": _YdlLogger(),
    }

    if is_youtube:
        # YouTube এর জন্য yt-dlp কে তার নিজস্ব per-client User-Agent (tv, visionos, android, ios, mweb)
        # ব্যবহার করতে দেওয়া হয় — কাস্টম ডেস্কটপ http_headers দিলে ক্লায়েন্ট মিসম্যাচে 429/403 হয়।
        if player_clients:
            opts["extractor_args"]["youtube"] = {"player_client": player_clients}
        if POT_PROVIDER_URL:
            opts["extractor_args"]["youtubepot-bgutilhttp"] = {"base_url": [POT_PROVIDER_URL]}
    else:
        hdrs = dict(std_headers)
        url_path = (urlparse(url).path.lower() if url else "")
        is_manifest = url_path.endswith((".m3u8", ".mpd")) or ".m3u8" in url_path or ".mpd" in url_path
        if is_manifest:
            hdrs.update(
                {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Dest": "empty",
                }
            )
        else:
            hdrs.update(
                {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Sec-Fetch-Mode": "navigate",
                }
            )
        if custom_referer:
            hdrs["Referer"] = custom_referer
            p_ref = urlparse(custom_referer)
            if p_ref.scheme and p_ref.netloc:
                hdrs["Origin"] = f"{p_ref.scheme}://{p_ref.netloc}"
        elif url and "redwansmethod" in url.lower():
            hdrs["Referer"] = "https://redwansmethod.com/"
            hdrs["Origin"] = "https://redwansmethod.com"
        opts["http_headers"] = hdrs
        if impersonate_chrome and ImpersonateTarget is not None:
            opts["impersonate"] = ImpersonateTarget("chrome")

    if use_cookies and ACTIVE_COOKIES_FILE and os.path.exists(ACTIVE_COOKIES_FILE) and os.path.getsize(ACTIVE_COOKIES_FILE) > 0:
        opts["cookiefile"] = ACTIVE_COOKIES_FILE
    if PROXY_URL:
        opts["proxy"] = PROXY_URL
    return opts


def _looks_like_geo_block(message: str) -> bool:
    m = message.lower()
    return "not made this video available in your country" in m or "geographic restriction" in m


def _looks_like_cookie_issue(message: str) -> bool:
    """
    ইউটিউব/সোশ্যাল প্ল্যাটফর্মের ক্ষণস্থায়ী ব্লক বা লগইন/কুকিজ/রেট-লিমিট এরর।
    ("bot" একক শব্দ রাখা হয়নি কারণ দেশের তালিকায় "Botswana" থাকলে ভুলভাবে ম্যাচ করত!)
    """
    if _looks_like_geo_block(message):
        return False
    m = message.lower()
    return any(
        p in m
        for p in (
            "reload",
            "sign in",
            "confirm you",
            "not a bot",
            "bot check",
            "cookies",
            "429",
            "too many requests",
            "403",
            "forbidden",
            "400",
            "bad request",
            "login required",
            "rate-limit",
            "impersonate",
        )
    )


def _looks_like_transient_format_issue(message: str) -> bool:
    """
    ইউটিউব বা অন্য সাইট মাঝেমধ্যে সাময়িকভাবে ফরম্যাট হাইড করে রাখে বা রেসপন্স এক্সট্র্যাক্ট করতে ব্যর্থ হয়।
    """
    m = message.lower()
    return (
        "requested format is not available" in m
        or "no video formats found" in m
        or "failed to extract any player response" in m
    )


def _is_youtube_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return host == "youtu.be" or host.endswith("youtube.com")


def _max_format_height(info: Optional[dict]) -> int:
    """
    আসল ভিডিও স্ট্রিমগুলোর মধ্যে সর্বোচ্চ height বের করে — storyboard/scrubber
    thumbnail mosaic ফরম্যাট (vcodec/acodec দুটোই "none") বাদ দিয়ে।
    """
    if not info:
        return 0
    return max(
        (f.get("height") or 0 for f in (info.get("formats") or []) if f.get("vcodec") not in (None, "none")),
        default=0,
    )


def _http_get_browser(url: str, extra_headers: Optional[dict] = None, use_cookies: bool = False) -> str:
    """Chrome TLS (curl_cffi) বা requests দিয়ে ব্রাউজার-সদৃশ পেজ ফেচ করে।"""
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Dest": "document",
    }
    if extra_headers:
        hdrs.update(extra_headers)

    cj = None
    if use_cookies and ACTIVE_COOKIES_FILE and os.path.exists(ACTIVE_COOKIES_FILE):
        try:
            cj = http.cookiejar.MozillaCookieJar(ACTIVE_COOKIES_FILE)
            cj.load(ignore_discard=True, ignore_expires=True)
        except Exception:
            cj = None

    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    if cffi_requests is not None:
        cookies_dict = {c.name: c.value for c in cj} if cj else None
        resp = cffi_requests.get(
            url, headers=hdrs, cookies=cookies_dict, proxies=proxies, impersonate="chrome", timeout=15, allow_redirects=True
        )
        resp.raise_for_status()
        return resp.text
    resp = requests.get(url, headers=hdrs, cookies=cj, proxies=proxies, timeout=15, allow_redirects=True)
    resp.raise_for_status()
    return resp.text


def _decode_json_str(raw: str) -> str:
    try:
        return json.loads(f'"{raw}"')
    except Exception:
        return raw.replace("\\/", "/").replace("\\u0026", "&").replace("\\u0025", "%")


def _fallback_facebook_extract(url: str) -> Optional[dict]:
    """yt-dlp ব্যর্থ হলে Facebook পেজ থেকে সরাসরি HD/SD fbcdn.net MP4 স্ট্রিম বের করে।"""
    try:
        page = _http_get_browser(url)
    except Exception as e:
        logger.warning("Facebook fallback fetch failed: %s", e)
        return None

    hd_url = None
    sd_url = None
    for pat in (r'"browser_native_hd_url"\s*:\s*"([^"]+)"', r'"playable_url_quality_hd"\s*:\s*"([^"]+)"'):
        m = re.search(pat, page)
        if m and m.group(1) != "null":
            hd_url = _decode_json_str(m.group(1))
            break
    for pat in (r'"browser_native_sd_url"\s*:\s*"([^"]+)"', r'"playable_url"\s*:\s*"([^"]+)"'):
        m = re.search(pat, page)
        if m and m.group(1) != "null":
            sd_url = _decode_json_str(m.group(1))
            break

    if not hd_url and not sd_url:
        return None

    title_m = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', page) or re.search(r"<title>([^<]+)</title>", page)
    title = html.unescape(title_m.group(1)).strip() if title_m else "Facebook Video"
    thumb_m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', page)
    thumb = html.unescape(thumb_m.group(1)) if thumb_m else None

    formats = []
    if sd_url:
        formats.append({"format_id": "sd", "url": sd_url, "ext": "mp4", "height": 360, "vcodec": "h264", "acodec": "aac"})
    if hd_url:
        formats.append({"format_id": "hd", "url": hd_url, "ext": "mp4", "height": 720, "vcodec": "h264", "acodec": "aac"})

    logger.info("✅ Facebook direct fallback সফল হয়েছে (HD=%s, SD=%s)", bool(hd_url), bool(sd_url))
    return {"title": title, "thumbnail": thumb, "formats": formats, "direct_url": hd_url or sd_url}


def _fallback_instagram_extract(url: str) -> Optional[dict]:
    """yt-dlp লগইন ব্লকে পড়লে Instagram /embed/captioned/ থেকে সরাসরি MP4 বের করে।"""
    m_code = re.search(r"/(?:reel|reels|p|tv)/([A-Za-z0-9_-]+)", url)
    if not m_code:
        return None
    shortcode = m_code.group(1)
    for prefix in ("reel", "p"):
        embed_url = f"https://www.instagram.com/{prefix}/{shortcode}/embed/captioned/"
        try:
            page = _http_get_browser(embed_url, {"Referer": "https://www.instagram.com/"})
        except Exception as e:
            logger.warning("Instagram embed fetch failed (%s): %s", embed_url, e)
            continue

        vid_url = None
        m_meta = re.search(r'<meta\s+property="og:video"\s+content="([^"]+)"', page)
        if m_meta:
            vid_url = html.unescape(m_meta.group(1))
        if not vid_url:
            m_json = re.search(r'\\?"video_url\\?"\s*:\s*\\?"((?:[^"\\]|\\.)+)\\?"', page)
            if m_json:
                cand = m_json.group(1).replace('\\\\/', '/').replace('\\/', '/').replace('\\u0026', '&')
                if cand.startswith("http"):
                    vid_url = cand
        if vid_url:
            thumb_m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', page)
            thumb = html.unescape(thumb_m.group(1)) if thumb_m else None
            logger.info("✅ Instagram embed fallback সফল হয়েছে (%s)", shortcode)
            return {
                "title": f"Instagram Reel ({shortcode})",
                "thumbnail": thumb,
                "formats": [{"format_id": "hd", "url": vid_url, "ext": "mp4", "height": 720, "vcodec": "h264", "acodec": "aac"}],
                "direct_url": vid_url,
            }
    return None


def _fallback_tiktok_extract(url: str) -> Optional[dict]:
    """TikTok ওয়াটারমার্ক-বিহীন HD ডাউনলোড ফলব্যাক (tikwm API)।"""
    try:
        resp = requests.post("https://www.tikwm.com/api/", data={"url": url, "hd": "1"}, timeout=15)
        data = resp.json().get("data") or {}
    except Exception as e:
        logger.warning("TikTok fallback failed: %s", e)
        return None

    hd = data.get("hdplay") or ""
    sd = data.get("play") or ""
    if hd.startswith("/"):
        hd = "https://www.tikwm.com" + hd
    if sd.startswith("/"):
        sd = "https://www.tikwm.com" + sd
    if not hd and not sd:
        return None

    formats = []
    if sd:
        formats.append({"format_id": "sd", "url": sd, "ext": "mp4", "height": 540, "vcodec": "h264", "acodec": "aac"})
    if hd:
        formats.append({"format_id": "hd", "url": hd, "ext": "mp4", "height": 1080, "vcodec": "h264", "acodec": "aac"})
    logger.info("✅ TikTok tikwm fallback সফল হয়েছে")
    return {
        "title": data.get("title") or "TikTok Video",
        "duration": data.get("duration"),
        "thumbnail": data.get("cover"),
        "formats": formats,
        "direct_url": hd or sd,
    }


def _fallback_twitter_extract(url: str) -> Optional[dict]:
    """Twitter/X ভিডিও ডাউনলোড ফলব্যাক (fxtwitter API)।"""
    m = re.search(r"/status/(\d+)", url)
    if not m:
        return None
    tweet_id = m.group(1)
    try:
        resp = requests.get(f"https://api.fxtwitter.com/status/{tweet_id}", timeout=15)
        tweet = (resp.json().get("tweet") or {})
        videos = ((tweet.get("media") or {}).get("videos") or [])
        if not videos:
            return None
        v = videos[0]
        direct = v.get("url")
        if not direct:
            return None
        logger.info("✅ Twitter/X fxtwitter fallback সফল হয়েছে")
        return {
            "title": (tweet.get("text") or "X Video")[:120],
            "duration": v.get("duration"),
            "thumbnail": v.get("thumbnail_url"),
            "formats": [{"format_id": "hd", "url": direct, "ext": "mp4", "height": v.get("height") or 720, "vcodec": "h264", "acodec": "aac"}],
            "direct_url": direct,
        }
    except Exception as e:
        logger.warning("Twitter fallback failed: %s", e)
        return None


def _extract_media_urls_from_html(page: str, base_url: str) -> list[str]:
    """HTML/JS পেজের ভেতর থেকে সব .m3u8 / .mpd / .mp4 স্ট্রিম লিংক খুঁজে বের করে।"""
    normalized = (
        page.replace("\\\\/", "/")
        .replace("\\/", "/")
        .replace("\\u0026", "&")
        .replace("\\u003d", "=")
        .replace("&amp;", "&")
    )
    candidates: list[str] = []
    for m in re.finditer(r'https?://[^\s"\'<>\\]+?\.(?:m3u8|mpd|mp4)(?:\?[^\s"\'<>\\]*)?', normalized, re.IGNORECASE):
        u = m.group(0).rstrip(".,;)]}")
        low = u.lower()
        if any(skip in low for skip in ("doubleclick", "googlesyndication", "preview", " trailer", "thumb", "sprite")):
            continue
        if u not in candidates:
            candidates.append(u)

    # master.m3u8 / playlist.m3u8 কে অগ্রাধিকার দেওয়া
    candidates.sort(
        key=lambda x: (
            0 if ("master.m3u8" in x.lower() or "playlist.m3u8" in x.lower())
            else (1 if ".m3u8" in x.lower() else (2 if ".mpd" in x.lower() else 3))
        )
    )
    return candidates


def _fallback_webpage_media_extract(url: str) -> Optional[dict]:
    """
    যেকোনো কাস্টম স্ট্রিমিং বা কোর্স ওয়েবপেজ থেকে সরাসরি সার্ভারের নিজস্ব IP তে ফ্রেশ
    .m3u8 / .mpd / .mp4 স্ট্রিম খুঁজে বের করে (iframe প্লেয়ারসহ)।
    """
    path_low = (urlparse(url).path or "").lower()
    if path_low.endswith((".m3u8", ".mpd", ".mp4", ".webm", ".mkv", ".ts")):
        return None

    use_c = _cookies_usable_for_url(url)
    try:
        page = _http_get_browser(url, use_cookies=use_c)
    except Exception as e:
        logger.warning("Generic webpage fallback fetch failed (%s): %s", url, e)
        return None

    title_m = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', page, re.IGNORECASE) or re.search(
        r"<title>([^<]+)</title>", page, re.IGNORECASE
    )
    page_title = html.unescape(title_m.group(1)).strip() if title_m else "Stream Video"
    thumb_m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', page, re.IGNORECASE)
    page_thumb = html.unescape(thumb_m.group(1)) if thumb_m else None

    streams = _extract_media_urls_from_html(page, url)
    ref_for_stream = url

    # মূল পেজে না পেলে ভেতরের প্লেয়ার iframe চেক করা
    if not streams:
        iframes = re.findall(r'<iframe[^>]+src=["\']([^"\']+)["\']', page, re.IGNORECASE)
        checked = 0
        for raw_if in iframes:
            if_url = urljoin(url, html.unescape(raw_if))
            if not if_url.startswith(("http://", "https://")):
                continue
            if any(ad in if_url.lower() for ad in ("google", "facebook.com/plugins/like", "doubleclick", "disqus", "recaptcha")):
                continue
            checked += 1
            if checked > 2:
                break
            try:
                if_page = _http_get_browser(if_url, extra_headers={"Referer": url}, use_cookies=use_c)
                found = _extract_media_urls_from_html(if_page, if_url)
                if found:
                    streams = found
                    ref_for_stream = if_url
                    break
            except Exception:
                continue

    if not streams:
        return None

    best_stream = streams[0]
    is_manifest = ".m3u8" in best_stream.lower() or ".mpd" in best_stream.lower()
    logger.info("✅ Webpage media sniffer সফল: %s (manifest=%s)", best_stream[:90], is_manifest)

    if is_manifest:
        try:
            probe_opts = _base_ydl_opts(use_cookies=use_c, is_youtube=False, url=best_stream, custom_referer=ref_for_stream)
            probe_opts["skip_download"] = True
            with yt_dlp.YoutubeDL(probe_opts) as ydl:
                m_info = ydl.extract_info(best_stream, download=False) or {}
            m_info["title"] = page_title or m_info.get("title") or "Stream Video"
            if page_thumb and not m_info.get("thumbnail"):
                m_info["thumbnail"] = page_thumb
            m_info["direct_url"] = best_stream
            m_info["referer"] = ref_for_stream
            m_info["is_manifest"] = True
            return m_info
        except Exception as e:
            logger.warning("Manifest probe warning (%s): %s", best_stream[:80], e)

    return {
        "title": page_title,
        "thumbnail": page_thumb,
        "formats": [{"format_id": "best", "url": best_stream, "ext": "mp4", "height": 720, "vcodec": "h264", "acodec": "aac"}],
        "direct_url": best_stream,
        "referer": ref_for_stream,
        "is_manifest": is_manifest,
    }


class AyloAuthError(RuntimeError):
    """Aylo / Project1Service মেম্বার এরিয়ার লগইন বা মেয়াদোত্তীর্ণ টোকেন সংক্রান্ত স্পষ্ট বার্তা।"""


AYLO_BASE_DOMAINS = {
    "brazzers.com",
    "brazzersnetwork.com",
    "spicevids.com",
    "realitykings.com",
    "mofos.com",
    "twistys.com",
    "babes.com",
    "digitalplayground.com",
    "fakehub.com",
    "men.com",
    "seancody.com",
    "transangels.com",
    "bangbros.com",
    "milehighmedia.com",
    "wicked.com",
    "iknowthatgirl.com",
    "letsdoeit.com",
    "biempire.com",
}


def _host_base_domain(host: str) -> str:
    h = (host or "").strip().lstrip(".").lower()
    parts = h.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else h


def _is_aylo_site_ma_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return host.startswith("site-ma.")


def _is_aylo_portal_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return False
    if host.startswith("site-ma."):
        return True
    if _host_base_domain(host) in AYLO_BASE_DOMAINS:
        return any(seg in path for seg in ("/scene/", "/video/", "/movie/", "/serie/"))
    return False


NETSCAPE_SPACE_LINE_RE = re.compile(
    r"^(?:#HttpOnly_)?(\S+)\s+(TRUE|FALSE)\s+(/\S*)\s+(TRUE|FALSE)\s+(-?\d+)\s+(\S+)\s*(.*)$",
    re.IGNORECASE,
)


def _clean_cookie_markdown(line: str) -> str:
    """চ্যাট বা মার্কডাউন থেকে কপি করার সময় [site-ma.brazzers.com](https://...) থাকলে আসল ডোমেইনে রূপান্তর করে।"""
    return re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", line)


def _is_netscape_cookie_text(text: str) -> bool:
    if (
        "# Netscape" in text
        or "# HTTP Cookie File" in text
        or "\tTRUE\t" in text
        or "\tFALSE\t" in text
    ):
        return True
    for raw_ln in (text or "").splitlines():
        ln = _clean_cookie_markdown(raw_ln.strip())
        if NETSCAPE_SPACE_LINE_RE.match(ln):
            return True
    return False


def _parse_netscape_cookies(text: str) -> list[dict]:
    """ট্যাব-সেপারেটেড (.txt ফাইল) এবং স্পেস-সেপারেটেড (টেলিগ্রাম চ্যাট পেস্ট) — উভয় ধরনের কুকিজ নিখুঁতভাবে পার্স করে।"""
    entries: list[dict] = []
    for raw_line in (text or "").splitlines():
        line = _clean_cookie_markdown(raw_line.strip())
        if not line:
            continue
        http_only = False
        if line.startswith("#HttpOnly_"):
            http_only = True
            line = line[len("#HttpOnly_"):].strip()
        elif line.startswith("#"):
            continue

        parts = line.split("\t")
        if len(parts) >= 7:
            domain, flag, path, secure, expires, name = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
            value = "\t".join(parts[6:])
        else:
            m_sp = NETSCAPE_SPACE_LINE_RE.match(line)
            if not m_sp:
                continue
            domain, flag, path, secure, expires, name, value = m_sp.groups()

        dom_clean = domain.strip()
        entries.append(
            {
                "domain": dom_clean,
                "flag": flag.strip().upper() or ("TRUE" if dom_clean.startswith(".") else "FALSE"),
                "path": path.strip() or "/",
                "secure": secure.strip().upper() or "FALSE",
                "expires": expires.strip() or "0",
                "name": name.strip(),
                "value": value.strip(),
                "http_only": http_only,
            }
        )
    return entries


def _serialize_netscape_cookies(entries: list[dict]) -> str:
    lines = [
        "# Netscape HTTP Cookie File",
        "# Merged multi-domain cookie jar",
    ]
    for c in entries:
        prefix = "#HttpOnly_" if c.get("http_only") else ""
        dom = c["domain"]
        flag = "TRUE" if dom.startswith(".") else "FALSE"
        lines.append(f"{prefix}{dom}\t{flag}\t{c['path']}\t{c['secure']}\t{c['expires']}\t{c['name']}\t{c['value']}")
    return "\n".join(lines) + "\n"


def _merge_netscape_cookies(existing_text: str, incoming_text: str) -> str:
    """
    যে ডোমেইনের নতুন কুকিজ দেওয়া হয়েছে সেই ডোমেইনকে ফ্রেশভাবে সেট করে এবং
    অন্যান্য সচল ডোমেইনের কুকিজ অক্ষুণ্ণ রাখে (সম্পূর্ণ মেয়াদোত্তীর্ণ ডেড সেশন বাদ দিয়ে)।
    """
    incoming_parsed = _parse_netscape_cookies(incoming_text)
    incoming_domains = {_host_base_domain(c["domain"]) for c in incoming_parsed if c.get("domain")}
    existing_parsed = _parse_netscape_cookies(existing_text)
    now = int(time.time())

    # পুরনো ডোমেইনগুলোর মধ্যে যেগুলোর Aylo টোকেন (access + refresh দুটোই) ইতিমধ্যে এক্সপায়ার্ড, সেগুলো বাদ দেওয়া
    by_existing_dom: dict[str, list[dict]] = {}
    for c in existing_parsed:
        bdom = _host_base_domain(c["domain"])
        if not bdom or bdom in incoming_domains:
            continue
        by_existing_dom.setdefault(bdom, []).append(c)

    merged_map: dict[tuple[str, str, str], dict] = {}
    for bdom, dom_list in by_existing_dom.items():
        acc_c = next((x for x in reversed(dom_list) if x["name"] == "access_token_ma" and x["value"]), None)
        rt_c = next((x for x in reversed(dom_list) if x["name"] == "refresh_token_ma" and x["value"]), None)
        if acc_c or rt_c:
            acc_e = _jwt_exp(acc_c["value"]) if acc_c else None
            if acc_e is None and acc_c:
                with contextlib.suppress(ValueError):
                    acc_e = int(acc_c["expires"]) if int(acc_c["expires"]) > 0 else None
            rt_e = _jwt_exp(rt_c["value"]) if rt_c else None
            if rt_e is None and rt_c:
                with contextlib.suppress(ValueError):
                    rt_e = int(rt_c["expires"]) if int(rt_c["expires"]) > 0 else None
            acc_ok = bool(acc_c and (acc_e is None or acc_e > now))
            rt_ok = bool(rt_c and (rt_e is None or rt_e > now))
            if not acc_ok and not rt_ok:
                continue  # ডেড মেয়াদোত্তীর্ণ ডোমেইন অটো-ক্লিনআপ
        for c in dom_list:
            key = (c["domain"].lstrip(".").lower(), c["path"], c["name"])
            merged_map[key] = c

    # যদি একই ডোমেইনের আংশিক আপডেট (যেমন অটো-রিনিউ টোকেন) আসে, তখন ওই ডোমেইনের আগের কুকিও ধরে রাখা
    is_partial_token_update = len(incoming_parsed) <= 4 and all(
        c["name"] in ("access_token_ma", "refresh_token_ma", "instance_token") for c in incoming_parsed
    )
    if is_partial_token_update:
        for c in existing_parsed:
            if _host_base_domain(c["domain"]) in incoming_domains:
                key = (c["domain"].lstrip(".").lower(), c["path"], c["name"])
                merged_map[key] = c

    for c in incoming_parsed:
        key = (c["domain"].lstrip(".").lower(), c["path"], c["name"])
        merged_map[key] = c

    return _serialize_netscape_cookies(list(merged_map.values()))


def _sync_cloud_env_var(var_name: str, value: str) -> bool:
    """Railway (skipDeploys=True সহ, যাতে কন্টেইনার রিস্টার্ট না হয়) অথবা Render এনভায়রনমেন্টে ভ্যারিয়েবল সেভ করে।"""
    if RAILWAY_API_TOKEN and RAILWAY_PROJECT_ID and RAILWAY_ENVIRONMENT_ID:
        try:
            inp: dict = {
                "projectId": RAILWAY_PROJECT_ID,
                "environmentId": RAILWAY_ENVIRONMENT_ID,
                "name": var_name,
                "value": value,
                "skipDeploys": True,
            }
            if RAILWAY_SERVICE_ID:
                inp["serviceId"] = RAILWAY_SERVICE_ID
            payload = {
                "query": "mutation($input: VariableUpsertInput!) { variableUpsert(input: $input) }",
                "variables": {"input": inp},
            }
            for gql_ep in ("https://backboard.railway.app/graphql/v2", "https://backboard.railway.com/graphql/v2"):
                try:
                    res = requests.post(
                        gql_ep,
                        headers={
                            "Authorization": f"Bearer {RAILWAY_API_TOKEN}",
                            "Content-Type": "application/json",
                        },
                        json=payload,
                        timeout=12,
                    )
                    if res.status_code == 200 and "errors" not in (res.json() or {}):
                        return True
                except Exception:
                    continue
            logger.warning("Railway variableUpsert (%s) warning", var_name)
        except Exception as e:
            logger.warning("Railway env sync error (%s): %s", var_name, e)

    r_api_key = RENDER_API_KEY or os.environ.get("RENDER_API_KEY", "").strip()
    r_svc_id = RENDER_SERVICE_ID or os.environ.get("RENDER_SERVICE_ID", "").strip()
    if r_api_key and r_svc_id:
        try:
            render_url = f"https://api.render.com/v1/services/{r_svc_id}/env-vars/{var_name}"
            headers = {
                "Authorization": f"Bearer {r_api_key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            if value == "":
                res = requests.delete(render_url, headers=headers, timeout=12)
                return res.status_code in (200, 204, 404)
            res = requests.put(render_url, headers=headers, json={"value": value}, timeout=12)
            if res.status_code in (200, 201):
                return True
            logger.warning("Render env-var sync HTTP %d (%s)", res.status_code, var_name)
        except Exception as e:
            logger.warning("Render env-var sync error (%s): %s", var_name, e)
    return False


def _save_and_sync_cookies(content: str, sync_render: bool = True) -> bool:
    """কুকিজ রানটাইম ফাইলে সেভ করে এবং ক্লাউড এনভায়রনমেন্টে (Railway/Render) সিঙ্ক করে।"""
    global ACTIVE_COOKIES_FILE
    Path("cookies.txt").write_text(content, encoding="utf-8")
    COOKIES_RUNTIME_PATH.write_text(content, encoding="utf-8")
    with contextlib.suppress(OSError):
        COOKIES_RUNTIME_PATH.chmod(0o600)
    ACTIVE_COOKIES_FILE = str(COOKIES_RUNTIME_PATH.resolve())

    if not sync_render:
        return False
    return _sync_cloud_env_var("COOKIES_DATA", content)


def _decode_jwt_payload(token: str) -> Optional[dict]:
    if not token or token.count(".") != 2:
        return None
    try:
        payload_b64 = token.split(".")[1]
        pad = (-len(payload_b64)) % 4
        raw = base64.urlsafe_b64decode(payload_b64 + ("=" * pad))
        data = json.loads(raw.decode("utf-8", errors="ignore"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _jwt_exp(token: str) -> Optional[int]:
    payload = _decode_jwt_payload(token)
    if not payload or "exp" not in payload:
        return None
    try:
        return int(payload["exp"])
    except (TypeError, ValueError):
        return None


def _fetch_fresh_aylo_instance_token(auth_host: str) -> Optional[str]:
    """পাবলিক পোর্টাল থেকে নতুন ২৪-ঘণ্টার instance_token ফেচ করে (পুরনোটি মেয়াদোত্তীর্ণ হওয়ার আগেই রিনিউ সচল রাখতে)।"""
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }
    target_u = f"https://{auth_host}/"
    try:
        if cffi_requests is not None:
            r_p = cffi_requests.get(target_u, headers=hdrs, proxies=proxies, impersonate="chrome", timeout=12, allow_redirects=True)
            if "instance_token" in r_p.cookies:
                return str(r_p.cookies["instance_token"])
        else:
            r_p = requests.get(target_u, headers=hdrs, proxies=proxies, timeout=12, allow_redirects=True)
            if "instance_token" in r_p.cookies:
                return str(r_p.cookies.get("instance_token") or "")
    except Exception as e:
        logger.debug("Fresh instance_token fetch warning (%s): %s", target_u, e)
    return None


def _try_renew_aylo_tokens(force: bool = False) -> bool:
    """
    refresh_token_ma কুকি থাকলে তার ৩০ মিনিটের মেয়াদ শেষ হওয়ার আগেই স্বয়ংক্রিয়ভাবে
    instance_token (২৪ ঘণ্টা) + access_token_ma (১ ঘণ্টা) + refresh_token_ma (৩০ মিনিট) রিনিউ করে ২৪/৭ সচল রাখে।
    """
    if not COOKIES_RUNTIME_PATH.exists():
        return False
    try:
        existing_raw = COOKIES_RUNTIME_PATH.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False

    cookies = _parse_netscape_cookies(existing_raw)
    if not cookies:
        return False

    now = int(time.time())
    renewed_any = False
    new_lines: list[dict] = []

    rt_cookies = [c for c in cookies if c["name"] == "refresh_token_ma" and c["value"]]
    for rt_c in rt_cookies:
        rt_val = rt_c["value"]
        rt_exp = _jwt_exp(rt_val)
        if rt_exp is None:
            try:
                rt_exp = int(rt_c["expires"]) if int(rt_c["expires"]) > 0 else None
            except ValueError:
                rt_exp = None
        if rt_exp is not None and rt_exp <= now:
            continue

        base_dom = _host_base_domain(rt_c["domain"])
        acc_c = next((c for c in reversed(cookies) if c["name"] == "access_token_ma" and _host_base_domain(c["domain"]) == base_dom), None)
        inst_c = next((c for c in reversed(cookies) if c["name"] == "instance_token" and _host_base_domain(c["domain"]) == base_dom), None)
        acc_exp = _jwt_exp(acc_c["value"]) if acc_c else 0
        inst_val = inst_c["value"] if inst_c else ""
        inst_exp = _jwt_exp(inst_val) if inst_val else 0

        needs_renew = (
            force
            or not acc_c
            or (acc_exp and acc_exp - now < 1200)
            or (rt_exp and rt_exp - now < 1440)
            or not inst_val
            or (inst_exp and inst_exp - now < 7200)
        )
        if not needs_renew:
            continue

        inst_payload = _decode_jwt_payload(inst_val) or {}
        auth_host = str(inst_payload.get("hostname") or rt_c["domain"].lstrip(".") or f"site-ma.{base_dom}")

        # ২৪ ঘণ্টার instance_token এক্সপায়ার হওয়ার ২ ঘণ্টা আগেই স্বয়ংক্রিয়ভাবে নতুন instance_token নিয়ে নেওয়া
        if not inst_val or (inst_exp and inst_exp - now < 7200):
            fresh_inst = _fetch_fresh_aylo_instance_token(auth_host)
            if fresh_inst:
                inst_val = fresh_inst
                fresh_inst_exp = _jwt_exp(fresh_inst) or (now + 86400)
                inst_dom = inst_c["domain"] if inst_c else auth_host
                new_lines.append(
                    {
                        "domain": inst_dom,
                        "flag": "TRUE" if inst_dom.startswith(".") else "FALSE",
                        "path": "/",
                        "secure": "FALSE",
                        "expires": str(fresh_inst_exp),
                        "name": "instance_token",
                        "value": fresh_inst,
                        "http_only": False,
                    }
                )
                logger.info("🔄 নতুন ২৪-ঘণ্টার instance_token অটো-ফেচ হয়েছে (%s)", auth_host)

        grant_data = None
        proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
        for attempt in range(2):
            try:
                hdrs = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Origin": f"https://{auth_host}",
                    "Referer": f"https://{auth_host}/",
                }
                if inst_val:
                    hdrs["Instance"] = inst_val
                resp = requests.post(
                    "https://auth-service.project1service.com/v1/authenticate/renew",
                    headers=hdrs,
                    json={"refreshToken": rt_val},
                    proxies=proxies,
                    timeout=12,
                )
                if resp.status_code in (200, 201):
                    cand = resp.json()
                    if isinstance(cand, dict) and cand.get("access_token"):
                        grant_data = cand
                        break
                elif resp.status_code in (401, 403) and attempt == 0:
                    # যদি পুরনো instance_token রিজেক্ট হয়, তাৎক্ষণিক ফ্রেশ instance_token নিয়ে দ্বিতীয় চেষ্টা
                    fresh_inst = _fetch_fresh_aylo_instance_token(auth_host)
                    if fresh_inst and fresh_inst != inst_val:
                        inst_val = fresh_inst
                        continue
            except Exception as e:
                logger.debug("Aylo auth-service renew warning: %s", e)

        if not grant_data:
            try:
                resp = requests.post(
                    "https://prod-keycloak-p1.geekadm.net/realms/P1INTERNAL/protocol/openid-connect/token",
                    data={"grant_type": "refresh_token", "client_id": "P1CLI", "refresh_token": rt_val},
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                    timeout=12,
                )
                if resp.status_code == 200:
                    cand = resp.json()
                    if isinstance(cand, dict) and cand.get("access_token"):
                        grant_data = cand
            except Exception as e:
                logger.debug("Aylo keycloak renew warning: %s", e)

        if not grant_data:
            continue

        new_acc = grant_data["access_token"]
        new_acc_exp = _jwt_exp(new_acc) or (now + int(grant_data.get("expires_in") or 3600))
        target_dom = acc_c["domain"] if acc_c else rt_c["domain"]
        new_lines.append(
            {
                "domain": target_dom,
                "flag": "TRUE" if target_dom.startswith(".") else "FALSE",
                "path": "/",
                "secure": "TRUE",
                "expires": str(new_acc_exp),
                "name": "access_token_ma",
                "value": new_acc,
                "http_only": False,
            }
        )
        new_rt = grant_data.get("refresh_token")
        if new_rt:
            new_rt_exp = _jwt_exp(new_rt) or (now + int(grant_data.get("refresh_expires_in") or 1800))
            new_lines.append(
                {
                    "domain": rt_c["domain"],
                    "flag": "TRUE" if rt_c["domain"].startswith(".") else "FALSE",
                    "path": "/",
                    "secure": "TRUE",
                    "expires": str(new_rt_exp),
                    "name": "refresh_token_ma",
                    "value": new_rt,
                    "http_only": False,
                }
            )
        renewed_any = True
        logger.info("✅ Aylo (%s) সেশন টোকেন স্বয়ংক্রিয়ভাবে রিনিউ হয়েছে!", base_dom)

    if renewed_any and new_lines:
        merged = _merge_netscape_cookies(existing_raw, _serialize_netscape_cookies(new_lines))
        _save_and_sync_cookies(merged, sync_render=True)
    return renewed_any


def _cookie_effective_exp(c: Optional[dict]) -> Optional[int]:
    """কুকির আসল মেয়াদ (JWT exp অথবা Netscape expires কলাম) বের করে। সেশন কুকি হলে 0 রিটার্ন করে।"""
    if not c:
        return None
    jwt_e = _jwt_exp(c.get("value") or "")
    if jwt_e is not None:
        return jwt_e
    try:
        raw_e = int(c.get("expires") or "0")
        return raw_e if raw_e >= 0 else None
    except ValueError:
        return None


def _format_duration_bn(seconds: float) -> str:
    """সেকেন্ডকে বাংলায় দিন / ঘণ্টা / মিনিট ফরম্যাটে দেখায়।"""
    sec = max(0, int(seconds))
    if sec >= 86400:
        d = sec // 86400
        h = (sec % 86400) // 3600
        return f"{d} দিন {h} ঘণ্টা" if h > 0 else f"{d} দিন"
    if sec >= 3600:
        h = sec // 3600
        m = (sec % 3600) // 60
        return f"{h} ঘণ্টা {m} মিনিট" if m > 0 else f"{h} ঘণ্টা"
    return f"{max(1, sec // 60)} মিনিট"


KNOWN_AUTH_COOKIES: dict[str, set[str]] = {
    "youtube.com": {"SID", "__Secure-1PSID", "__Secure-3PSID", "LOGIN_INFO", "SAPISID"},
    "facebook.com": {"xs", "c_user"},
    "instagram.com": {"sessionid"},
    "tiktok.com": {"sessionid", "sid_tt", "sessionid_ss"},
    "x.com": {"auth_token"},
    "twitter.com": {"auth_token"},
}

NON_AUTH_COOKIE_PREFIXES = ("_ga", "_gid", "_gat", "cookieconsent", "__cf_bm", "vjs-analytics", "atstrack", "notification_")


def _normalize_Logical_domain(base_dom: str, all_base_doms: set[str]) -> str:
    if base_dom == "google.com" and "youtube.com" in all_base_doms:
        return "youtube.com"
    return base_dom


def _per_domain_cookie_status_lines(raw_cookie_text: str, target_domains: Optional[set[str]] = None) -> list[str]:
    """প্রতিটি ডোমেইনের জন্য আলাদা আলাদা সঠিক স্ট্যাটাস এবং কতক্ষণ মেয়াদ বাকি আছে তা হিসাব করে।"""
    cookies = _parse_netscape_cookies(raw_cookie_text)
    if not cookies:
        return []

    raw_doms = {_host_base_domain(c["domain"]) for c in cookies if c.get("domain")}
    grouped: dict[str, list[dict]] = {}
    for c in cookies:
        bdom = _host_base_domain(c["domain"])
        if not bdom:
            continue
        ldom = _normalize_Logical_domain(bdom, raw_doms)
        grouped.setdefault(ldom, []).append(c)

    if target_domains is not None:
        norm_targets = {_normalize_Logical_domain(d, raw_doms) for d in target_domains if d}
    else:
        norm_targets = set(grouped.keys())

    now = int(time.time())
    lines: list[str] = []

    for dom in sorted(grouped.keys()):
        if dom not in norm_targets:
            continue
        dom_cookies = grouped[dom]
        acc_c = next((c for c in reversed(dom_cookies) if c["name"] == "access_token_ma" and c["value"]), None)
        rt_c = next((c for c in reversed(dom_cookies) if c["name"] == "refresh_token_ma" and c["value"]), None)

        # ১. Aylo / site-ma.* মেম্বার পোর্টাল ডোমেইন (যেমন brazzers.com, spicevids.com, realitykings.com)
        if acc_c or rt_c or dom in AYLO_BASE_DOMAINS:
            acc_exp = _cookie_effective_exp(acc_c)
            rt_exp = _cookie_effective_exp(rt_c)
            acc_alive = bool(acc_c and (acc_exp is None or acc_exp == 0 or acc_exp > now))
            rt_alive = bool(rt_c and (rt_exp is None or rt_exp == 0 or rt_exp > now))

            if rt_alive:
                acc_rem = _format_duration_bn(acc_exp - now) if (acc_exp and acc_exp > now) else "সচল"
                rt_rem = _format_duration_bn(rt_exp - now) if (rt_exp and rt_exp > now) else "সচল"
                lines.append(
                    f"🎬 <b>{html.escape(dom)}:</b> ✅ সচল | মেয়াদ: <b>২৪/৭ অটো-রিনিউ সক্রিয় (স্থায়ী)</b> "
                    f"<i>[অ্যাক্সেস টোকেন: ~{acc_rem}, রিফ্রেশ টোকেন: ~{rt_rem} বাকি — প্রতি ৪ মিনিটে অটো-রিনিউ হয়]</i>"
                )
            elif acc_alive:
                acc_rem = _format_duration_bn(acc_exp - now) if (acc_exp and acc_exp > now) else "সেশন কুকি"
                lines.append(
                    f"🎬 <b>{html.escape(dom)}:</b> ✅ সচল | মেয়াদ বাকি: <b>~{acc_rem}</b> "
                    "<i>(⚠️ refresh_token_ma নেই — মেয়াদ শেষে নতুন কুকিজ দিতে হবে)</i>"
                )
            else:
                last_exp = max(acc_exp or 0, rt_exp or 0)
                ago_txt = f" ({_format_duration_bn(now - last_exp)} আগে শেষ হয়েছে)" if last_exp > 0 else ""
                lines.append(f"🎬 <b>{html.escape(dom)}:</b> ❌ মেয়াদ শেষ{ago_txt}")
            continue

        # ২. সাধারণ প্ল্যাটফর্ম ডোমেইন (YouTube, Facebook, Instagram, TikTok, X বা যেকোনো কাস্টম সাইট)
        if dom == "youtube.com" and YOUTUBE_COOKIES_EXPIRED:
            lines.append(f"🌐 <b>{html.escape(dom)}:</b> ❌ মেয়াদোত্তীর্ণ <i>(ব্রাউজারে রোটেশন হয়েছে — নতুন কুকিজ দিন)</i>")
            continue

        key_names = KNOWN_AUTH_COOKIES.get(dom)
        if key_names:
            auth_cs = [c for c in dom_cookies if c["name"] in key_names and c["value"]]
        else:
            auth_cs = [
                c for c in dom_cookies
                if c["value"] and not c["name"].lower().startswith(NON_AUTH_COOKIE_PREFIXES)
            ] or dom_cookies

        pos_future = [e for e in (_cookie_effective_exp(c) for c in auth_cs) if e and e > now]
        has_session = any(_cookie_effective_exp(c) == 0 for c in auth_cs)
        pos_expired = [e for e in (_cookie_effective_exp(c) for c in auth_cs) if e and 0 < e <= now]

        if pos_future:
            # মূল সেশন কুকির সর্বনিম্ন সক্রিয় মেয়াদ দেখানো যাতে ভুলভাবে ২ বছরের ট্র্যাকিং কুকির মেয়াদ না দেখায়
            eff_exp = min(pos_future)
            lines.append(
                f"🌐 <b>{html.escape(dom)}:</b> ✅ সচল | মেয়াদ বাকি: <b>~{_format_duration_bn(eff_exp - now)}</b>"
            )
        elif has_session:
            lines.append(f"🌐 <b>{html.escape(dom)}:</b> ✅ সচল | মেয়াদ: <b>ব্রাউজার সেশন কুকি</b>")
        elif pos_expired:
            ago = _format_duration_bn(now - max(pos_expired))
            lines.append(f"🌐 <b>{html.escape(dom)}:</b> ❌ মেয়াদ শেষ <i>({ago} আগে)</i>")
        else:
            lines.append(f"🌐 <b>{html.escape(dom)}:</b> ✅ সেট করা আছে ({len(dom_cookies)}টি কুকি)")

    return lines


def _aylo_cookies_status_summary(raw_cookie_text: str, target_domains: Optional[set[str]] = None) -> str:
    """কুকিজ টেক্সটে থাকা প্রতিটি ডোমেইনের নাম ও সঠিক মেয়াদ বাংলায় সুন্দরভাবে ফরম্যাট করে।"""
    lines = _per_domain_cookie_status_lines(raw_cookie_text, target_domains=target_domains)
    return ("\n" + "\n".join(lines)) if lines else ""


def _parse_hls_master_heights(master_url: str, referer: str) -> list[int]:
    """HLS master.m3u8 থেকে দ্রুত সব রেজুলেশন (height) বের করে।"""
    p_ref = urlparse(referer)
    origin = f"{p_ref.scheme}://{p_ref.netloc}" if (p_ref.scheme and p_ref.netloc) else ""
    hdrs = {
        "Accept": "*/*",
        "Referer": referer,
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    if origin:
        hdrs["Origin"] = origin
    try:
        manifest_text = _http_get_browser(master_url, extra_headers=hdrs, use_cookies=True)
    except Exception as e:
        logger.debug("HLS master height parse warning: %s", e)
        return []
    heights: set[int] = set()
    for m in re.finditer(r"RESOLUTION=\d+x(\d+)", manifest_text):
        try:
            heights.add(int(m.group(1)))
        except ValueError:
            pass
    return sorted(heights)


def _extract_aylo_best_poster(result: dict) -> Optional[str]:
    best_url = None
    best_area = -1
    images = result.get("images")
    if isinstance(images, dict):
        for group in images.values():
            if not isinstance(group, dict):
                continue
            for idx_val in group.values():
                if not isinstance(idx_val, dict):
                    continue
                for sz_val in idx_val.values():
                    if not isinstance(sz_val, dict):
                        continue
                    u = sz_val.get("url") or ((sz_val.get("urls") or {}).get("default") if isinstance(sz_val.get("urls"), dict) else None)
                    area = int(sz_val.get("width") or 0) * int(sz_val.get("height") or 0)
                    if u and area > best_area:
                        best_area = area
                        best_url = u
    if not best_url:
        for key in ("imageMasters", "images"):
            arr = result.get(key)
            if isinstance(arr, list) and arr:
                first = arr[0]
                if isinstance(first, dict):
                    best_url = first.get("src") or first.get("url") or first.get("path")
                    if best_url:
                        break
    return best_url


def _fallback_aylo_member_extract(url: str) -> Optional[dict]:
    """
    Aylo / Project1Service মেম্বার এরিয়া (site-ma.brazzers.com ইত্যাদি) থেকে সার্ভারের নিজস্ব IP-তে
    সাইন করা ফুল-রেজুলেশন MP4 বা HLS স্ট্রিম বের করে।
    """
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    base_domain = _host_base_domain(host)
    is_site_ma = host.startswith("site-ma.")

    scene_id: Optional[str] = None
    slug: Optional[str] = None
    m1 = re.search(r"/(?:scene|video/(?:view/)?|movie|serie)/(\d+)(?:/([A-Za-z0-9_-]+))?", parsed.path, re.IGNORECASE)
    if m1:
        scene_id, slug = m1.group(1), m1.group(2)
    else:
        m2 = re.search(r"/video/([A-Za-z0-9_-]+)/(\d+)", parsed.path, re.IGNORECASE)
        if m2:
            slug, scene_id = m2.group(1), m2.group(2)

    if not scene_id:
        if is_site_ma:
            raise AyloAuthError(
                "❌ <b>নির্দিষ্ট ভিডিওর লিংক পাওয়া যায়নি!</b>\n\n"
                "অনুগ্রহ করে ভিডিওর মূল পেজ লিংকটি পাঠান (যেমন: <code>https://site-ma.brazzers.com/scene/12345/...</code>)।"
            )
        return None

    def _load_domain_tokens() -> tuple[str, str, str, str]:
        if not COOKIES_RUNTIME_PATH.exists():
            return "", "", "", ""
        raw_txt = COOKIES_RUNTIME_PATH.read_text(encoding="utf-8", errors="ignore")
        dom_cookies = [c for c in _parse_netscape_cookies(raw_txt) if _host_base_domain(c["domain"]) == base_domain]
        inst = next((c["value"] for c in reversed(dom_cookies) if c["name"] == "instance_token" and c["value"]), "")
        acc = next((c["value"] for c in reversed(dom_cookies) if c["name"] == "access_token_ma" and c["value"]), "")
        rt = next((c["value"] for c in reversed(dom_cookies) if c["name"] == "refresh_token_ma" and c["value"]), "")
        sess = next((c["value"] for c in reversed(dom_cookies) if c["name"] == "app_session_id" and c["value"]), "")
        return inst, acc, rt, sess

    instance_token, access_token, refresh_token, app_session_id = _load_domain_tokens()
    now = int(time.time())
    acc_exp = _jwt_exp(access_token) if access_token else None
    acc_expired = bool(access_token and acc_exp and acc_exp <= now)

    if (not access_token or acc_expired) and refresh_token:
        if _try_renew_aylo_tokens(force=True):
            instance_token, access_token, refresh_token, app_session_id = _load_domain_tokens()
            now = int(time.time())
            acc_exp = _jwt_exp(access_token) if access_token else None
            acc_expired = bool(access_token and acc_exp and acc_exp <= now)

    data_api = "https://site-api.project1service.com"
    inst_exp = _jwt_exp(instance_token) if instance_token else None
    if not instance_token or (inst_exp and inst_exp <= now):
        probe_host = host if is_site_ma else f"site-ma.{base_domain}"
        for target_u in (url, f"https://{probe_host}/"):
            try:
                proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
                hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
                if cffi_requests is not None:
                    r_p = cffi_requests.get(target_u, headers=hdrs, proxies=proxies, impersonate="chrome", timeout=12, allow_redirects=True)
                    body_txt = r_p.text or ""
                    if "instance_token" in r_p.cookies:
                        instance_token = r_p.cookies["instance_token"]
                else:
                    r_p = requests.get(target_u, headers=hdrs, proxies=proxies, timeout=12, allow_redirects=True)
                    body_txt = r_p.text or ""
                    if "instance_token" in r_p.cookies:
                        instance_token = r_p.cookies.get("instance_token") or ""
                m_cfg = re.search(r"__JUAN\.config\s*=\s*(\{.*?\})\s*;", body_txt)
                if m_cfg:
                    with contextlib.suppress(Exception):
                        cfg_j = json.loads(m_cfg.group(1))
                        if cfg_j.get("dataApiUrl"):
                            data_api = str(cfg_j["dataApiUrl"]).replace("\\u002F", "/").rstrip("/")
                if instance_token:
                    break
            except Exception as e:
                logger.debug("Aylo instance_token fetch warning (%s): %s", target_u, e)

    if not instance_token:
        if is_site_ma:
            raise AyloAuthError(
                "❌ <b>মেম্বার সেশন টোকেন পাওয়া যায়নি!</b>\n\n"
                f"ব্রাউজারে <code>{html.escape(host)}</code>-এ লগইন থাকা অবস্থায় <b>Cookie-Editor → Export → Netscape</b> "
                "থেকে নতুন <code>cookies.txt</code> পাঠান।"
            )
        return None

    inst_payload = _decode_jwt_payload(instance_token) or {}
    auth_host = str(inst_payload.get("hostname") or (host if is_site_ma else f"site-ma.{base_domain}"))
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

    auth_candidates: list[Optional[str]] = []
    if access_token and not acc_expired:
        auth_candidates.extend([access_token, f"Bearer {access_token}"])
    else:
        auth_candidates.append(None)

    result: Optional[dict] = None
    for auth_header_val in auth_candidates:
        api_hdrs = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Origin": f"https://{auth_host}",
            "Referer": url,
            "Instance": instance_token,
        }
        if app_session_id:
            api_hdrs["x-app-session-id"] = app_session_id
        if auth_header_val:
            api_hdrs["Authorization"] = auth_header_val

        for ep in (f"{data_api}/v2/releases/{scene_id}", f"{data_api}/v1/releases/{scene_id}"):
            try:
                r_api = requests.get(ep, headers=api_hdrs, proxies=proxies, timeout=15)
                if r_api.status_code == 200:
                    cand = (r_api.json().get("result") or {})
                    if isinstance(cand, dict) and cand:
                        result = cand
                        break
            except Exception as e:
                logger.warning("Aylo API fetch error (%s): %s", ep, e)

        if not result and slug:
            try:
                r_s = requests.get(
                    f"{data_api}/v2/releases",
                    params={"search": slug, "limit": "5", "type": "scene"},
                    headers=api_hdrs,
                    proxies=proxies,
                    timeout=15,
                )
                if r_s.status_code == 200:
                    arr = r_s.json().get("result") or []
                    if isinstance(arr, list) and arr:
                        result = next((x for x in arr if isinstance(x, dict) and x.get("type") == "scene"), arr[0])
            except Exception as e:
                logger.debug("Aylo slug search warning: %s", e)

        if result and isinstance((result.get("videos") or {}).get("full"), dict):
            break

    if not result:
        if is_site_ma:
            raise AyloAuthError(
                f"❌ <b>ভিডিও তথ্য (Scene {html.escape(scene_id)}) লোড করা যায়নি!</b>\n\n"
                "দয়া করে ব্রাউজার রিফ্রেশ করে নতুন <code>cookies.txt</code> এক্সপোর্ট করে পাঠান।"
            )
        return None

    # যদি parent scene থাকে (trailer/gallery id হলে)
    if result.get("type") not in ("scene", None) and isinstance(result.get("parent"), dict) and result["parent"].get("id"):
        parent_id = str(result["parent"]["id"])
        if parent_id != scene_id:
            return _fallback_aylo_member_extract(f"https://{auth_host}/scene/{parent_id}")

    title = str(result.get("title") or f"Scene {scene_id}").strip()
    thumb = _extract_aylo_best_poster(result)
    videos = result.get("videos") or {}
    full_track = videos.get("full") if isinstance(videos, dict) else None

    if not isinstance(full_track, dict) or not full_track.get("files"):
        if is_site_ma or access_token:
            if not access_token:
                raise AyloAuthError(
                    "❌ <b>মেম্বার লগইন কুকিজ পাওয়া যায়নি!</b>\n\n"
                    f"🔒 <code>{html.escape(auth_host)}</code>-এর ফুল ভিডিও ডাউনলোড করতে ব্রাউজারে লগইন থাকা অবস্থায় "
                    "<b>Cookie-Editor → Export → Netscape</b> ক্লিক করে কুকিজ ফাইলটি (অথবা টেক্সটটি) এই চ্যাটে পাঠান।"
                )
            if acc_expired:
                mins_ago = max(1, (now - (acc_exp or now)) // 60)
                raise AyloAuthError(
                    "❌ <b>আপনার মেম্বার সেশন টোকেনের মেয়াদ শেষ হয়ে গেছে!</b>\n\n"
                    f"⏳ <code>access_token_ma</code> টোকেনটি <b>{mins_ago} মিনিট আগে</b> এক্সপায়ার হয়েছে (এই টোকেনের মেয়াদ ১ ঘণ্টা থাকে)।\n\n"
                    "✅ <b>সমাধান (মাত্র ১০ সেকেন্ড লাগবে):</b>\n"
                    f"১. ব্রাউজারে <code>{html.escape(auth_host)}</code> ভিডিও পেজটি একবার <b>Refresh (F5)</b> করুন।\n"
                    "২. সাথে সাথে <b>Cookie-Editor → Export → Netscape</b> ক্লিক করে নতুন <code>cookies.txt</code> ফাইল বা টেক্সট সরাসরি এই চ্যাটে পাঠান।\n\n"
                    "💡 <i>টিপস: পেজ রিফ্রেশ করার পরপরই কুকিজ দিলে তাতে <code>refresh_token_ma</code> থাকে, ফলে বট নিজেই প্রতি ১৫ মিনিটে সেশন অটো-রিনিউ করে সচল রাখবে!</i>"
                )
            raise AyloAuthError(
                "❌ <b>ফুল ভিডিও আনলক হয়নি (শুধু ট্রেইলার পাওয়া গেছে)!</b>\n\n"
                "আপনার অ্যাকাউন্টে এই ভিডিওটির ফুল অ্যাক্সেস নেই অথবা ব্রাউজার সেশন লগআউট হয়ে গেছে। "
                "ব্রাউজারে ফুল ভিডিও প্লে হচ্ছে কিনা নিশ্চিত হয়ে নতুন <code>cookies.txt</code> পাঠান।"
            )

        mediabook = videos.get("mediabook") if isinstance(videos, dict) else None
        if not isinstance(mediabook, dict) or not mediabook.get("files"):
            return None
        track = mediabook
        title = f"{title} (Trailer)"
    else:
        track = full_track

    raw_len = int(track.get("length") or result.get("duration") or 0)
    duration = raw_len if raw_len > 5 else None
    raw_files = track.get("files")
    file_items: list[tuple[str, dict]] = []
    if isinstance(raw_files, list):
        file_items = [(str(f.get("format") or f.get("label") or ""), f) for f in raw_files if isinstance(f, dict)]
    elif isinstance(raw_files, dict):
        file_items = [(str(k), v) for k, v in raw_files.items() if isinstance(v, dict)]

    def _item_height(hint: str, f_obj: dict) -> int:
        if f_obj.get("height"):
            with contextlib.suppress(ValueError, TypeError):
                return int(f_obj["height"])
        for txt in (str(f_obj.get("format") or ""), str(f_obj.get("label") or ""), hint):
            low = txt.lower()
            if "4k" in low or "2160" in low:
                return 2160
            m_p = re.search(r"(\d{3,4})p?", low)
            if m_p:
                return int(m_p.group(1))
        return 0

    http_raw = [(k, f) for k, f in file_items if str(f.get("type") or "http").lower() == "http"]
    hls_raw = [(k, f) for k, f in file_items if str(f.get("type") or "").lower() == "hls"]

    if any(str(f.get("codec") or "h264").lower() == "h264" for _, f in http_raw):
        http_raw = [(k, f) for k, f in http_raw if str(f.get("codec") or "h264").lower() != "av1"]
    if any(str(f.get("codec") or "h264").lower() == "h264" for _, f in hls_raw):
        hls_raw = [(k, f) for k, f in hls_raw if str(f.get("codec") or "h264").lower() != "av1"]

    ref_origin = f"https://{auth_host}/"

    # ১. রেজুলেশন-ভিত্তিক সরাসরি স্ট্রিম ম্যাপিং (HTTP MP4 এবং per-resolution HLS master.m3u8 উভয়ের জন্য)
    by_height: dict[int, dict] = {}
    for proto_tag, group_list in (("http", http_raw), ("hls", hls_raw)):
        for k, f in group_list:
            urls_obj = f.get("urls") if isinstance(f.get("urls"), dict) else {}
            dl_u = (urls_obj.get("download") or urls_obj.get("view")) if proto_tag == "http" else (urls_obj.get("view") or urls_obj.get("download"))
            if not dl_u:
                continue
            h = _item_height(k, f)
            if h <= 0:
                continue
            sz = int(f.get("sizeBytes") or 0) or None
            cand_fmt = {
                "format_id": f"{proto_tag}-{h}p",
                "url": dl_u,
                "ext": "mp4",
                "height": h,
                "filesize": sz,
                "vcodec": "h264",
                "acodec": "aac",
                "_is_http": proto_tag == "http",
            }
            prev = by_height.get(h)
            if not prev or (cand_fmt["_is_http"] and not prev.get("_is_http")):
                by_height[h] = cand_fmt

    if by_height:
        formats = [by_height[h] for h in sorted(by_height)]
        best_fmt = formats[-1]
        # Quick Download ("best") এ ফাইল সাইজ ~১.৫GB এর নিচে রেখে সর্বোচ্চ HD রেজুলেশন বেছে নেওয়া (যেমন 720p/1080p),
        # যাতে ২GB সিঙ্গেল-ফাইল টেলিগ্রাম লিমিটে পার্ট না ভেঙে একটি পূর্ণ ভিডিও হিসেবেই দ্রুত যায়!
        if (best_fmt.get("filesize") or 0) > 1_500_000_000 or best_fmt["height"] > 1080:
            under_limit = [f for f in formats if (f.get("filesize") or 0) <= 1_500_000_000 and f["height"] <= 1080]
            if under_limit:
                best_fmt = under_limit[-1]
            else:
                le_1080 = [f for f in formats if f["height"] <= 1080]
                if le_1080:
                    best_fmt = le_1080[-1]
        logger.info("✅ Aylo এক্সট্র্যাকশন সফল: scene=%s qualities=%s default=%dp", scene_id, [f["height"] for f in formats], best_fmt["height"])
        return {
            "title": title,
            "duration": duration,
            "thumbnail": thumb,
            "formats": formats,
            "direct_url": best_fmt["url"],
            "referer": ref_origin,
            "is_manifest": False,
        }

    # ২. একক অ্যাডাপ্টিভ HLS master.m3u8 স্ট্রিম ফলব্যাক (যখন ফাইল অবজেক্টে আলাদা রেজুলেশন দেওয়া না থাকে)
    for _, f in hls_raw:
        urls_obj = f.get("urls") if isinstance(f.get("urls"), dict) else {}
        hls_u = urls_obj.get("view") or urls_obj.get("download")
        if not hls_u:
            continue
        heights = _parse_hls_master_heights(hls_u, ref_origin) or [480, 720, 1080]
        formats = [
            {"format_id": f"hls-{h}p", "url": hls_u, "ext": "mp4", "height": h, "vcodec": "h264", "acodec": "aac"}
            for h in heights
        ]
        logger.info("✅ Aylo অ্যাডাপ্টিভ HLS এক্সট্র্যাকশন সফল: scene=%s heights=%s", scene_id, heights)
        return {
            "title": title,
            "duration": duration,
            "thumbnail": thumb,
            "formats": formats,
            "direct_url": hls_u,
            "referer": ref_origin,
            "is_manifest": True,
        }

    return None


def _fallback_social_extract(url: str) -> Optional[dict]:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return None
    if _is_aylo_portal_url(url):
        return _fallback_aylo_member_extract(url)
    if "facebook.com" in host or "fb.watch" in host:
        return _fallback_facebook_extract(url)
    if "instagram.com" in host:
        return _fallback_instagram_extract(url)
    if "tiktok.com" in host:
        return _fallback_tiktok_extract(url)
    if host in ("x.com", "www.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"):
        return _fallback_twitter_extract(url)
    return _fallback_webpage_media_extract(url)


class _TurboHTTPAdapter(HTTPAdapter):
    """সব ডাউনলোড সকেটে TCP_NODELAY, SO_KEEPALIVE ও 1MB SO_RCVBUF সক্রিয় করে যাতে হাই-ব্যান্ডউইথ CDN স্ট্রিম থ্রটল না হয়।"""

    _SOCK_OPTS = [
        (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024),
    ]

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs.setdefault("socket_options", self._SOCK_OPTS)
        return super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs.setdefault("socket_options", self._SOCK_OPTS)
        return super().proxy_manager_for(proxy, **proxy_kwargs)


def _try_turbo_http_download(
    target_url: str,
    extra_opts: dict,
    custom_referer: Optional[str] = None,
    use_cookies: bool = False,
) -> bool:
    """
    সিঙ্গেল-ফাইল HTTP MP4/ভিডিও স্ট্রিমের জন্য ২৪-কানেকশন প্যারালাল Range: ডাউনলোডার (Linux pwrite + জিরো-কপি 512KB readinto)।
    CDN Range (HTTP 206) সাপোর্ট না করলে False রিটার্ন করে যাতে yt-dlp নির্বিঘ্নে টেকওভার করে।
    """
    outtmpl = str(extra_opts.get("outtmpl") or "")
    if not outtmpl or ".%(ext)s" not in outtmpl:
        return False
    stem_base = outtmpl.replace(".%(ext)s", "")
    hooks = extra_opts.get("progress_hooks") or []
    req_fmt = str(extra_opts.get("format") or "")
    postprocessors = extra_opts.get("postprocessors") or []
    pp_args = (extra_opts.get("postprocessor_args") or {}).get("default") or []

    is_audio_only = any(p.get("key") == "FFmpegExtractAudio" for p in postprocessors) or req_fmt == "bestaudio/best"
    is_novoice = "-an" in pp_args or req_fmt == "bestvideo/best"

    def _emit_hook(downloaded: int, total: int, speed: float, eta: Optional[float]) -> None:
        d = {
            "status": "downloading",
            "downloaded_bytes": downloaded,
            "total_bytes": total,
            "speed": speed,
            "eta": eta,
        }
        for hk in hooks:
            hk(d)

    # ক্যানসেল চেক (হুকের মাধ্যমে)
    _emit_hook(0, 0, 0.0, None)

    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    if custom_referer:
        hdrs["Referer"] = custom_referer
        p_ref = urlparse(custom_referer)
        if p_ref.scheme and p_ref.netloc:
            hdrs["Origin"] = f"{p_ref.scheme}://{p_ref.netloc}"

    cj = None
    if use_cookies and ACTIVE_COOKIES_FILE and os.path.exists(ACTIVE_COOKIES_FILE):
        with contextlib.suppress(Exception):
            cj = http.cookiejar.MozillaCookieJar(ACTIVE_COOKIES_FILE)
            cj.load(ignore_discard=True, ignore_expires=True)
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

    try:
        probe_hdrs = {**hdrs, "Range": "bytes=0-0"}
        with requests.get(
            target_url, headers=probe_hdrs, cookies=cj, proxies=proxies, stream=True, timeout=12, allow_redirects=True
        ) as r_probe:
            if r_probe.status_code != 206:
                return False
            ctype = (r_probe.headers.get("Content-Type") or "").lower()
            if "text/html" in ctype or "mpegurl" in ctype or "dash+xml" in ctype:
                return False
            cr = r_probe.headers.get("Content-Range") or ""
            m_cr = re.search(r"/(\d+)\s*$", cr)
            if not m_cr:
                return False
            total_size = int(m_cr.group(1))
            if total_size < 2 * 1024 * 1024:
                return False
            final_url = r_probe.url or target_url
    except DownloadCancelledError:
        raise
    except Exception as e:
        logger.debug("Turbo HTTP probe skipped (%s): %s", target_url[:70], e)
        return False

    raw_path = Path(f"{stem_base}_turboraw.mp4")
    worker_count = max(1, min(MAX_HTTP_RANGE_THREADS, 24))
    seg_size = max(2 * 1024 * 1024, min(10 * 1024 * 1024, math.ceil(total_size / worker_count)))
    segments: list[tuple[int, int]] = []
    pos = 0
    while pos < total_size:
        end = min(pos + seg_size - 1, total_size - 1)
        segments.append((pos, end))
        pos = end + 1

    has_pwrite = hasattr(os, "pwrite")
    write_lock = threading.Lock()
    prog_lock = threading.Lock()
    downloaded_bytes = 0
    last_hook_ts = 0.0
    abort_flag = threading.Event()
    cancelled_flag = threading.Event()
    start_t = time.time()
    fd: Optional[int] = None
    shared_sess: Optional[requests.Session] = None

    try:
        fd = os.open(str(raw_path), os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
        with contextlib.suppress(OSError):
            os.ftruncate(fd, total_size)

        # ১টি শেয়ার্ড Session/SSLContext + 1MB SO_RCVBUF সকেট টিউনিং (~৫০MB C-heap সাশ্রয় ও সর্বোচ্চ থ্রুপুট!)
        shared_sess = requests.Session()
        adapter = _TurboHTTPAdapter(pool_connections=12, pool_maxsize=worker_count + 6)
        shared_sess.mount("https://", adapter)
        shared_sess.mount("http://", adapter)
        if cj is not None:
            shared_sess.cookies = cj
        if proxies:
            shared_sess.proxies.update(proxies)

        def _dl_segment(seg: tuple[int, int]) -> None:
            nonlocal downloaded_bytes, last_hook_ts
            s_byte, e_byte = seg
            expected = e_byte - s_byte + 1
            buf = bytearray(524_288)
            for attempt in range(4):
                if abort_flag.is_set():
                    return
                cur_off = s_byte
                got = 0
                try:
                    assert shared_sess is not None
                    r_h = {**hdrs, "Range": f"bytes={s_byte}-{e_byte}"}
                    with shared_sess.get(final_url, headers=r_h, stream=True, timeout=25) as resp:
                        if resp.status_code != 206:
                            raise RuntimeError(f"HTTP {resp.status_code}")
                        ce = (resp.headers.get("Content-Encoding") or "").strip().lower()
                        use_readinto = hasattr(resp.raw, "readinto") and ce in ("", "identity")
                        if use_readinto:
                            mv = memoryview(buf)
                            try:
                                while not abort_flag.is_set():
                                    n = resp.raw.readinto(buf)
                                    if not n:
                                        break
                                    assert fd is not None
                                    view_n = mv[:n]
                                    if has_pwrite:
                                        os.pwrite(fd, view_n, cur_off)  # type: ignore[attr-defined]
                                    else:
                                        with write_lock:
                                            os.lseek(fd, cur_off, os.SEEK_SET)
                                            os.write(fd, view_n)
                                    cur_off += n
                                    got += n
                                    should_emit = False
                                    now_t = time.time()
                                    with prog_lock:
                                        downloaded_bytes += n
                                        done_snap = downloaded_bytes
                                        if done_snap >= total_size or (now_t - last_hook_ts) >= 0.20:
                                            last_hook_ts = now_t
                                            should_emit = True
                                    if should_emit:
                                        elapsed = max(0.05, now_t - start_t)
                                        spd = done_snap / elapsed
                                        rem = ((total_size - done_snap) / spd) if spd > 0 else None
                                        try:
                                            _emit_hook(done_snap, total_size, spd, rem)
                                        except DownloadCancelledError:
                                            cancelled_flag.set()
                                            abort_flag.set()
                                            return
                                if abort_flag.is_set():
                                    return
                            finally:
                                mv.release()
                        else:
                            for blk in resp.iter_content(chunk_size=262_144):
                                if abort_flag.is_set():
                                    return
                                if not blk:
                                    continue
                                assert fd is not None
                                if has_pwrite:
                                    os.pwrite(fd, blk, cur_off)  # type: ignore[attr-defined]
                                else:
                                    with write_lock:
                                        os.lseek(fd, cur_off, os.SEEK_SET)
                                        os.write(fd, blk)
                                n = len(blk)
                                cur_off += n
                                got += n
                                should_emit = False
                                now_t = time.time()
                                with prog_lock:
                                    downloaded_bytes += n
                                    done_snap = downloaded_bytes
                                    if done_snap >= total_size or (now_t - last_hook_ts) >= 0.20:
                                        last_hook_ts = now_t
                                        should_emit = True
                                if should_emit:
                                    elapsed = max(0.05, now_t - start_t)
                                    spd = done_snap / elapsed
                                    rem = ((total_size - done_snap) / spd) if spd > 0 else None
                                    try:
                                        _emit_hook(done_snap, total_size, spd, rem)
                                    except DownloadCancelledError:
                                        cancelled_flag.set()
                                        abort_flag.set()
                                        return
                    if got == expected:
                        if fd is not None:
                            _drop_file_page_cache(fd, s_byte, expected, sync_first=False)
                        return
                    with prog_lock:
                        downloaded_bytes = max(0, downloaded_bytes - got)
                    raise RuntimeError(f"Incomplete segment ({got}/{expected})")
                except DownloadCancelledError:
                    cancelled_flag.set()
                    abort_flag.set()
                    return
                except Exception:
                    if got > 0:
                        with prog_lock:
                            downloaded_bytes = max(0, downloaded_bytes - got)
                    if attempt == 3:
                        abort_flag.set()
                        raise
                    time.sleep(0.25 * (attempt + 1))

        with ThreadPoolExecutor(max_workers=min(worker_count, len(segments))) as pool:
            futs = [pool.submit(_dl_segment, seg) for seg in segments]
            for fut in as_completed(futs):
                if cancelled_flag.is_set():
                    abort_flag.set()
                    break
                fut.result()

        if cancelled_flag.is_set():
            raise DownloadCancelledError("ডাউনলোড বাতিল করা হয়েছে")

        _drop_file_page_cache(fd, 0, 0, sync_first=True)
        os.close(fd)
        fd = None
        if shared_sess is not None:
            shared_sess.close()
            shared_sess = None
        _trim_process_memory()

        elapsed_dl = max(0.05, time.time() - start_t)
        mb = total_size / (1024 * 1024)
        logger.info("⚡ Turbo HTTP Range ডাউনলোড সম্পন্ন: %.1f MB in %.1fs (%.1f MB/s, threads=%d)", mb, elapsed_dl, mb / elapsed_dl, worker_count)

        if is_audio_only:
            out_mp3 = Path(f"{stem_base}.mp3")
            _run_ffmpeg(["-threads", "2", "-i", str(raw_path), "-vn", "-acodec", "libmp3lame", "-b:a", "192k", str(out_mp3)])
            raw_path.unlink(missing_ok=True)
            _drop_path_page_cache(out_mp3, sync_first=True)
        elif is_novoice:
            out_nv = Path(f"{stem_base}.mp4")
            _run_ffmpeg(["-threads", "2", "-i", str(raw_path), "-c:v", "copy", "-an", str(out_nv)])
            raw_path.unlink(missing_ok=True)
            _drop_path_page_cache(out_nv, sync_first=True)
        else:
            u_ext = Path(urlparse(final_url).path or "").suffix.lower()
            real_ext = u_ext if u_ext in (".mp4", ".mkv", ".webm", ".mov") else ".mp4"
            out_vid = Path(f"{stem_base}{real_ext}")
            raw_path.replace(out_vid)
        return True
    except DownloadCancelledError:
        raise
    except Exception as e:
        logger.warning("Turbo HTTP Range ফলব্যাক (%s) — yt-dlp দিয়ে চেষ্টা করা হচ্ছে...", e)
        return False
    finally:
        if shared_sess is not None:
            with contextlib.suppress(Exception):
                shared_sess.close()
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)
        raw_path.unlink(missing_ok=True)
        _trim_process_memory()


def _try_turbo_hls_download(
    manifest_url: str,
    extra_opts: dict,
    custom_referer: Optional[str] = None,
    use_cookies: bool = False,
) -> bool:
    """
    ২৮-থ্রেড কিপ-অ্যালাইভ Turbo HLS (.m3u8) ডাউনলোডার + অন-দ্য-ফ্লাই লাইভ স্ট্রিমিং পাইপ মাক্সার (~০.১ সেকেন্ডে তাৎক্ষণিক ফিনিশ!)।
    এনক্রিপ্টেড বা fMP4/লাইভ স্ট্রিম হলে False রিটার্ন করে যাতে yt-dlp নির্বিঘ্নে হ্যান্ডেল করে।
    """
    if not FFMPEG_AVAILABLE:
        return False
    outtmpl = str(extra_opts.get("outtmpl") or "")
    if not outtmpl or ".%(ext)s" not in outtmpl:
        return False
    stem_base = outtmpl.replace(".%(ext)s", "")
    hooks = extra_opts.get("progress_hooks") or []
    req_fmt = str(extra_opts.get("format") or "")
    postprocessors = extra_opts.get("postprocessors") or []
    pp_args = (extra_opts.get("postprocessor_args") or {}).get("default") or []

    is_audio_only = any(p.get("key") == "FFmpegExtractAudio" for p in postprocessors) or req_fmt == "bestaudio/best"
    is_novoice = "-an" in pp_args or req_fmt == "bestvideo/best"

    def _emit_hook(downloaded: int, total: int, speed: float, eta: Optional[float]) -> None:
        d = {
            "status": "downloading",
            "downloaded_bytes": downloaded,
            "total_bytes_estimate": total,
            "speed": speed,
            "eta": eta,
        }
        for hk in hooks:
            hk(d)

    _emit_hook(0, 0, 0.0, None)

    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    if custom_referer:
        hdrs["Referer"] = custom_referer
        p_ref = urlparse(custom_referer)
        if p_ref.scheme and p_ref.netloc:
            hdrs["Origin"] = f"{p_ref.scheme}://{p_ref.netloc}"

    cj = None
    if use_cookies and ACTIVE_COOKIES_FILE and os.path.exists(ACTIVE_COOKIES_FILE):
        with contextlib.suppress(Exception):
            cj = http.cookiejar.MozillaCookieJar(ACTIVE_COOKIES_FILE)
            cj.load(ignore_discard=True, ignore_expires=True)
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

    def _get_manifest_with_retry(m_u: str) -> Optional[requests.Response]:
        for m_try in range(4):
            try:
                r = requests.get(m_u, headers=hdrs, cookies=cj, proxies=proxies, timeout=12, allow_redirects=True)
                if r.status_code == 200:
                    return r
                if r.status_code in (429, 500, 502, 503, 504) and m_try < 3:
                    ra_hdr = r.headers.get("Retry-After") or ""
                    wait_m = float(ra_hdr) if ra_hdr.strip().isdigit() else (2.5 * (m_try + 1))
                    time.sleep(min(15.0, max(1.5, wait_m)))
                    continue
                return None
            except Exception:
                if m_try == 3:
                    raise
                time.sleep(1.5 * (m_try + 1))
        return None

    try:
        r_m = _get_manifest_with_retry(manifest_url)
        if r_m is None or r_m.status_code != 200:
            return False
        m_text = r_m.text or ""
        base_m_url = r_m.url or manifest_url

        # যদি এটি Master Playlist হয় (#EXT-X-STREAM-INF), তাহলে যদি আলাদা #EXT-X-MEDIA অডিও ট্র্যাক থাকে তবে yt-dlp কে দেওয়া;
        # আর muxed স্ট্রিম হলে সেরা রেজুলেশন ভ্যারিয়েন্টটি বেছে নেওয়া।
        if "#EXT-X-STREAM-INF" in m_text:
            if "#EXT-X-MEDIA:TYPE=AUDIO" in m_text and 'URI="' in m_text:
                return False
            m_cap = re.search(r"height<=(\d+)", req_fmt)
            cap_h = int(m_cap.group(1)) if m_cap else (1080 if "best" in req_fmt else 9999)
            variants: list[tuple[int, int, str]] = []
            lines_m = [ln.strip() for ln in m_text.splitlines() if ln.strip()]
            for idx_l, ln in enumerate(lines_m):
                if ln.startswith("#EXT-X-STREAM-INF:") and idx_l + 1 < len(lines_m):
                    nxt = lines_m[idx_l + 1]
                    if nxt.startswith("#"):
                        continue
                    bw_m = re.search(r"BANDWIDTH=(\d+)", ln)
                    res_m = re.search(r"RESOLUTION=\d+x(\d+)", ln)
                    bw = int(bw_m.group(1)) if bw_m else 0
                    vh = int(res_m.group(1)) if res_m else 0
                    variants.append((vh, bw, urljoin(base_m_url, nxt)))
            if not variants:
                return False
            under_v = [v for v in variants if v[0] == 0 or v[0] <= cap_h]
            chosen = sorted(under_v or variants, key=lambda x: (x[0], x[1]))[-1]
            base_m_url = chosen[2]
            r_sub = _get_manifest_with_retry(base_m_url)
            if r_sub is None or r_sub.status_code != 200:
                return False
            m_text = r_sub.text or ""
            base_m_url = r_sub.url or base_m_url

        # এনক্রিপ্টেড (#EXT-X-KEY), fMP4 (#EXT-X-MAP), বাইট-রেঞ্জ বা লাইভ স্ট্রিম হলে yt-dlp তে ফলব্যাক
        if (
            "#EXT-X-KEY" in m_text
            or "#EXT-X-MAP" in m_text
            or "#EXT-X-BYTERANGE" in m_text
            or "#EXT-X-ENDLIST" not in m_text
            or "#EXTINF" not in m_text
        ):
            return False

        seg_urls: list[str] = []
        for ln in m_text.splitlines():
            s = ln.strip()
            if not s or s.startswith("#"):
                continue
            seg_urls.append(urljoin(base_m_url, s))

        if len(seg_urls) < 2:
            return False
    except DownloadCancelledError:
        raise
    except Exception as e:
        logger.debug("Turbo HLS manifest probe skipped: %s", e)
        return False

    seg_dir = Path(f"{stem_base}_hlssegs")
    seg_dir.mkdir(parents=True, exist_ok=True)
    total_segs = len(seg_urls)
    has_discontinuity = "#EXT-X-DISCONTINUITY" in m_text
    can_live_pipe_mux = (not is_audio_only) and (not has_discontinuity) and (total_segs >= 4)
    is_strict_cdn = "project1content.com" in base_m_url.lower()
    max_hls_workers = 12 if is_strict_cdn else 28
    workers_n = max(4, min(MAX_DL_THREADS, max_hls_workers, total_segs))
    prog_lock = threading.Lock()
    rate_lock = threading.Lock()
    mux_cond = threading.Condition()
    ready_segs: set[int] = set()
    pipe_mux_ok = False
    live_mux_proc: Optional[subprocess.Popen] = None
    mux_thread: Optional[threading.Thread] = None
    out_live_mp4 = Path(f"{stem_base}.mp4")
    tight_disk = _free_disk_mb() < 4096
    cooldown_until = 0.0
    next_req_ts = 0.0
    pace_interval = 0.0
    done_segs = 0
    downloaded_bytes = 0
    last_hook_ts = 0.0
    abort_flag = threading.Event()
    cancelled_flag = threading.Event()
    start_t = time.time()
    shared_sess: Optional[requests.Session] = None

    def _mark_abort() -> None:
        abort_flag.set()
        with mux_cond:
            mux_cond.notify_all()

    def _live_mux_feeder() -> None:
        """ডাউনলোড চলাকালীনই ক্রমানুসারে s_00000..s_{N-1}.ts সরাসরি ffmpeg pipe:0-এ ফিড করে যাতে শেষ সেগমেন্টেই MP4 প্রস্তুত হয়ে যায়!"""
        nonlocal pipe_mux_ok, live_mux_proc
        err_log_f = None
        proc: Optional[subprocess.Popen] = None
        try:
            for m_idx in range(total_segs):
                with mux_cond:
                    while m_idx not in ready_segs and not abort_flag.is_set():
                        mux_cond.wait(timeout=0.5)
                    if abort_flag.is_set():
                        return
                seg_p = seg_dir / f"s_{m_idx:05d}.ts"
                if not seg_p.exists():
                    return
                if m_idx == 0:
                    with seg_p.open("rb") as pf:
                        hdr_b = pf.read(192)
                    if not hdr_b or hdr_b[0] != 0x47 or (len(hdr_b) >= 189 and hdr_b[188] != 0x47):
                        return
                    err_log_f = open(seg_dir / "live_mux_err.log", "wb")
                    ff_cmd = [
                        "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostats", "-y",
                        "-threads", "2", "-f", "mpegts", "-i", "pipe:0",
                    ]
                    if is_novoice:
                        ff_cmd += ["-c:v", "copy", "-an", str(out_live_mp4)]
                    else:
                        ff_cmd += ["-c", "copy", "-bsf:a", "aac_adtstoasc", str(out_live_mp4)]
                    proc = subprocess.Popen(
                        ff_cmd,
                        stdin=subprocess.PIPE,
                        stdout=subprocess.DEVNULL,
                        stderr=err_log_f,
                        bufsize=1024 * 1024,
                    )
                    live_mux_proc = proc

                assert proc is not None and proc.stdin is not None
                with seg_p.open("rb") as sf:
                    while True:
                        if abort_flag.is_set():
                            return
                        blk_ts = sf.read(524_288)
                        if not blk_ts:
                            break
                        proc.stdin.write(blk_ts)
                if tight_disk:
                    seg_p.unlink(missing_ok=True)
                else:
                    _drop_path_page_cache(seg_p, sync_first=False)

            if proc is not None and proc.stdin is not None:
                proc.stdin.close()
                proc.wait(timeout=180)
                if not abort_flag.is_set() and proc.returncode == 0 and out_live_mp4.exists() and out_live_mp4.stat().st_size > 0:
                    pipe_mux_ok = True
        except Exception as lm_err:
            logger.debug("Live HLS pipe mux skipped (%s) — concat fallback ready.", lm_err)
        finally:
            if proc is not None:
                with contextlib.suppress(Exception):
                    if proc.stdin and not proc.stdin.closed:
                        proc.stdin.close()
                if proc.poll() is None:
                    with contextlib.suppress(Exception):
                        proc.kill()
                        proc.wait(timeout=3)
            live_mux_proc = None
            if err_log_f is not None:
                with contextlib.suppress(Exception):
                    err_log_f.close()
            if not pipe_mux_ok:
                out_live_mp4.unlink(missing_ok=True)

    def _sleep_cancelable(sec: float) -> bool:
        end_m = time.monotonic() + max(0.0, sec)
        while time.monotonic() < end_m:
            if abort_flag.is_set():
                return False
            time.sleep(min(0.25, max(0.01, end_m - time.monotonic())))
        return not abort_flag.is_set()

    def _await_rate_gate() -> bool:
        nonlocal next_req_ts
        while not abort_flag.is_set():
            with rate_lock:
                now_m = time.monotonic()
                cd_wait = cooldown_until - now_m
                if cd_wait <= 0:
                    slot_wait = max(0.0, next_req_ts - now_m)
                    if pace_interval > 0.0:
                        next_req_ts = max(now_m, next_req_ts) + pace_interval
                    break
            if not _sleep_cancelable(cd_wait + random.uniform(0.05, 0.35)):
                return False
        else:
            return False
        if slot_wait > 0.002:
            return _sleep_cancelable(min(slot_wait, 10.0))
        return not abort_flag.is_set()

    def _note_cdn_rate_limit(rl_try: int, retry_after_hdr: Optional[str] = None) -> None:
        nonlocal cooldown_until, pace_interval, next_req_ts
        ra_sec = 0.0
        if retry_after_hdr and retry_after_hdr.strip().isdigit():
            ra_sec = float(retry_after_hdr.strip())
        wait_s = max(ra_sec, min(25.0, 2.5 * (1.55 ** rl_try))) + random.uniform(0.15, 0.65)
        with rate_lock:
            now_m = time.monotonic()
            if now_m >= cooldown_until:
                logger.info("⏳ CDN HTTP 429 রেট-লিমিট শনাক্ত — সব থ্রেড %.1fs কুলডাউন নিয়ে স্বয়ংক্রিয়ভাবে রিজিউম করবে...", wait_s)
            cooldown_until = max(cooldown_until, now_m + wait_s)
            pace_interval = min(0.16, max(0.045, pace_interval * 1.45 if pace_interval > 0 else 0.045))
            next_req_ts = max(next_req_ts, cooldown_until)

    try:
        # ১টি শেয়ার্ড SSLContext/PoolManager + 1MB SO_RCVBUF সকেট টিউনিং (প্রায় ১৬০ MB OpenSSL C-heap সাশ্রয়!)
        shared_sess = requests.Session()
        adapter = _TurboHTTPAdapter(pool_connections=16, pool_maxsize=workers_n + 8)
        shared_sess.mount("https://", adapter)
        shared_sess.mount("http://", adapter)
        if cj is not None:
            shared_sess.cookies = cj
        if proxies:
            shared_sess.proxies.update(proxies)

        if can_live_pipe_mux:
            mux_thread = threading.Thread(target=_live_mux_feeder, daemon=True, name="hls-live-mux")
            mux_thread.start()

        def _fetch_one(idx_and_url: tuple[int, str]) -> None:
            nonlocal done_segs, downloaded_bytes, last_hook_ts, pace_interval
            s_idx, s_url = idx_and_url
            out_seg = seg_dir / f"s_{s_idx:05d}.ts"
            tmp_seg = seg_dir / f"s_{s_idx:05d}.ts.part"
            if out_seg.exists() and out_seg.stat().st_size > 512:
                n_prev = out_seg.stat().st_size
                with prog_lock:
                    done_segs += 1
                    downloaded_bytes += n_prev
                if can_live_pipe_mux:
                    with mux_cond:
                        ready_segs.add(s_idx)
                        mux_cond.notify_all()
                return

            buf = bytearray(524_288)
            rl_tries = 0
            err_tries = 0
            while not abort_flag.is_set():
                if not _await_rate_gate():
                    return
                seg_fd: Optional[int] = None
                n_b = 0
                try:
                    assert shared_sess is not None
                    with shared_sess.get(s_url, headers=hdrs, stream=True, timeout=22) as resp:
                        if resp.status_code in (429, 500, 502, 503, 504):
                            if resp.status_code == 429:
                                _note_cdn_rate_limit(rl_tries, resp.headers.get("Retry-After"))
                            else:
                                _sleep_cancelable(min(12.0, 1.5 * (rl_tries + 1)))
                            rl_tries += 1
                            if rl_tries > 14:
                                _mark_abort()
                                raise RuntimeError(f"HTTP {resp.status_code} (after {rl_tries} retries)")
                            continue
                        if resp.status_code != 200:
                            raise RuntimeError(f"HTTP {resp.status_code}")
                        seg_fd = os.open(str(tmp_seg), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
                        ce = (resp.headers.get("Content-Encoding") or "").strip().lower()
                        if hasattr(resp.raw, "readinto") and ce in ("", "identity"):
                            mv = memoryview(buf)
                            try:
                                while not abort_flag.is_set():
                                    nr = resp.raw.readinto(buf)
                                    if not nr:
                                        break
                                    os.write(seg_fd, mv[:nr])
                                    n_b += nr
                                if abort_flag.is_set():
                                    return
                            finally:
                                mv.release()
                        else:
                            for chunk in resp.iter_content(chunk_size=262_144):
                                if abort_flag.is_set():
                                    return
                                if not chunk:
                                    continue
                                os.write(seg_fd, chunk)
                                n_b += len(chunk)
                    if n_b <= 0:
                        raise RuntimeError("Empty HLS segment")
                    if not can_live_pipe_mux:
                        _drop_file_page_cache(seg_fd, 0, 0, sync_first=False)
                    os.close(seg_fd)
                    seg_fd = None
                    tmp_seg.replace(out_seg)

                    if can_live_pipe_mux:
                        with mux_cond:
                            ready_segs.add(s_idx)
                            mux_cond.notify_all()

                    if pace_interval > 0.0:
                        with rate_lock:
                            pace_interval = pace_interval * 0.985 if pace_interval > 0.008 else 0.0

                    should_emit = False
                    now_t = time.time()
                    with prog_lock:
                        done_segs += 1
                        downloaded_bytes += n_b
                        d_segs_snap = done_segs
                        d_bytes_snap = downloaded_bytes
                        if d_segs_snap >= total_segs or (now_t - last_hook_ts) >= 0.20:
                            last_hook_ts = now_t
                            should_emit = True
                    if should_emit:
                        est_total = int(d_bytes_snap * (total_segs / max(1, d_segs_snap)))
                        elapsed = max(0.05, now_t - start_t)
                        spd = d_bytes_snap / elapsed
                        rem = ((est_total - d_bytes_snap) / spd) if spd > 0 else None
                        try:
                            _emit_hook(d_bytes_snap, est_total, spd, rem)
                        except DownloadCancelledError:
                            cancelled_flag.set()
                            _mark_abort()
                            return
                    return
                except DownloadCancelledError:
                    cancelled_flag.set()
                    _mark_abort()
                    return
                except Exception:
                    err_tries += 1
                    if err_tries >= 6:
                        _mark_abort()
                        raise
                    if not _sleep_cancelable(0.6 * err_tries):
                        return
                finally:
                    if seg_fd is not None:
                        with contextlib.suppress(OSError):
                            os.close(seg_fd)
                    tmp_seg.unlink(missing_ok=True)

        with ThreadPoolExecutor(max_workers=workers_n) as pool:
            futs = [pool.submit(_fetch_one, (i, u)) for i, u in enumerate(seg_urls)]
            for fut in as_completed(futs):
                if cancelled_flag.is_set():
                    _mark_abort()
                    break
                fut.result()

        if cancelled_flag.is_set():
            _mark_abort()
            raise DownloadCancelledError("ডাউনলোড বাতিল করা হয়েছে")

        if shared_sess is not None:
            shared_sess.close()
            shared_sess = None

        dl_elapsed = max(0.05, time.time() - start_t)
        mb = downloaded_bytes / (1024 * 1024)

        t_mux = time.time()
        if mux_thread is not None:
            mux_thread.join(timeout=180)
            mux_thread = None

        if pipe_mux_ok and out_live_mp4.exists() and out_live_mp4.stat().st_size > 0:
            out_final = out_live_mp4
            mux_mode = "live-pipe"
        else:
            # ফলব্যাক সিঙ্গেল-পাস স্ট্রিম-কপি মাক্সিং (#EXT-X-DISCONTINUITY বা MP3 অডিও মোডের জন্য)
            if tight_disk and any(not (seg_dir / f"s_{i:05d}.ts").exists() for i in range(total_segs)):
                raise RuntimeError("Live pipe mux failed under tight disk mode")
            concat_file = seg_dir / "concat.txt"
            concat_lines = [f"file 's_{i:05d}.ts'" for i in range(total_segs)]
            concat_file.write_text("\n".join(concat_lines) + "\n", encoding="utf-8")
            pre_in = ["-fflags", "+genpts"] if has_discontinuity else []
            if is_audio_only:
                out_final = Path(f"{stem_base}.mp3")
                _run_ffmpeg(
                    ["-threads", "2", *pre_in, "-f", "concat", "-safe", "0", "-i", str(concat_file), "-vn", "-acodec", "libmp3lame", "-b:a", "192k", str(out_final)]
                )
            elif is_novoice:
                out_final = Path(f"{stem_base}.mp4")
                _run_ffmpeg(
                    ["-threads", "2", *pre_in, "-f", "concat", "-safe", "0", "-i", str(concat_file), "-c:v", "copy", "-an", str(out_final)]
                )
            else:
                out_final = Path(f"{stem_base}.mp4")
                _run_ffmpeg(
                    ["-threads", "2", *pre_in, "-f", "concat", "-safe", "0", "-i", str(concat_file), "-c", "copy", "-bsf:a", "aac_adtstoasc", str(out_final)]
                )
            mux_mode = "concat"

        shutil.rmtree(seg_dir, ignore_errors=True)
        _drop_path_page_cache(out_final, sync_first=True)
        _trim_process_memory()
        mux_elapsed = max(0.01, time.time() - t_mux)
        logger.info(
            "⚡ Turbo HLS ডাউনলোড ও ইনস্ট্যান্ট মার্জ সম্পন্ন: %.1f MB (%d segs) DL=%.1fs (%.1f MB/s, %d threads), Mux=%.2fs (%s)!",
            mb, total_segs, dl_elapsed, mb / dl_elapsed, workers_n, mux_elapsed, mux_mode,
        )
        return out_final.exists() and out_final.stat().st_size > 0
    except DownloadCancelledError:
        _mark_abort()
        raise
    except Exception as e:
        _mark_abort()
        logger.warning("Turbo HLS ফলব্যাক (%s) — yt-dlp দিয়ে চেষ্টা করা হচ্ছে...", e)
        return False
    finally:
        _mark_abort()
        if live_mux_proc is not None and live_mux_proc.poll() is None:
            with contextlib.suppress(Exception):
                live_mux_proc.kill()
        if mux_thread is not None:
            with contextlib.suppress(Exception):
                mux_thread.join(timeout=3.0)
        if shared_sess is not None:
            with contextlib.suppress(Exception):
                shared_sess.close()
        shutil.rmtree(seg_dir, ignore_errors=True)
        _trim_process_memory()


def _purge_partial_frag_files(outtmpl: str) -> None:
    """yt-dlp ফলব্যাক বা রিট্রাইয়ের আগে অর্ধসমাপ্ত .part / .part-Frag* / .ytdl পরিষ্কার করে যাতে [Errno 2] কখনো না হয়।"""
    if not outtmpl or ".%(ext)s" not in outtmpl:
        return
    stem_p = Path(outtmpl.replace(".%(ext)s", ""))
    parent = stem_p.parent
    pref = stem_p.name
    if not parent.exists():
        return
    for p in parent.glob(f"{pref}*"):
        if ".part" in p.name or p.name.endswith(".ytdl"):
            with contextlib.suppress(OSError):
                if p.is_dir():
                    shutil.rmtree(p, ignore_errors=True)
                else:
                    p.unlink(missing_ok=True)


def _download_from_fallback_info(fb_info: dict, extra_opts: dict) -> dict:
    """ফলব্যাক থেকে পাওয়া সরাসরি CDN স্ট্রিম বা HLS ম্যানিফেস্ট সর্বোচ্চ স্পিডে ডাউনলোড ও postprocess করে।"""
    formats = fb_info.get("formats") or []
    target_url = fb_info.get("direct_url")
    ref = fb_info.get("referer")
    is_manifest = bool(fb_info.get("is_manifest"))
    req_fmt = str(extra_opts.get("format") or "")
    outtmpl_str = str(extra_opts.get("outtmpl") or "")
    use_c = _cookies_usable_for_url(ref or target_url or "")

    if not is_manifest:
        m_h = re.search(r"height<=(\d+)", req_fmt)
        if m_h and formats:
            max_h = int(m_h.group(1))
            under = [f for f in formats if (f.get("height") or 0) <= max_h and f.get("url")]
            if under:
                target_url = sorted(under, key=lambda x: x.get("height") or 0, reverse=True)[0]["url"]

        if target_url and ".m3u8" not in target_url.lower() and ".mpd" not in target_url.lower():
            if _try_turbo_http_download(target_url, extra_opts, custom_referer=ref, use_cookies=use_c):
                return fb_info
        elif target_url and ".m3u8" in target_url.lower():
            if _try_turbo_hls_download(target_url, extra_opts, custom_referer=ref, use_cookies=use_c):
                return fb_info
    elif target_url and ".m3u8" in target_url.lower():
        if _try_turbo_hls_download(target_url, extra_opts, custom_referer=ref, use_cookies=use_c):
            return fb_info

    _purge_partial_frag_files(outtmpl_str)
    dl_opts = _base_ydl_opts(
        use_cookies=use_c,
        is_youtube=False,
        url=target_url,
        custom_referer=ref,
    )
    for k in ("outtmpl", "progress_hooks", "postprocessors", "merge_output_format"):
        if k in extra_opts:
            dl_opts[k] = extra_opts[k]

    if target_url and "project1content.com" in target_url:
        # CDN রেট-লিমিট (HTTP 429) ও .part-Frag মিসিং এড়াতে নিরাপদ ৪-কানেকশন ফ্র্যাগমেন্ট ফলব্যাক
        dl_opts["concurrent_fragment_downloads"] = 4

    if is_manifest and req_fmt:
        # Quick Download ("best") এ ৪কে (2160p) স্ট্রিম থাকলে ডিফল্টে 1080p ক্যাপ করা যাতে কন্টেইনার ডিস্ক ফুল না হয়;
        # তবে ইউজার Quality মেনু থেকে নিজে 2160p সিলেক্ট করলে (height<=2160) পূর্ণ 4K ডাউনলোড হবে।
        has_4k = any((f.get("height") or 0) > 1080 for f in formats)
        has_1080 = any(720 <= (f.get("height") or 0) <= 1080 for f in formats)
        if has_4k and has_1080 and req_fmt == "bestvideo+bestaudio/bestvideo/bestaudio/best":
            dl_opts["format"] = "bestvideo[height<=1080]+bestaudio/best[height<=1080]/bestvideo+bestaudio/best"
        else:
            dl_opts["format"] = req_fmt
    else:
        dl_opts["format"] = "bestaudio/best" if "bestaudio/best" == req_fmt else "best"
        if req_fmt == "bestvideo/best":
            dl_opts["postprocessor_args"] = {"default": ["-loglevel", "error", "-nostats", "-an"]}

    try:
        with yt_dlp.YoutubeDL(dl_opts) as ydl:
            ydl.extract_info(target_url, download=True)
    except DownloadCancelledError:
        raise
    except Exception as ydl_err:
        err_s = str(ydl_err)
        if "part-Frag" in err_s or "No such file or directory" in err_s or "429" in err_s:
            logger.warning("yt-dlp fragment fallback retry after cleanup (%s)", err_s[:160])
            _purge_partial_frag_files(outtmpl_str)
            time.sleep(4.0)
            retry_opts = {**dl_opts, "concurrent_fragment_downloads": 2}
            with yt_dlp.YoutubeDL(retry_opts) as ydl_r:
                ydl_r.extract_info(target_url, download=True)
        else:
            raise
    return fb_info


def _unpack_stored_url(stored: str) -> tuple[str, Optional[str]]:
    """LINKS এ 'media_url\\nreferer_url' থাকলে আলাদা করে রিটার্ন করে।"""
    if "\n" in stored:
        u, ref = stored.split("\n", 1)
        return u.strip(), (ref.strip() or None)
    return stored.strip(), None


def _enrich_manifest_metadata(url: str, info: Optional[dict]) -> Optional[dict]:
    """সরাসরি .m3u8 / .mpd / Bunny CDN লিংকের ক্ষেত্রে ক্লিন টাইটেল ও অটো-থাম্বনেইল যুক্ত করে।"""
    if not isinstance(info, dict):
        return info
    raw_title = (info.get("title") or "").strip()
    if raw_title.lower() in ("", "playlist", "master", "index", "video", "manifest", "stream"):
        segs = [s for s in (urlparse(url).path or "").split("/") if s and not s.lower().endswith((".m3u8", ".mpd"))]
        short_tag = segs[-1][:12] if segs else "Stream"
        info["title"] = f"Stream Video ({short_tag})"
    if not info.get("thumbnail") and "b-cdn.net/" in url.lower() and url.lower().split("?", 1)[0].endswith(".m3u8"):
        base_dir = url.split("?", 1)[0].rsplit("/", 1)[0]
        info["thumbnail"] = f"{base_dir}/thumbnail.jpg"
    return info


def _run_ydl(url: str, extra_opts: dict, download: bool, prefer_best_quality: bool = False) -> Optional[dict]:
    """
    yt-dlp চালায়, সব প্ল্যাটফর্মের (YouTube, FB, Insta, TikTok, X, Aylo SPA, Bunny/HLS .m3u8 ইত্যাদি) জন্য
    মাল্টি-স্ট্র্যাটেজি + স্মার্ট Referer অটো-ফলব্যাক + ওয়েবপেজ স্নিফার ফলব্যাক সহ।
    """
    url, custom_referer = _unpack_stored_url(url)

    # site-ma.* React SPA সাইটে yt-dlp এর নেটিভ এক্সট্র্যাক্টর নেই — সরাসরি ডেডিকেটেড API এক্সট্র্যাক্টর চালানো
    if _is_aylo_site_ma_url(url):
        fb_info = (_get_cached_extract_info(url) if download else None) or _fallback_aylo_member_extract(url)
        if fb_info:
            _put_cached_extract_info(url, fb_info)
            return _download_from_fallback_info(fb_info, extra_opts) if download else fb_info

    is_yt = _is_youtube_url(url)
    usable_cookies = _cookies_usable_for_url(custom_referer or url)

    # সরাসরি .mp4/.mkv/.webm/.mov বা .m3u8 ফাইল লিংক হলে প্রথমেই Turbo ডাউনলোডার চেষ্টা (জিরো পোস্টপ্রসেসিং ল্যাগ!)
    if download and not is_yt:
        direct_path_low = (urlparse(url).path or "").lower()
        if direct_path_low.endswith((".mp4", ".mkv", ".webm", ".mov")):
            if _try_turbo_http_download(url, extra_opts, custom_referer=custom_referer, use_cookies=usable_cookies):
                seg_name = Path(direct_path_low).stem[:60] or "Direct Video"
                return {"title": seg_name}
        elif direct_path_low.endswith(".m3u8") or ".m3u8" in direct_path_low:
            eff_ref = custom_referer or ("https://iframe.mediadelivery.net/" if "b-cdn.net" in url.lower() else None)
            if _try_turbo_hls_download(url, extra_opts, custom_referer=eff_ref, use_cookies=usable_cookies):
                return _enrich_manifest_metadata(url, {"title": "playlist"})

    if is_yt:
        strategies: list[dict] = []
        if usable_cookies:
            # বৈধ YouTube কুকিজ থাকলে প্রথমেই কুকিজ + yt-dlp ডিফল্ট/PO-token চেষ্টা
            strategies.append({"use_cookies": True, "delay": 0.0, "clients": None, "impersonate": False})
        strategies.extend(
            [
                # ১. yt-dlp এর লেটেস্ট ডিফল্ট আনঅথেন্টিকেটেড ক্লায়েন্টস (tv, visionos, ios/mweb + bgutil-pot)
                {"use_cookies": False, "delay": 0.0, "clients": None, "impersonate": False},
                # ২. android + mweb (কুকিজ ছাড়া — কারণ android ক্লায়েন্টে কুকিজ সাপোর্ট নেই)
                {"use_cookies": False, "delay": 0.5, "clients": ["android", "mweb"], "impersonate": False},
                # ৩. ios + web_safari
                {"use_cookies": False, "delay": 0.8, "clients": ["ios", "web_safari"], "impersonate": False},
                # ৪. web + bgutil-pot PO Token
                {"use_cookies": False, "delay": 1.0, "clients": ["web"], "impersonate": False},
            ]
        )
    else:
        strategies = []
        if usable_cookies:
            strategies.append({"use_cookies": True, "delay": 0.0, "clients": None, "impersonate": False, "referer": custom_referer})
        strategies.append({"use_cookies": False, "delay": 0.0, "clients": None, "impersonate": False, "referer": custom_referer})
        url_low = url.lower()
        if not custom_referer and ("b-cdn.net" in url_low or "mediadelivery.net" in url_low):
            # Bunny Stream pullzone এ ডিফল্ট নো-রেফারার ব্লক (403) থাকলে অফিসিয়াল এম্বেড ডোমেইন ও পরিচিত রেফারার অটো-ফলব্যাক
            strategies.append(
                {"use_cookies": False, "delay": 0.2, "clients": None, "impersonate": False, "referer": "https://iframe.mediadelivery.net/"}
            )
            strategies.append(
                {"use_cookies": False, "delay": 0.2, "clients": None, "impersonate": False, "referer": "https://redwansmethod.com/"}
            )
        if ImpersonateTarget is not None:
            strategies.append({"use_cookies": usable_cookies, "delay": 0.4, "clients": None, "impersonate": True, "referer": custom_referer})

    last_err: Optional[Exception] = None
    best_info: Optional[dict] = None
    best_height = -1

    for i, strat in enumerate(strategies):
        if strat["delay"]:
            time.sleep(strat["delay"])
        # রানটাইমে কুকিজ এক্সপায়ার্ড ধরা পড়লে পরের চেষ্টায় আর কুকিজ পাঠানো হবে না
        use_c = strat["use_cookies"] and not (is_yt and YOUTUBE_COOKIES_EXPIRED)
        eff_referer = strat.get("referer") if "referer" in strat else custom_referer
        opts = (
            _base_ydl_opts(
                use_cookies=use_c,
                player_clients=strat["clients"],
                is_youtube=is_yt,
                url=url,
                impersonate_chrome=strat.get("impersonate", False),
                custom_referer=eff_referer,
            )
            | extra_opts
        )
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = _enrich_manifest_metadata(url, ydl.extract_info(url, download=download))
        except DownloadError as e:
            last_err = e
            msg = str(e)
            if download and extra_opts.get("outtmpl"):
                _purge_partial_frag_files(str(extra_opts["outtmpl"]))
            is_last = i == len(strategies) - 1
            if not is_last and (
                _looks_like_cookie_issue(msg)
                or _looks_like_transient_format_issue(msg)
                or "part-Frag" in msg
                or "No such file or directory" in msg
            ):
                logger.warning("attempt %d/%d ব্যর্থ (%s), পরের strategy তে চেষ্টা করা হচ্ছে...", i + 1, len(strategies), e)
                continue
            if best_info is not None:
                return best_info
            break

        if not prefer_best_quality:
            return info

        height = _max_format_height(info)
        if height > best_height:
            best_info, best_height = info, height
        is_last = i == len(strategies) - 1
        if height >= MIN_GOOD_HEIGHT or is_last:
            return best_info
        logger.info(
            "attempt %d/%d এ সর্বোচ্চ %dp পাওয়া গেছে — ভালো quality খুঁজতে পরের client combo চেষ্টা করা হচ্ছে...",
            i + 1, len(strategies), height,
        )

    # নন-ইউটিউব সাইটে (FB, Insta, TikTok, X, বা কাস্টম ওয়েবপেজ) yt-dlp ব্যর্থ হলে ডাইরেক্ট ফলব্যাক চেষ্টা
    if not is_yt:
        fb_info = _fallback_social_extract(url)
        if fb_info:
            if download:
                return _download_from_fallback_info(fb_info, extra_opts)
            return fb_info

    raise last_err or RuntimeError("yt-dlp ব্যর্থ হয়েছে")


def _extract_info_sync(url: str) -> dict:
    return _run_ydl(url, {"skip_download": True}, download=False, prefer_best_quality=True) or {}


def _download_sync(url: str, ydl_opts: dict) -> Optional[str]:
    """ডাউনলোড করে, সফল হলে ভিডিওর title রিটার্ন করে (ক্যাপশনে ব্যবহারের জন্য)।"""
    info = _run_ydl(url, ydl_opts, download=True)
    return (info or {}).get("title")


def _format_opts_for(mode: str, quality: str) -> dict:
    """mode: 'audio' | 'novoice' | 'video' এর জন্য yt-dlp format অপশন তৈরি করে।"""
    if mode == "audio":
        return {
            "format": "bestaudio/best",
            "postprocessors": [
                {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}
            ],
        }
    if mode == "novoice":
        # আলাদা video-only ট্র্যাক না থাকলেও (যেমন muxed HLS .m3u8) -an দিয়ে অডিও মুছে ফেলা নিশ্চিত করা
        return {
            "format": "bestvideo/best",
            "merge_output_format": "mp4",
            "postprocessor_args": {"default": ["-loglevel", "error", "-nostats", "-an"]},
        }

    # mode == "video" (অডিওসহ)
    if quality == "best":
        return {"format": "bestvideo+bestaudio/bestvideo/bestaudio/best", "merge_output_format": "mp4"}
    return {
        "format": f"bestvideo[height<={quality}]+bestaudio/best[height<={quality}]/bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
    }


# ---------------------------------------------------------------------------
# ffmpeg/ffprobe হেল্পার — memory-safe: stdout বাতিল, verbose stderr বন্ধ
# ---------------------------------------------------------------------------
FFMPEG_QUIET_FLAGS = ["-hide_banner", "-loglevel", "error", "-nostats", "-nostdin"]


def _run_ffmpeg(args: list[str], timeout: int = FFMPEG_TIMEOUT_SECONDS) -> None:
    """
    ffmpeg চালায় stdout বাতিল করে আর stderr কে quiet flag দিয়ে সীমিত রেখে —
    এর ফলে দীর্ঘ ট্রান্সকোডেও progress-stats জমে RAM বাড়ে না।
    """
    cmd = ["ffmpeg", "-y", *FFMPEG_QUIET_FLAGS, *args]
    result = subprocess.run(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg ব্যর্থ হয়েছে: {result.stderr[-500:]}")


def _ffprobe_duration_seconds(path: Path) -> float:
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    try:
        return float(result.stdout.strip())
    except ValueError:
        raise RuntimeError(f"duration বের করা যায়নি: {result.stderr.strip()}")


def _ffprobe_video_meta(path: Path) -> tuple[int, int, int]:
    """ভিডিওর (width, height, duration_sec) বের করে যাতে Telethon আপলোডে সঠিক অ্যাসপেক্ট রেশিও ও স্ট্রিমিং থাকে।"""
    if not FFPROBE_AVAILABLE:
        return 1280, 720, 0
    try:
        res = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height:format=duration",
                "-of", "json",
                str(path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=15,
        )
        data = json.loads(res.stdout or "{}")
        streams = data.get("streams") or []
        w = int((streams[0].get("width") if streams else 0) or 1280)
        h = int((streams[0].get("height") if streams else 0) or 720)
        dur = int(float(((data.get("format") or {}).get("duration")) or 0))
        return w, h, dur
    except Exception:
        return 1280, 720, 0


def _extract_video_thumb_sync(video_path: Path, seek_sec: float = 2.0) -> Optional[Path]:
    """আপলোডের আগে ভিডিও থেকে হালকা 320px JPEG প্রিভিউ থাম্বনেইল তৈরি করে।"""
    if not FFMPEG_AVAILABLE:
        return None
    thumb_path = video_path.with_name(f"{video_path.stem}_thumb.jpg")
    for ss in (f"{seek_sec:.1f}", "0.1"):
        try:
            _run_ffmpeg(
                ["-threads", "1", "-ss", ss, "-i", str(video_path), "-frames:v", "1", "-vf", "scale=320:-2", "-q:v", "5", str(thumb_path)],
                timeout=15,
            )
            if thumb_path.exists() and thumb_path.stat().st_size > 0:
                return thumb_path
        except Exception:
            continue
    thumb_path.unlink(missing_ok=True)
    return None


def _split_video_sync(src: Path, target_bytes: int) -> list[Path]:
    """
    src কে সমান দৈর্ঘ্যের কয়েকটা পার্টে কাটে, প্রতিটা আনুমানিক target_bytes এর মধ্যে।
    `-c copy` (stream copy) ব্যবহার করা হয় বলে re-encode লাগে না — তাই এটা দ্রুত এবং
    কোনো কোয়ালিটি হারায় না। যত বড় ফাইল হোক, প্রয়োজন অনুযায়ী যত পার্ট লাগে তত হবে।
    """
    duration = _ffprobe_duration_seconds(src)
    size = src.stat().st_size
    if duration <= 0 or size <= 0:
        raise RuntimeError("ভিডিওর duration/size বের করা যায়নি, স্প্লিট করা সম্ভব হয়নি।")

    num_parts = max(math.ceil(size / target_bytes), 1)
    part_duration = duration / num_parts

    parts: list[Path] = []
    for i in range(num_parts):
        start = i * part_duration
        out = src.with_name(f"{src.stem}_part{i + 1}.mp4")
        try:
            _run_ffmpeg(
                [
                    "-ss", f"{start:.2f}", "-i", str(src),
                    "-t", f"{part_duration:.2f}",
                    "-c", "copy", "-avoid_negative_ts", "make_zero",
                    str(out),
                ]
            )
        except Exception:
            for p in parts:  # আংশিক তৈরি হওয়া পার্টগুলো পরিষ্কার করে fallback এ যাওয়া
                p.unlink(missing_ok=True)
            out.unlink(missing_ok=True)
            raise
        if not out.exists() or out.stat().st_size == 0:
            for p in parts:
                p.unlink(missing_ok=True)
            out.unlink(missing_ok=True)
            raise RuntimeError("ffmpeg স্প্লিট আউটপুট ফাঁকা।")
        parts.append(out)

    src.unlink(missing_ok=True)
    return parts


def _compress_video_sync(src: Path, target_bytes: int) -> Path:
    """
    src কে re-encode করে target_bytes এর কাছাকাছি সাইজে আনার চেষ্টা করে (bitrate কমিয়ে)।
    স্প্লিট ব্যর্থ হলে শেষ উপায় হিসেবে ব্যবহৃত হয় — কোয়ালিটি কিছুটা কমে যায়।
    """
    duration = _ffprobe_duration_seconds(src)
    if duration <= 0:
        raise RuntimeError("ভিডিওর duration বের করা যায়নি, তাই কমপ্রেস করা সম্ভব হয়নি।")

    out = src.with_name(f"{src.stem}_compressed.mp4")
    audio_kbps = 128
    shrink = 1.0  # প্রতি ব্যর্থ চেষ্টায় আরেকটু টাইট টার্গেট ধরা হয়

    for attempt in range(1, COMPRESS_MAX_ATTEMPTS + 1):
        this_target = int(target_bytes * shrink)
        total_kbps = (this_target * 8 / 1000) / duration
        video_kbps = max(int(total_kbps - audio_kbps), 150)  # খুব বেশি নিচে না নামানোর জন্য floor

        logger.info(
            "compress attempt %d/%d: target=%dMB video_kbps=%d",
            attempt, COMPRESS_MAX_ATTEMPTS, this_target // (1024 * 1024), video_kbps,
        )
        _run_ffmpeg(
            [
                "-i", str(src),
                "-c:v", "libx264", "-preset", "veryfast",
                "-b:v", f"{video_kbps}k", "-maxrate", f"{video_kbps}k", "-bufsize", f"{video_kbps * 2}k",
                "-c:a", "aac", "-b:a", f"{audio_kbps}k",
                "-movflags", "+faststart",
                str(out),
            ]
        )

        if out.stat().st_size <= target_bytes or attempt == COMPRESS_MAX_ATTEMPTS:
            src.unlink(missing_ok=True)
            return out

        shrink *= 0.85  # আরেকটু কম bitrate নিয়ে পরের চেষ্টা

    return out  # কখনো এখানে পৌঁছাবে না, শুধু টাইপ-চেকারের জন্য


# ---------------------------------------------------------------------------
# ডিস্ক/মেমোরি হাইজিন
# ---------------------------------------------------------------------------
def _free_disk_mb(path: Path = DOWNLOAD_DIR) -> float:
    return shutil.disk_usage(path).free / (1024 * 1024)


QUEUE_STATE_FILE = DOWNLOAD_DIR / "_queue_state.json"
PROTECTED_FILE_IDS: set[str] = set()


def _preload_protected_file_ids_from_disk() -> None:
    raw_sources: list[str] = []
    if QUEUE_STATE_FILE.exists():
        with contextlib.suppress(Exception):
            raw_sources.append(QUEUE_STATE_FILE.read_text(encoding="utf-8"))
    env_q = os.environ.get("PERSISTED_QUEUE", "").strip()
    if env_q:
        raw_sources.append(env_q)
    for raw in raw_sources:
        with contextlib.suppress(Exception):
            arr = json.loads(raw)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict) and item.get("file_id"):
                        PROTECTED_FILE_IDS.add(str(item["file_id"]))


def _cleanup_stale_files(max_age_seconds: int = 0) -> int:
    """
    max_age_seconds=0 দিলে পুরনো ফাইল মোছে — তবে ACTIVE_FILE_IDS, PROTECTED_FILE_IDS,
    _queue_state.json এবং _links_cache.json কখনোই মোছা হয় না, যাতে রিজিউমযোগ্য ডাউনলোড ও বাটন ক্যাশ অক্ষত থাকে।
    """
    now = time.time()
    removed = 0
    keep_prefixes = ACTIVE_FILE_IDS | PROTECTED_FILE_IDS
    for f in DOWNLOAD_DIR.glob("*"):
        if f.name.startswith(("_queue_state", "_links_cache")):
            continue
        if any(f.name.startswith(fid) for fid in keep_prefixes):
            continue
        try:
            if now - f.stat().st_mtime >= max_age_seconds:
                if f.is_dir():
                    shutil.rmtree(f, ignore_errors=True)
                else:
                    f.unlink(missing_ok=True)
                removed += 1
        except OSError:
            pass
    return removed


def _current_rss_mb() -> float:
    try:
        statm = Path("/proc/self/statm")
        if statm.exists():
            parts = statm.read_text(encoding="ascii").split()
            if len(parts) >= 2:
                page_sz = os.sysconf("SC_PAGE_SIZE")
                return (int(parts[1]) * page_sz) / (1024 * 1024)
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        return -1.0


async def _ensure_disk_space(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """যথেষ্ট ডিস্ক স্পেস না থাকলে আগে পুরনো ফাইল ক্লিনআপ করে, তাতেও না হলে False রিটার্ন করে।"""
    if _free_disk_mb() >= MIN_FREE_DISK_MB:
        return True

    logger.warning("ডিস্ক স্পেস কম, ইমার্জেন্সি ক্লিনআপ চালানো হচ্ছে...")
    _cleanup_stale_files(max_age_seconds=0)
    gc.collect()

    if _free_disk_mb() >= MIN_FREE_DISK_MB:
        return True

    await context.bot.send_message(
        chat_id,
        "সার্ভারে ডিস্ক স্পেস কম থাকায় এই মুহূর্তে ডাউনলোড করা যাচ্ছে না। একটু পরে আবার চেষ্টা করুন।",
    )
    return False


def _url_reachable(url: str, timeout: float = 3.0, retries: int = 5, delay: float = 2.0) -> bool:
    """কোনো লোকাল/এক্সটার্নাল সার্ভিস সত্যিই চালু ও রেসপন্স দিচ্ছে কিনা যাচাই করে, কয়েকবার রিট্রাই সহ।"""
    for attempt in range(retries):
        try:
            requests.get(url, timeout=timeout)
            return True
        except Exception:
            if attempt < retries - 1:
                time.sleep(delay)
    return False


def _local_api_reachable(timeout: float = 3.0, retries: int = 5, delay: float = 2.0) -> bool:
    """
    Local Bot API সার্ভার সত্যিই চালু ও রেসপন্স দিচ্ছে কিনা যাচাই করে (start.sh এর
    নিজস্ব wait-loop এর পরও, বাড়তি সেফটি হিসেবে)। কয়েকবার রিট্রাই করে, কারণ সার্ভার
    ধীরে চালু হতে পারে।
    """
    return _url_reachable(LOCAL_BOT_API_URL, timeout, retries, delay)


def _check_cookies_file() -> None:
    """
    COOKIES_FILE (যেটা Render এর Secret File হলে read-only) কে একটা writable
    রানটাইম কপিতে নিয়ে ACTIVE_COOKIES_FILE সেট করে — yt-dlp সেশন-আপডেট লিখতে
    চাইলেও যাতে crash না করে। এই কপি DOWNLOAD_DIR এর বাইরে রাখা হয়, যাতে
    maintenance job এর stale-file sweep এটা ভুলবশত ডিলিট না করে দেয়।
    """
    global ACTIVE_COOKIES_FILE

    if not COOKIES_FILE:
        logger.info("COOKIES_FILE সেট করা নেই — কুকিজ ছাড়াই চলবে (পাবলিক ভিডিওতে যথেষ্ট)।")
        return

    src = Path(COOKIES_FILE)
    if not src.exists():
        logger.warning("COOKIES_FILE (%s) পাওয়া যায়নি — কুকিজ ছাড়াই চলবে।", COOKIES_FILE)
        return

    try:
        shutil.copyfile(src, COOKIES_RUNTIME_PATH)
        ACTIVE_COOKIES_FILE = str(COOKIES_RUNTIME_PATH)
        logger.info(
            "কুকিজ ফাইল লোড হয়েছে (%s থেকে writable কপি তৈরি হয়েছে, %d বাইট)",
            COOKIES_FILE, COOKIES_RUNTIME_PATH.stat().st_size,
        )
    except OSError as e:
        logger.warning("কুকিজ ফাইলের writable কপি তৈরি করা যায়নি (%s) — কুকিজ ছাড়াই চলবে।", e)
        ACTIVE_COOKIES_FILE = None


async def _maintenance_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """প্রতি MAINTENANCE_INTERVAL_SECONDS পরপর চলে — cache/leftover ক্লিনআপ, Aylo টোকেন কিপ-অ্যালাইভ + মেমোরি লগ।"""
    now_t = time.time()
    for cid, f_until in list(CHAT_FLOOD_UNTIL.items()):
        if f_until <= now_t:
            CHAT_FLOOD_UNTIL.pop(cid, None)
    for fjid, fjob in list(FAILED_RESUMABLE_JOBS.items()):
        if (now_t - getattr(fjob, "created_at", now_t)) > 7200:
            FAILED_RESUMABLE_JOBS.pop(fjid, None)
            PROTECTED_FILE_IDS.discard(fjob.file_id)
            for leftover in DOWNLOAD_DIR.glob(f"{fjob.file_id}*"):
                with contextlib.suppress(OSError):
                    if leftover.is_dir():
                        shutil.rmtree(leftover, ignore_errors=True)
                    else:
                        leftover.unlink(missing_ok=True)
    purged_links = LINKS.purge_expired()
    BATCHES.purge_expired()
    _purge_expired_extract_info()
    removed_files = _cleanup_stale_files(max_age_seconds=STALE_FILE_AGE_SECONDS)
    with contextlib.suppress(Exception):
        await asyncio.get_running_loop().run_in_executor(None, _try_renew_aylo_tokens, False)
    _trim_process_memory()

    if purged_links or removed_files:
        logger.info("maintenance: %d expired link(s), %d stale file(s) মুছে ফেলা হয়েছে", purged_links, removed_files)
    logger.info("maintenance: RSS ~%.1f MB, free disk ~%.0f MB", _current_rss_mb(), _free_disk_mb())


# ---------------------------------------------------------------------------
# টেলিগ্রাম হ্যান্ডলার
# ---------------------------------------------------------------------------
WELCOME_TEXT = (
    "স্বাগতম! 👋\n\n"
    "Facebook, Instagram, TikTok, YouTube সহ ১৮০০+ প্ল্যাটফর্মের একটি বা একসাথে একাধিক (Bulk) "
    "ভিডিও লিংক এখানে পাঠান — আমি একের পর এক (সর্বোচ্চ ৫ স্লট Queue) সুপার-ফাস্ট মাল্টি-থ্রেড স্পিডে "
    "ডাউনলোড ও আপলোড করে দেব, এবং প্রতিটির সাথেই আলাদা ❌ Cancel বাটন থাকবে!\n\n"
    "ফিচার তালিকা দেখতে /help লিখুন।"
)

HELP_TEXT = (
    "🛠️ <b>ফিচার তালিকা</b>\n\n"
    "🎬 ভিডিও ডাউনলোড — quality সিলেক্ট করে (144p থেকে Best পর্যন্ত)\n"
    "📦 ৫-স্লট ওভারলোড-প্রুফ কিউ — একসাথে সর্বোচ্চ ৫টি টাস্ক (১টি রানিং + ৪টি ওয়েটিং) ১-বাই-১ সুপার-ফাস্ট প্রসেসিং\n"
    "♻️ স্মার্ট অটো-রিজিউম — নেটওয়ার্ক বা ফ্লাড-ওয়েটে আপলোড আটকে গেলে ঠিক সেই চাঙ্ক থেকেই স্বয়ংক্রিয় রিজিউম (+ /resume কমান্ড)\n"
    "❌ ইনস্ট্যান্ট ক্যানসেল — প্রতিটি কিউ ও চলমান ভিডিওর সাথে আলাদা Cancel বাটন (+ /cancelall কমান্ড)\n"
    "🎵 অডিও-only ডাউনলোড (MP3, 192kbps)\n"
    "🔇 সাউন্ড ছাড়া ভিডিও\n"
    "🖼️ থাম্বনেইল ডাউনলোড — সর্বোচ্চ কোয়ালিটিতে\n"
    "📊 রিয়েল-টাইম স্মুথ অ্যানিমেশন — সাব-ব্লক প্রোগ্রেস বার + লাইভ স্পিড + বাকি সময়\n"
    "🍪 কুকিজ সাপোর্ট — লগইন/এজ-রেস্ট্রিক্টেড কনটেন্ট (/cookies)\n"
    "⚡ ২৮-থ্রেড লাইভ-পাইপ HLS + ২৪x Range ডাউনলোড ও ৩২x৫ FastTelethon v6 আপলোড (uvloop + cryptg)\n"
    "📦 বড় ফাইল (~২GB পর্যন্ত সিঙ্গেল ফাইল, এর বড় হলে লসলেস মাল্টি-পার্ট)\n\n"
    "ব্যবহার: একটি বা একসাথে একাধিক ভিডিও লিংক পাঠান, বাকিটা বাটনে ক্লিক করেই হয়ে যাবে।"
)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(WELCOME_TEXT)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_html(HELP_TEXT)


def _clean_url_params(url: str, keep_params: Optional[set[str]] = None) -> str:
    """
    URL থেকে অপ্রয়োজনীয় ট্র্যাকিং কুয়েরি প্যারামিটারগুলো (যেমন ?rdid=, ?share_url=, ?igsh=, ?fbclid=, ?utm_*) বাদ দেয়।
    """
    try:
        parsed = urlparse(url)
        if not parsed.query:
            return url
        query_dict = parse_qs(parsed.query, keep_blank_values=True)
        TRACKING_KEYS = {
            "rdid", "share_url", "mibextid", "fbclid", "ref", "refsrc", "fs", "s",
            "wtsid", "__tn__", "epa", "igsh", "utm_source", "utm_medium", "utm_campaign",
            "utm_term", "utm_content", "si", "feature", "app",
        }
        filtered = {}
        for k, v in query_dict.items():
            k_lower = k.lower()
            if keep_params and k_lower in keep_params:
                filtered[k] = v
            elif k_lower in TRACKING_KEYS or k_lower.startswith("utm_"):
                continue
            else:
                filtered[k] = v

        new_query = urlencode(filtered, doseq=True)
        return urlunparse(parsed._replace(query=new_query))
    except Exception:
        return url


def _resolve_share_url(url: str) -> str:
    """
    Facebook, Instagram, TikTok, YouTube, শর্টনার ইত্যাদি সব প্ল্যাটফর্মের
    শেয়ার/রিডাইরেক্ট লিংককে আসল ক্যানোনিকাল লিংকে কনভার্ট করে এবং ট্র্যাকিং প্যারামিটার বাদ দেয়।
    """
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()

        # ১. Facebook Share / Reel / Watch
        if "facebook.com" in host or "fb.watch" in host:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Sec-Fetch-Mode": "navigate",
            }
            resp = requests.get(url, allow_redirects=False, timeout=10, headers=headers, verify=True)
            if resp.status_code in (301, 302, 303, 307, 308) and "Location" in resp.headers:
                resolved = resp.headers["Location"]
                if resolved.startswith("/"):
                    resolved = f"https://www.facebook.com{resolved}"
                logger.info("Resolved Facebook share URL %s -> %s", url, resolved)
                # Reel এর জন্য কোনো query parameter দরকার নেই — পুরোপুরি ক্লিন করা
                if "/reel/" in resolved:
                    return urlunparse(urlparse(resolved)._replace(query=""))
                return _clean_url_params(resolved, keep_params={"v"})
            if "/reel/" in url:
                return urlunparse(urlparse(url)._replace(query=""))
            return _clean_url_params(url, keep_params={"v"})

        # ২. Instagram Share / Reel / Post
        elif "instagram.com" in host or "instagr.am" in host or "ig.me" in host:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Sec-Fetch-Mode": "navigate",
            }
            if "/share/" in url or "ig.me" in host or "instagr.am" in host:
                resp = requests.get(url, allow_redirects=False, timeout=10, headers=headers, verify=True)
                if resp.status_code in (301, 302, 303, 307, 308) and "Location" in resp.headers:
                    resolved = resp.headers["Location"]
                    if resolved.startswith("/"):
                        resolved = f"https://www.instagram.com{resolved}"
                    logger.info("Resolved Instagram share URL %s -> %s", url, resolved)
                    return urlunparse(urlparse(resolved)._replace(query=""))
            return urlunparse(urlparse(url)._replace(query=""))

        # ৩. TikTok Short Links
        elif "tiktok.com" in host:
            if "vm.tiktok.com" in host or "vt.tiktok.com" in host or "/t/" in url:
                resp = requests.head(url, allow_redirects=True, timeout=10, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
                if resp.url:
                    logger.info("Resolved TikTok short URL %s -> %s", url, resp.url)
                    return _clean_url_params(resp.url)
            return _clean_url_params(url)

        # ৪. YouTube Links
        elif host == "youtu.be" or host.endswith("youtube.com"):
            return _clean_url_params(url, keep_params={"v", "t", "list"})

        # ৫. Twitter / X Links
        elif host in ("x.com", "www.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"):
            return _clean_url_params(url)

        # ৬. অন্যান্য URL shortener (bit.ly, tinyurl, t.co ইত্যাদি)
        elif any(s in host for s in ("bit.ly", "tinyurl.com", "t.co", "cutt.ly", "shorturl")):
            resp = requests.head(url, allow_redirects=True, timeout=10, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
            if resp.url:
                return resp.url

    except Exception as e:
        logger.warning("Failed to resolve share URL %s: %s", url, e)

    # অন্যান্য ডোমেইন বা সরাসরি সাইনড CDN লিংকের (.m3u8/.mp4?validto=...&hash=...&s=...)
    # কুয়েরি স্ট্রিং হুবহু অপরিবর্তিত রাখা হয় যাতে সিকিউরিটি সিগনেচার নষ্ট না হয়।
    return url


def _ip_locked_stream_message(stored_url: str, err_msg: str) -> Optional[str]:
    """IP-bound বা মেয়াদোত্তীর্ণ .m3u8 / CDN টোকেন লিংক শনাক্ত করে স্পষ্ট সমাধান দেখায়।"""
    u, _ = _unpack_stored_url(stored_url)
    low_err = err_msg.lower()
    qs = parse_qs(urlparse(u).query)

    # টোকেনের মেয়াদ (validto / expires / exp) চেক
    for exp_key in ("validto", "expires", "exp", "e"):
        if exp_key in qs:
            try:
                exp_ts = int(qs[exp_key][0])
                if 1_600_000_000 < exp_ts < int(time.time()):
                    return (
                        "❌ <b>স্ট্রিম লিংকটির মেয়াদ শেষ হয়ে গেছে (Token Expired)!</b>\n\n"
                        "⏳ এই <code>.m3u8</code> লিংকটির সিকিউরিটি টোকেনের সময়সীমা পেরিয়ে গেছে।\n"
                        "💡 ব্রাউজারের <b>আসল ওয়েবপেজের লিংকটি (Page URL)</b> সরাসরি বটে পাঠান, অথবা পেজ রিফ্রেশ করে নতুন লিংক দিন।"
                    )
            except ValueError:
                pass

    # IP-locked CDN টোকেন চেক (যেমন ?ip=51.158.254.160&hash=... + HTTP 470/474/403)
    bound_ip = (qs.get("ip") or [None])[0]
    if bound_ip and any(code in low_err for code in ("470", "474", "403", "401", "forbidden", "client error")):
        return (
            "❌ <b>এই <code>.m3u8</code> লিংকটি আপনার ব্রাউজার/ভিপিএন আইপিতে লক করা!</b>\n\n"
            f"🔒 লিংকটির ভেতরে <code>ip={html.escape(bound_ip)}</code> এবং সিকিউরিটি <code>hash</code> দেওয়া আছে—"
            "অর্থাৎ আপনি ব্রাউজারে যে IP/VPN দিয়ে ভিডিওটি প্লে করেছেন, CDN সার্ভার কেবল সেই IP থেকেই এই <code>.m3u8</code> লিংকটি চলতে দেবে "
            "(বটের সার্ভার আইপি ভিন্ন হওয়ায় CDN <code>HTTP 470</code> ব্লক দিয়েছে)।\n\n"
            "✅ <b>সমাধান (যেভাবে বট দিয়েই ডাউনলোড করবেন):</b>\n"
            "১. DevTools-এর <code>master.m3u8</code> লিংক না দিয়ে ব্রাউজারের অ্যাড্রেস বারের <b>মূল ওয়েবপেজের লিংকটি (Page URL)</b> সরাসরি বটে পাঠান—বট নিজের সার্ভার আইপি দিয়েই নতুন <code>.m3u8</code> তৈরি করে ডাউনলোড করে নেবে।\n"
            "২. সাইটটি প্রাইভেট/মেম্বারশিপ সাইট হলে ওই সাইটে লগইন থাকা অবস্থায় Cookie-Editor থেকে <code>cookies.txt</code> এক্সপোর্ট করে বটে পাঠান, তারপর ওয়েবপেজের লিংক দিন!"
        )
    return None


async def _safe_status_edit(status_msg, text: str, parse_mode: Optional[str] = "HTML") -> None:
    try:
        await status_msg.edit_text(text, parse_mode=parse_mode)
    except RetryAfter as ra:
        sec = _extract_flood_seconds(ra) or 5
        _note_chat_flood(status_msg.chat_id, sec)
        logger.warning("Status edit hit RetryAfter (%ds) — চ্যাট ফ্লাড কুলডাউন নোট করা হলো।", sec)
    except TelegramError as te:
        if "not modified" not in str(te).lower():
            logger.warning("Status edit warning: %s", te)


async def _apply_incoming_cookie_text(raw_cookie_text: str, status_msg) -> None:
    """নতুন Netscape কুকিজ টেক্সট আগের সব ডোমেইনের কুকিজের সাথে মার্জ করে সেভ ও যাচাই করে।"""
    global YOUTUBE_COOKIES_EXPIRED
    parsed_new = _parse_netscape_cookies(raw_cookie_text)
    if not parsed_new:
        await _safe_status_edit(
            status_msg,
            "⚠️ কোনো বৈধ কুকিজ লাইন পাওয়া যায়নি! অনুগ্রহ করে Cookie-Editor → Export → Netscape ফরম্যাটে এক্সপোর্ট করে পাঠান।",
            parse_mode=None,
        )
        return

    existing_raw = ""
    if COOKIES_RUNTIME_PATH.exists():
        with contextlib.suppress(Exception):
            existing_raw = COOKIES_RUNTIME_PATH.read_text(encoding="utf-8", errors="ignore")

    merged = _merge_netscape_cookies(existing_raw, raw_cookie_text)
    if "youtube.com" in raw_cookie_text.lower() or "google.com" in raw_cookie_text.lower():
        YOUTUBE_COOKIES_EXPIRED = False

    loop = asyncio.get_running_loop()
    render_synced = await loop.run_in_executor(None, _save_and_sync_cookies, merged, True)

    incoming_domains = {
        _host_base_domain(c["domain"]) for c in parsed_new if c.get("domain")
    }

    # Aylo মেম্বার সাইটের কুকিজ হলে সাথে সাথে সার্ভার থেকে লাইভ যাচাই (টোকেন রিনিউ) করা
    live_check_note = ""
    has_aylo_rt = any(
        c["name"] == "refresh_token_ma" and c.get("value") for c in parsed_new
    )
    if has_aylo_rt:
        renewed_ok = False
        with contextlib.suppress(Exception):
            renewed_ok = await loop.run_in_executor(None, _try_renew_aylo_tokens, True)
        live_check_note = (
            "🔐 <b>লাইভ যাচাই:</b> ✅ সার্ভার থেকে টোকেন সফলভাবে রিনিউ হয়েছে — সেশন সচল!\n"
            if renewed_ok
            else "🔐 <b>লাইভ যাচাই:</b> ⚠️ সার্ভার এই refresh_token গ্রহণ করেনি — পেজ রিফ্রেশ করে সাথে সাথে আবার কুকিজ পাঠান।\n"
        )
    else:
        with contextlib.suppress(Exception):
            await loop.run_in_executor(None, _try_renew_aylo_tokens, False)
    if COOKIES_RUNTIME_PATH.exists():
        with contextlib.suppress(Exception):
            merged = COOKIES_RUNTIME_PATH.read_text(encoding="utf-8", errors="ignore")

    raw_all_doms = {_host_base_domain(c["domain"]) for c in _parse_netscape_cookies(merged) if c.get("domain")}
    norm_incoming = sorted({_normalize_Logical_domain(d, raw_all_doms) for d in incoming_domains if d})

    msg = (
        f"✅ <b>কুকিজ সফলভাবে সেট হয়েছে! ({len(parsed_new)}টি কুকি)</b>\n"
        f"🎯 <b>সেটকৃত ডোমেইন:</b> <code>{html.escape(', '.join(norm_incoming) or 'none')}</code>\n"
    )
    if render_synced:
        cloud_lbl = "Render Pro" if (RENDER_SERVICE_ID or os.environ.get("RENDER_SERVICE_ID")) else "ক্লাউড"
        msg += f"☁️ {cloud_lbl} সার্ভার এনভায়রনমেন্টেও স্থায়ীভাবে সেভ হয়েছে।\n"
    else:
        msg += "💡 লোকাল রানটাইমে সক্রিয় করা হয়েছে।\n"

    msg += live_check_note

    # শুধুমাত্র যে ডোমেইনের কুকিজ মাত্র পাঠানো হয়েছে সেটিরই মেয়াদ ও স্ট্যাটাস দেখানো (অন্য ডোমেইনের কোনো উল্লেখ নয়)
    uploaded_status = _per_domain_cookie_status_lines(merged, target_domains=set(norm_incoming))
    if uploaded_status:
        msg += "\n📊 <b>মেয়াদ ও স্ট্যাটাস:</b>\n" + "\n".join(uploaded_status)

    if "youtube.com" in raw_cookie_text.lower() or "google.com" in raw_cookie_text.lower():
        await _safe_status_edit(status_msg, msg + "\n🔍 YouTube কুকিজ যাচাই করা হচ্ছে... ⏳", parse_mode="HTML")
        try:
            test_info = await loop.run_in_executor(
                None, _extract_info_sync, "https://www.youtube.com/watch?v=YEgaXkTqbIM"
            )
            h = _max_format_height(test_info)
            msg += f"\n🎉 <b>YouTube যাচাই সফল!</b> ({h}p ফরম্যাট আনলকড)"
        except Exception as yt_err:
            msg += f"\n⚠️ YouTube টেস্ট বার্তা: <code>{html.escape(str(yt_err)[:180])}</code>"

    await _safe_status_edit(status_msg, msg, parse_mode="HTML")


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg_text = update.message.text or ""
    user = update.effective_user
    chat_id = update.effective_chat.id
    input_msg_id: Optional[int] = update.message.message_id

    # ওনার সরাসরি চ্যাটে Netscape কুকিজ টেক্সট পেস্ট করলে কুকিজ সেভ করে ইউজারের কুকি টেক্সট মেসেজটি সাথে সাথে ডিলিট করে দেওয়া!
    if user and (not OWNER_ID or str(user.id) == str(OWNER_ID)) and _is_netscape_cookie_text(msg_text):
        cookie_status = None
        with contextlib.suppress(TelegramError):
            cookie_status = await context.bot.send_message(chat_id, "পেস্ট করা কুকিজ প্রসেস ও যাচাই করা হচ্ছে... ⏳")
        try:
            if cookie_status is not None:
                await _apply_incoming_cookie_text(msg_text, cookie_status)
            else:
                merged_only = _merge_netscape_cookies(
                    COOKIES_RUNTIME_PATH.read_text(encoding="utf-8", errors="ignore") if COOKIES_RUNTIME_PATH.exists() else "",
                    msg_text,
                )
                await asyncio.get_running_loop().run_in_executor(None, _save_and_sync_cookies, merged_only, True)
        except Exception as e:
            logger.exception("Pasted cookies save error: %s", e)
            if cookie_status is not None:
                await _safe_status_edit(cookie_status, f"❌ কুকিজ সংরক্ষণে ত্রুটি: {e}", parse_mode=None)

        # কমেন্ট, ট্যাব-যুক্ত ও স্পেস-যুক্ত কুকিজ লাইন বাদ দিয়ে আসল ভিডিও লিংক আছে কিনা দেখা
        non_cookie_lines = []
        for ln in msg_text.splitlines():
            s = _clean_cookie_markdown(ln.strip())
            if not s or s.startswith("#") or "\tTRUE\t" in s or "\tFALSE\t" in s or NETSCAPE_SPACE_LINE_RE.match(s):
                continue
            non_cookie_lines.append(s)
        urls = [
            u for u in URL_REGEX.findall("\n".join(non_cookie_lines))
            if "curl.haxx.se" not in u.lower()
        ]
        # শুধুমাত্র খাঁটি কুকিজ পেস্ট হলে ইউজারের কুকি টেক্সট মেসেজটি ডিলিট করা (যাতে ভিডিও লিংক মেসেজ কখনো না মোছে)
        if not urls:
            with contextlib.suppress(TelegramError):
                await update.message.delete()
            return
    else:
        urls = URL_REGEX.findall(msg_text)

    if not urls:
        await context.bot.send_message(
            chat_id,
            "দয়া করে একটি সঠিক ভিডিও লিংক পাঠান। (ফিচার তালিকা: /help)",
            reply_parameters=_reply_params(input_msg_id),
        )
        return

    cleaned_urls = list(dict.fromkeys(u.rstrip(",;)") for u in urls if u.rstrip(",;)")))

    # বিশেষ কেস: মাত্র ২টি লিংক যার একটি raw .m3u8/.mpd ম্যানিফেস্ট এবং অপরটি তার কাস্টম প্যারেন্ট ওয়েবপেজ
    is_manifest_referer_pair = False
    raw_url = cleaned_urls[0]
    referer_url: Optional[str] = None
    if len(cleaned_urls) == 2:
        u1, u2 = cleaned_urls[0], cleaned_urls[1]
        u1_man = any(ext in u1.lower() for ext in (".m3u8", ".mpd"))
        u2_man = any(ext in u2.lower() for ext in (".m3u8", ".mpd"))
        social_domains = ("youtube.com", "youtu.be", "facebook.com", "fb.watch", "instagram.com", "tiktok.com", "x.com", "twitter.com", "site-ma.")
        if u2_man and not u1_man and not any(d in u1.lower() for d in social_domains) and ".mp4" not in u1.lower():
            raw_url, referer_url = u2, u1
            is_manifest_referer_pair = True
        elif u1_man and not u2_man and not any(d in u2.lower() for d in social_domains) and ".mp4" not in u2.lower():
            raw_url, referer_url = u1, u2
            is_manifest_referer_pair = True

    # এক মেসেজে একাধিক (Bulk) ভিডিও লিংক দিলে বাল্ক কিউ অপশন দেখানো (ওই ইনপুট মেসেজের নিচে রিপ্লাই হিসেবে)
    if len(cleaned_urls) >= 2 and not is_manifest_referer_pair:
        batch_id = BATCHES.put(cleaned_urls, source_message_id=input_msg_id)
        count = len(cleaned_urls)
        bulk_kb = [
            [InlineKeyboardButton(f"⚡ সব ডাউনলোড শুরু করুন ({count}টি — Best HD)", callback_data=f"bulk|video|best|{batch_id}")],
            [
                InlineKeyboardButton("🎬 সব 1080p", callback_data=f"bulk|video|1080|{batch_id}"),
                InlineKeyboardButton("🎬 সব 720p", callback_data=f"bulk|video|720|{batch_id}"),
                InlineKeyboardButton("🎬 সব 480p", callback_data=f"bulk|video|480|{batch_id}"),
            ],
            [
                InlineKeyboardButton("🎵 সব অডিও (MP3)", callback_data=f"bulk|audio|best|{batch_id}"),
                InlineKeyboardButton("🔇 সব সাউন্ড ছাড়া", callback_data=f"bulk|novoice|best|{batch_id}"),
            ],
            [InlineKeyboardButton("📋 প্রতিটি লিংকের আলাদা মেনু দেখান", callback_data=f"bulksplit|{batch_id}")],
            [InlineKeyboardButton("❌ বাতিল করুন (Cancel)", callback_data="cancelmenu")],
        ]
        await context.bot.send_message(
            chat_id,
            f"📦 <b>মোট {count}টি ভিডিও লিংক পাওয়া গেছে!</b>\n"
            f"একের পর এক (সর্বোচ্চ {MAX_QUEUE_SLOTS} স্লট কিউ) সর্বোচ্চ স্পিডে ডাউনলোড করতে ফরম্যাট বেছে নিন 👇\n"
            f"<i>(আগে ডাউনলোড হওয়া লিংক থাকলে আলাদা Re-Download / Cancel দেখাবে)</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(bulk_kb),
            reply_parameters=_reply_params(input_msg_id),
        )
        return

    await _send_single_link_menu(
        chat_id,
        context,
        raw_url,
        referer_url=referer_url,
        source_message_id=input_msg_id,
    )


def _single_download_menu_kb(short_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⚡ দ্রুত ডাউনলোড (Best Quality)", callback_data=f"dl|video|best|{short_id}")],
            [InlineKeyboardButton("🎬 রেজুলেশন পছন্দ করুন (Quality)", callback_data=f"menu|{short_id}")],
            [InlineKeyboardButton("🎵 শুধু অডিও (MP3)", callback_data=f"dl|audio|best|{short_id}")],
            [InlineKeyboardButton("🔇 সাউন্ড ছাড়া ভিডিও", callback_data=f"dl|novoice|best|{short_id}")],
            [InlineKeyboardButton("🖼️ থাম্বনেইল ডাউনলোড", callback_data=f"thumb|{short_id}")],
            [InlineKeyboardButton("❌ বাতিল করুন (Cancel)", callback_data="cancelmenu")],
        ]
    )


async def _send_single_link_menu(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    raw_url: str,
    referer_url: Optional[str] = None,
    source_message_id: Optional[int] = None,
    prefix_label: str = "",
    skip_duplicate_check: bool = False,
) -> None:
    canonical_url = await asyncio.get_running_loop().run_in_executor(None, _resolve_share_url, raw_url)
    stored_value = f"{canonical_url}\n{referer_url}" if referer_url else canonical_url
    short_id = LINKS.put(stored_value, source_message_id=source_message_id)

    # ডুপ্লিকেট লিংক চেক: আগে ডাউনলোড হয়ে থাকলে (বা কিউতে চলমান থাকলে) Re-Download নাকি Cancel তা জিজ্ঞাসা করা
    if not skip_duplicate_check:
        prev = _lookup_previous_download(chat_id, stored_value)
        if prev is not None:
            prev_title = (prev.get("title") or "").strip()
            title_line = f"🎬 <i>{html.escape(prev_title)}</i>\n" if prev_title else ""
            if prev.get("in_queue"):
                dup_text = (
                    f"{html.escape(prefix_label)}⚠️ <b>এই ভিডিওটি বর্তমানে ডাউনলোড কিউতে ইতিমধ্যেই আছে!</b>\n"
                    f"{title_line}"
                    f"🔗 <code>{html.escape(_url_short_preview(stored_value))}</code>\n\n"
                    f"আপনি কি এটি আবার ডাউনলোড করতে চান নাকি বাতিল করবেন? 👇"
                )
            else:
                dup_text = (
                    f"{html.escape(prefix_label)}⚠️ <b>এই ভিডিওটি আগেও ডাউনলোড করা হয়েছে!</b>\n"
                    f"{title_line}"
                    f"🔗 <code>{html.escape(_url_short_preview(stored_value))}</code>\n\n"
                    f"আপনি কি এটি আবার ডাউনলোড করতে চান নাকি বাতিল করবেন? 👇"
                )
            dup_kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔄 আবার ডাউনলোড করুন (Re-Download)", callback_data=f"redl|{short_id}")],
                    [InlineKeyboardButton("❌ বাতিল করুন (Cancel)", callback_data="cancelmenu")],
                ]
            )
            await context.bot.send_message(
                chat_id,
                dup_text,
                parse_mode="HTML",
                reply_markup=dup_kb,
                reply_parameters=_reply_params(source_message_id),
            )
            return

    prompt = f"{prefix_label}কী ধরনের ডাউনলোড চান বেছে নিন 👇"
    await context.bot.send_message(
        chat_id,
        prompt,
        reply_markup=_single_download_menu_kb(short_id),
        reply_parameters=_reply_params(source_message_id),
    )


async def _resolve_link_entry_or_recover(query, short_id: str) -> Optional[_CacheEntry]:
    """
    ক্যাশে short_id থাকলে সাথে সাথে রিটার্ন করে; আর কন্টেইনার রিস্টার্ট বা দীর্ঘ সময় পর ক্যাশে না থাকলেও
    বটের রিপ্লাই করা ইউজারের মূল মেসেজ (reply_to_message) থেকে স্বয়ংক্রিয়ভাবে লিংক উদ্ধার করে সচল রাখে!
    """
    entry = LINKS.get_entry(short_id)
    if entry is not None:
        return entry

    msg = getattr(query, "message", None)
    if msg is None:
        return None
    rep = getattr(msg, "reply_to_message", None)
    rep_mid = getattr(rep, "message_id", None) if rep is not None else None
    rep_txt = (getattr(rep, "text", None) or getattr(rep, "caption", None) or "") if rep is not None else ""
    msg_txt = getattr(msg, "text", None) or ""

    recovered_raw: Optional[str] = None
    if rep_txt:
        rep_urls = _extract_clean_urls_from_text(rep_txt)
        if len(rep_urls) == 1:
            recovered_raw = rep_urls[0]
        elif len(rep_urls) == 2:
            paired = _pair_or_first_url(rep_urls)
            if paired and "\n" in paired:
                recovered_raw = paired
            else:
                m_idx = re.search(r"\[(\d+)/(\d+)\]", msg_txt)
                if m_idx and 1 <= int(m_idx.group(1)) <= len(rep_urls):
                    recovered_raw = rep_urls[int(m_idx.group(1)) - 1]
                else:
                    recovered_raw = rep_urls[0]
        elif len(rep_urls) > 2:
            m_idx = re.search(r"\[(\d+)/(\d+)\]", msg_txt)
            if m_idx and 1 <= int(m_idx.group(1)) <= len(rep_urls):
                recovered_raw = rep_urls[int(m_idx.group(1)) - 1]

    if not recovered_raw and msg_txt:
        self_urls = [u for u in _extract_clean_urls_from_text(msg_txt) if not u.endswith("...")]
        if self_urls:
            recovered_raw = self_urls[0]

    if not recovered_raw:
        return None

    if "\n" in recovered_raw:
        u_part, r_part = recovered_raw.split("\n", 1)
        canon_u = await asyncio.get_running_loop().run_in_executor(None, _resolve_share_url, u_part.strip())
        stored_val = f"{canon_u}\n{r_part.strip()}"
    else:
        stored_val = await asyncio.get_running_loop().run_in_executor(None, _resolve_share_url, recovered_raw)

    LINKS.put(stored_val, source_message_id=rep_mid, short_id=short_id)
    logger.info("♻️ বাটনের লিংক স্বয়ংক্রিয়ভাবে মূল মেসেজ থেকে উদ্ধার হয়েছে (short_id=%s)", short_id)
    return LINKS.get_entry(short_id)


def _resolve_batch_entry_or_recover(query, batch_id: str) -> Optional[_BatchEntry]:
    b_entry = BATCHES.get_entry(batch_id)
    if b_entry is not None:
        return b_entry
    msg = getattr(query, "message", None)
    rep = getattr(msg, "reply_to_message", None) if msg is not None else None
    if rep is None:
        return None
    rep_txt = getattr(rep, "text", None) or getattr(rep, "caption", None) or ""
    rep_urls = _extract_clean_urls_from_text(rep_txt)
    if len(rep_urls) >= 2:
        rep_mid = getattr(rep, "message_id", None)
        BATCHES.put(rep_urls, source_message_id=rep_mid, batch_id=batch_id)
        logger.info("♻️ বাল্ক লিংক তালিকা স্বয়ংক্রিয়ভাবে মূল মেসেজ থেকে উদ্ধার হয়েছে (batch_id=%s, count=%d)", batch_id, len(rep_urls))
        return BATCHES.get_entry(batch_id)
    return None


async def show_quality_menu(update: Update, short_id: str) -> None:
    query = update.callback_query
    entry = await _resolve_link_entry_or_recover(query, short_id)
    if not entry:
        await query.edit_message_text("লিংকের মেয়াদ শেষ হয়ে গেছে, নতুন করে লিংক পাঠান।")
        return
    url = entry.url

    await query.edit_message_text(
        "ফরম্যাট লোড হচ্ছে, একটু অপেক্ষা করুন... ⏳",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ বাতিল করুন (Cancel)", callback_data="cancelmenu")]]),
    )

    try:
        info = await asyncio.get_running_loop().run_in_executor(None, _extract_info_sync, url)
    except Exception as e:
        logger.warning("extract_info failed: %s", e)
        if isinstance(e, AyloAuthError):
            await query.edit_message_text(str(e), parse_mode="HTML")
            return
        msg_str = str(e)
        ip_lock_msg = _ip_locked_stream_message(url, msg_str)
        if ip_lock_msg:
            await query.edit_message_text(ip_lock_msg, parse_mode="HTML")
            return
        if "not made this video available in your country" in msg_str.lower() or "geographic restriction" in msg_str.lower():
            err_text = (
                "❌ <b>ভিডিওটি কান্ট্রি-লকড (Geo-Restricted)!</b>\n\n"
                "🌍 আপলোডার এই ভিডিওটি ক্লাউড সার্ভারের দেশে ব্লক করে রেখেছে।\n"
                "💡 ওনার চ্যাটে <code>/proxy http://user:pass@ip:port</code> কমান্ড দিয়ে এশিয়া/ইউরোপের প্রক্সি যুক্ত করলে এই ভিডিওও ডাউনলোড হবে।"
            )
            await query.edit_message_text(err_text, parse_mode="HTML")
            return
        hint = ""
        if "confirm you" in msg_str.lower() or "not a bot" in msg_str.lower():
            hint = (
                "\n\n💡 <b>ইউটিউব নোটিশ:</b> ক্লাউড আইপিতে ইউটিউব বট প্রটেকশন দিয়েছে।\n"
                "বট ওনার সরাসরি এই টেলিগ্রাম চ্যাটে নতুন <code>cookies.txt</code> ফাইল পাঠালে বট অবিলম্বে সক্রিয় করে নেবে!"
            )
        elif _looks_like_transient_format_issue(msg_str):
            hint = "\n\nভিডিওটির ফরম্যাট এই মুহূর্তে লোড করা যাচ্ছে না — কিছুক্ষণ পর আবার চেষ্টা করুন।"
        await query.edit_message_text(f"ভিডিও তথ্য আনতে সমস্যা হয়েছে:\n{e}{hint}", parse_mode="HTML" if hint else None)
        return

    title = (info.get("title") or "").strip()
    duration = info.get("duration")
    size_by_h: dict[int, int] = {}
    for f in (info.get("formats") or []):
        h = f.get("height")
        if h and f.get("vcodec") not in (None, "none"):
            sz = int(f.get("filesize") or f.get("filesize_approx") or 0)
            if sz > size_by_h.get(h, 0):
                size_by_h[h] = sz

    heights = sorted(
        {
            f.get("height")
            for f in (info.get("formats") or [])
            if f.get("height") and f.get("vcodec") not in (None, "none")
        },
        reverse=True,
    )
    info = None  # বড় info dict দ্রুত রেফারেন্স-মুক্ত করা

    header_lines = []
    if title:
        header_lines.append(f"🎬 {title[:120]}")
    if duration:
        header_lines.append(f"⏱️ দৈর্ঘ্য: {_format_eta(duration)}")
    header_lines.append("কোয়ালিটি বেছে নিন 👇")
    header = "\n".join(header_lines)

    buttons = [
        [
            InlineKeyboardButton(
                f"{h}p (~{_format_size(size_by_h[h])})" if size_by_h.get(h) else f"{h}p",
                callback_data=f"dl|video|{h}|{short_id}",
            )
        ]
        for h in heights[:QUALITY_OPTIONS_LIMIT]
    ]
    buttons.append([InlineKeyboardButton("⭐ দ্রুত সেরা কোয়ালিটি (Recommended)", callback_data=f"dl|video|best|{short_id}")])
    buttons.append([InlineKeyboardButton("❌ বাতিল করুন (Cancel)", callback_data="cancelmenu")])

    await query.edit_message_text(header, reply_markup=InlineKeyboardMarkup(buttons))


async def handle_thumbnail(update: Update, context: ContextTypes.DEFAULT_TYPE, short_id: str) -> None:
    query = update.callback_query
    entry = await _resolve_link_entry_or_recover(query, short_id)
    if not entry:
        await query.edit_message_text("লিংকের মেয়াদ শেষ হয়ে গেছে, নতুন করে লিংক পাঠান।")
        return
    url = entry.url
    src_msg_id = entry.source_message_id or getattr(getattr(query.message, "reply_to_message", None), "message_id", None)

    await query.edit_message_text("🖼️ থাম্বনেইল খোঁজা হচ্ছে... ⏳")
    chat_id = query.message.chat_id

    try:
        info = await asyncio.get_running_loop().run_in_executor(None, _extract_info_sync, url)
    except Exception as e:
        logger.warning("thumbnail extract_info failed: %s", e)
        if isinstance(e, AyloAuthError):
            await query.edit_message_text(str(e), parse_mode="HTML")
            return
        await query.edit_message_text(f"থাম্বনেইল আনতে সমস্যা হয়েছে:\n{e}")
        return

    title = (info.get("title") or "").strip()
    thumbnails = info.get("thumbnails") or []
    best_url = info.get("thumbnail")
    if thumbnails:
        try:
            best = max(thumbnails, key=lambda t: (t.get("width") or 0) * (t.get("height") or 0))
            best_url = best.get("url") or best_url
        except (TypeError, ValueError):
            pass
    info = None

    if not best_url:
        await query.edit_message_text("এই লিংকের জন্য কোনো থাম্বনেইল পাওয়া যায়নি।")
        return

    try:
        await context.bot.send_photo(
            chat_id,
            photo=best_url,
            caption=title[:1024] if title else None,
            reply_parameters=_reply_params(src_msg_id),
        )
        with contextlib.suppress(TelegramError):
            await context.bot.delete_message(chat_id=chat_id, message_id=query.message.message_id)
    except TelegramError as e:
        logger.warning("thumbnail send failed: %s", e)
        await query.edit_message_text(f"থাম্বনেইল পাঠাতে সমস্যা হয়েছে:\n{e}")


# ---------------------------------------------------------------------------
# সিকোয়েন্সিয়াল ১-বাই-১ ডাউনলোড কিউ, পারসিস্টেন্ট স্টেট ও অটো-রিজিউম ম্যানেজার
# ---------------------------------------------------------------------------
@dataclass
class DownloadJob:
    job_id: str
    chat_id: int
    status_message_id: int
    url: str
    mode: str
    quality: str
    context: object
    item_tag: str = ""
    source_message_id: Optional[int] = None
    file_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    title: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    running_task: Optional[asyncio.Task] = None

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def to_persist_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "file_id": self.file_id,
            "chat_id": self.chat_id,
            "status_message_id": self.status_message_id,
            "source_message_id": self.source_message_id,
            "url": self.url,
            "mode": self.mode,
            "quality": self.quality,
            "item_tag": self.item_tag,
            "title": self.title,
        }


JOBS_BY_ID: dict[str, DownloadJob] = {}
FAILED_RESUMABLE_JOBS: dict[str, DownloadJob] = {}
WAITING_QUEUE: list[DownloadJob] = []
CURRENT_RUNNING_JOB: Optional[DownloadJob] = None
QUEUE_WAKE_EVENT: Optional[asyncio.Event] = None
QUEUE_WORKER_TASK: Optional[asyncio.Task] = None
_QUEUE_CLOUD_SYNC_TASK: Optional[asyncio.Task] = None
_LAST_SYNCED_QUEUE_JSON: Optional[str] = None
_STARTUP_RECOVERY_DONE: bool = False


def _serialize_active_queue_json() -> str:
    items: list[dict] = []
    seen_ids: set[str] = set()
    if CURRENT_RUNNING_JOB is not None and not CURRENT_RUNNING_JOB.is_cancelled():
        items.append(CURRENT_RUNNING_JOB.to_persist_dict())
        seen_ids.add(CURRENT_RUNNING_JOB.job_id)
    for wj in list(WAITING_QUEUE):
        if not wj.is_cancelled() and wj.job_id not in seen_ids:
            items.append(wj.to_persist_dict())
            seen_ids.add(wj.job_id)
    return json.dumps(items, ensure_ascii=False, separators=(",", ":"))


def _write_queue_disk_sync(serialized: str) -> None:
    tmp = QUEUE_STATE_FILE.with_suffix(".tmp")
    with contextlib.suppress(OSError):
        tmp.write_text(serialized, encoding="utf-8")
        tmp.replace(QUEUE_STATE_FILE)


async def _debounced_cloud_queue_sync() -> None:
    global _LAST_SYNCED_QUEUE_JSON
    await asyncio.sleep(1.0)
    serialized = _serialize_active_queue_json()
    if len(serialized) > 28000:
        # ক্লাউড env var সাইজ নিরাপদ রাখতে প্রথম ৬০টি জব রাখা
        arr = json.loads(serialized)[:60]
        serialized = json.dumps(arr, ensure_ascii=False, separators=(",", ":"))
    if serialized == _LAST_SYNCED_QUEUE_JSON:
        return
    _LAST_SYNCED_QUEUE_JSON = serialized
    os.environ["PERSISTED_QUEUE"] = serialized
    await asyncio.get_running_loop().run_in_executor(None, _sync_cloud_env_var, "PERSISTED_QUEUE", serialized)


def _persist_queue_state() -> None:
    global _QUEUE_CLOUD_SYNC_TASK
    try:
        serialized = _serialize_active_queue_json()
        PROTECTED_FILE_IDS.clear()
        if CURRENT_RUNNING_JOB is not None and not CURRENT_RUNNING_JOB.is_cancelled():
            PROTECTED_FILE_IDS.add(CURRENT_RUNNING_JOB.file_id)
        for wj in WAITING_QUEUE:
            if not wj.is_cancelled():
                PROTECTED_FILE_IDS.add(wj.file_id)
        for fj in FAILED_RESUMABLE_JOBS.values():
            PROTECTED_FILE_IDS.add(fj.file_id)
        _write_queue_disk_sync(serialized)
        loop = asyncio.get_running_loop()
        if _QUEUE_CLOUD_SYNC_TASK is None or _QUEUE_CLOUD_SYNC_TASK.done():
            _QUEUE_CLOUD_SYNC_TASK = loop.create_task(_debounced_cloud_queue_sync())
    except Exception as e:
        logger.debug("Queue state persist warning: %s", e)


def _url_short_preview(stored_url: str) -> str:
    u, _ = _unpack_stored_url(stored_url)
    return (u[:62] + "...") if len(u) > 65 else u


def _ensure_queue_worker_started() -> None:
    global QUEUE_WAKE_EVENT, QUEUE_WORKER_TASK
    if QUEUE_WAKE_EVENT is None:
        QUEUE_WAKE_EVENT = asyncio.Event()
    if QUEUE_WORKER_TASK is None or QUEUE_WORKER_TASK.done():
        QUEUE_WORKER_TASK = asyncio.create_task(_queue_worker_loop())


def _active_queue_count() -> int:
    running = 1 if (CURRENT_RUNNING_JOB is not None and not CURRENT_RUNNING_JOB.is_cancelled()) else 0
    waiting = sum(1 for w in WAITING_QUEUE if not w.is_cancelled())
    return running + waiting


def _free_queue_slots() -> int:
    return max(0, MAX_QUEUE_SLOTS - _active_queue_count())


def _queue_full_alert_text() -> str:
    return (
        f"⚠️ কিউ পূর্ণ ({MAX_QUEUE_SLOTS}/{MAX_QUEUE_SLOTS} স্লট বুকড)!\n"
        f"ওভারলোড এড়াতে একসাথে সর্বোচ্চ {MAX_QUEUE_SLOTS}টি টাস্ক নেওয়া হয়। একটি ভিডিও শেষ হলে আবার ক্লিক করুন।"
    )


async def _refresh_waiting_queue_ui() -> None:
    """শুধুমাত্র পরবর্তী ১টি অপেক্ষমাণ আইটেমের স্ট্যাটাস আপডেট করে যাতে চ্যাট ফ্লাড-ব্যান কখনো না হয়।"""
    snapshot = list(WAITING_QUEUE[:1])
    used = _active_queue_count()
    for wjob in snapshot:
        if wjob.is_cancelled():
            continue
        if _chat_flood_remaining(wjob.chat_id) > 0:
            continue
        txt = (
            f"⏳ {wjob.item_tag}কিউতে অপেক্ষমাণ (পরবর্তী ভিডিও • স্লট {used}/{MAX_QUEUE_SLOTS})\n"
            f"চলমান ভিডিওটি শেষ হলেই স্বয়ংক্রিয়ভাবে শুরু হবে।\n"
            f"🔗 {_url_short_preview(wjob.url)}"
        )
        try:
            await wjob.context.bot.edit_message_text(
                chat_id=wjob.chat_id,
                message_id=wjob.status_message_id,
                text=txt,
                reply_markup=_cancel_kb(wjob.job_id),
            )
        except RetryAfter as ra:
            _note_chat_flood(wjob.chat_id, _extract_flood_seconds(ra) or 5)
        except TelegramError:
            pass


async def _queue_worker_loop() -> None:
    """একের পর এক (FIFO 1-by-1) সর্বোচ্চ স্পিডে প্রতিটি ভিডিও ডাউনলোড ও আপলোড সম্পন্ন করে।"""
    global CURRENT_RUNNING_JOB
    while True:
        if not WAITING_QUEUE:
            _persist_queue_state()
            if QUEUE_WAKE_EVENT is not None:
                QUEUE_WAKE_EVENT.clear()
                if not WAITING_QUEUE:
                    await QUEUE_WAKE_EVENT.wait()
            else:
                await asyncio.sleep(0.5)
            continue

        job = WAITING_QUEUE.pop(0)
        if job.is_cancelled():
            JOBS_BY_ID.pop(job.job_id, None)
            _persist_queue_state()
            continue

        CURRENT_RUNNING_JOB = job
        _persist_queue_state()
        asyncio.create_task(_refresh_waiting_queue_ui())
        try:
            job.running_task = asyncio.create_task(_execute_job_safe(job))
            await job.running_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception("Queue worker unexpected error: %s", e)
        finally:
            CURRENT_RUNNING_JOB = None
            JOBS_BY_ID.pop(job.job_id, None)
            _persist_queue_state()
            _trim_process_memory()


async def _enqueue_download_job(
    context: object,
    chat_id: int,
    status_message_id: int,
    url: str,
    mode: str,
    quality: str,
    item_tag: str = "",
    source_message_id: Optional[int] = None,
    job_id: Optional[str] = None,
    file_id: Optional[str] = None,
    title: Optional[str] = None,
    quiet_enqueue_edit: bool = False,
) -> DownloadJob:
    _ensure_queue_worker_started()
    jid = job_id or uuid.uuid4().hex[:8]
    fid = file_id or uuid.uuid4().hex[:8]
    job = DownloadJob(
        job_id=jid,
        chat_id=chat_id,
        status_message_id=status_message_id,
        url=url,
        mode=mode,
        quality=quality,
        context=context,
        item_tag=item_tag,
        source_message_id=source_message_id,
        file_id=fid,
        title=title,
    )
    FAILED_RESUMABLE_JOBS.pop(jid, None)
    JOBS_BY_ID[jid] = job
    PROTECTED_FILE_IDS.add(fid)
    ahead = _active_queue_count()
    WAITING_QUEUE.append(job)
    used_slots = _active_queue_count()
    _persist_queue_state()
    if QUEUE_WAKE_EVENT is not None:
        QUEUE_WAKE_EVENT.set()

    if not quiet_enqueue_edit and _chat_flood_remaining(chat_id) <= 0:
        preview = _url_short_preview(url)
        if ahead == 0:
            init_txt = f"⏳ {item_tag}ডাউনলোড/আপলোড শুরু হচ্ছে... (স্লট {used_slots}/{MAX_QUEUE_SLOTS})\n🔗 {preview}"
        else:
            pos_note = "পরবর্তী ভিডিও" if ahead == 1 else f"পজিশন #{ahead}"
            init_txt = (
                f"⏳ {item_tag}কিউতে যুক্ত হয়েছে ({pos_note} • স্লট {used_slots}/{MAX_QUEUE_SLOTS})\n"
                f"আগের ভিডিওটি শেষ হলেই স্বয়ংক্রিয়ভাবে শুরু হবে।\n"
                f"🔗 {preview}"
            )
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message_id,
                text=init_txt,
                reply_markup=_cancel_kb(jid),
            )
        except RetryAfter as ra:
            _note_chat_flood(chat_id, _extract_flood_seconds(ra) or 5)
        except TelegramError:
            pass
    return job


async def _force_abort_task_after_grace(task: Optional[asyncio.Task], delay: float = 1.2) -> None:
    if task is None or task.done():
        return
    await asyncio.sleep(delay)
    if not task.done():
        task.cancel()


async def _cancel_job_by_id(job_id: str, bot) -> tuple[bool, str]:
    failed_j = FAILED_RESUMABLE_JOBS.pop(job_id, None)
    if failed_j is not None:
        PROTECTED_FILE_IDS.discard(failed_j.file_id)
        for leftover in DOWNLOAD_DIR.glob(f"{failed_j.file_id}*"):
            with contextlib.suppress(OSError):
                leftover.unlink(missing_ok=True)
        _persist_queue_state()
        with contextlib.suppress(TelegramError):
            await bot.edit_message_text(
                chat_id=failed_j.chat_id,
                message_id=failed_j.status_message_id,
                text=f"🚫 {failed_j.item_tag}ডাউনলোড বাতিল করা হয়েছে।",
                reply_markup=None,
            )
        return True, "বাতিল করা হয়েছে!"

    job = JOBS_BY_ID.pop(job_id, None)
    if job is None:
        return False, "ইতিমধ্যে সম্পন্ন বা বাতিল হয়েছে।"

    job.cancel_event.set()
    PROTECTED_FILE_IDS.discard(job.file_id)
    if job in WAITING_QUEUE:
        with contextlib.suppress(ValueError):
            WAITING_QUEUE.remove(job)
        for leftover in DOWNLOAD_DIR.glob(f"{job.file_id}*"):
            with contextlib.suppress(OSError):
                leftover.unlink(missing_ok=True)
        _persist_queue_state()
        with contextlib.suppress(TelegramError):
            await bot.edit_message_text(
                chat_id=job.chat_id,
                message_id=job.status_message_id,
                text=f"🚫 {job.item_tag}ডাউনলোড কিউ থেকে বাতিল করা হয়েছে।",
                reply_markup=None,
            )
        asyncio.create_task(_refresh_waiting_queue_ui())
        return True, "কিউ থেকে বাতিল করা হয়েছে!"

    _persist_queue_state()
    with contextlib.suppress(TelegramError):
        await bot.edit_message_text(
            chat_id=job.chat_id,
            message_id=job.status_message_id,
            text=f"🛑 {job.item_tag}ডাউনলোড বাতিল করা হচ্ছে...",
            reply_markup=None,
        )
    asyncio.create_task(_force_abort_task_after_grace(job.running_task, 1.2))
    return True, "চলমান ডাউনলোড বাতিল করা হয়েছে!"


async def _cancel_all_for_chat(chat_id: int, bot) -> int:
    cancelled_count = 0
    for fjid, fjob in list(FAILED_RESUMABLE_JOBS.items()):
        if fjob.chat_id == chat_id:
            FAILED_RESUMABLE_JOBS.pop(fjid, None)
            PROTECTED_FILE_IDS.discard(fjob.file_id)
            for leftover in DOWNLOAD_DIR.glob(f"{fjob.file_id}*"):
                with contextlib.suppress(OSError):
                    leftover.unlink(missing_ok=True)
            cancelled_count += 1
            with contextlib.suppress(TelegramError):
                await bot.edit_message_text(
                    chat_id=fjob.chat_id,
                    message_id=fjob.status_message_id,
                    text=f"🚫 {fjob.item_tag}ডাউনলোড বাতিল করা হয়েছে।",
                    reply_markup=None,
                )

    for wjob in list(WAITING_QUEUE):
        if wjob.chat_id == chat_id:
            wjob.cancel_event.set()
            PROTECTED_FILE_IDS.discard(wjob.file_id)
            with contextlib.suppress(ValueError):
                WAITING_QUEUE.remove(wjob)
            for leftover in DOWNLOAD_DIR.glob(f"{wjob.file_id}*"):
                with contextlib.suppress(OSError):
                    leftover.unlink(missing_ok=True)
            JOBS_BY_ID.pop(wjob.job_id, None)
            cancelled_count += 1
            with contextlib.suppress(TelegramError):
                await bot.edit_message_text(
                    chat_id=wjob.chat_id,
                    message_id=wjob.status_message_id,
                    text=f"🚫 {wjob.item_tag}ডাউনলোড কিউ থেকে বাতিল করা হয়েছে।",
                    reply_markup=None,
                )

    cur = CURRENT_RUNNING_JOB
    if cur is not None and cur.chat_id == chat_id and not cur.is_cancelled():
        cur.cancel_event.set()
        PROTECTED_FILE_IDS.discard(cur.file_id)
        JOBS_BY_ID.pop(cur.job_id, None)
        cancelled_count += 1
        with contextlib.suppress(TelegramError):
            await bot.edit_message_text(
                chat_id=cur.chat_id,
                message_id=cur.status_message_id,
                text=f"🛑 {cur.item_tag}ডাউনলোড বাতিল করা হচ্ছে...",
                reply_markup=None,
            )
        asyncio.create_task(_force_abort_task_after_grace(cur.running_task, 1.2))
    _persist_queue_state()
    return cancelled_count


async def cancelall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    n = await _cancel_all_for_chat(update.effective_chat.id, context.bot)
    if n > 0:
        await update.message.reply_text(f"🚫 মোট {n}টি চলমান/অপেক্ষমাণ ডাউনলোড বাতিল করা হয়েছে।")
    else:
        await update.message.reply_text("এই মুহূর্তে কোনো চলমান বা অপেক্ষমাণ ডাউনলোড নেই।")


async def _enqueue_bulk_batch(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    header_msg_id: int,
    urls: list[str],
    mode: str,
    quality: str,
    source_message_id: Optional[int] = None,
) -> None:
    total = len(urls)
    cancel_all_row = [InlineKeyboardButton("🚫 সব কিউ বাতিল করুন (Cancel All)", callback_data="cancelall")]
    cancel_all_kb = InlineKeyboardMarkup([cancel_all_row])
    with contextlib.suppress(TelegramError):
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=header_msg_id,
            text=f"📦 <b>{total}টি লিংক কিউতে যুক্ত হচ্ছে (সর্বোচ্চ {MAX_QUEUE_SLOTS} স্লট)...</b>",
            parse_mode="HTML",
            reply_markup=cancel_all_kb,
        )

    loop = asyncio.get_running_loop()
    enqueued = 0
    dup_prompted = 0
    deferred_urls: list[str] = []
    for idx, raw_u in enumerate(urls, start=1):
        if _free_queue_slots() <= 0:
            deferred_urls.extend(urls[idx - 1 :])
            break
        canonical_u = await loop.run_in_executor(None, _resolve_share_url, raw_u)
        tag = f"[{idx}/{total}] "
        prev = _lookup_previous_download(chat_id, canonical_u)
        if prev is not None:
            sid_dup = LINKS.put(canonical_u, source_message_id=source_message_id)
            prev_title = (prev.get("title") or "").strip()
            t_line = f"🎬 <i>{html.escape(prev_title)}</i>\n" if prev_title else ""
            dup_txt = (
                f"⚠️ <b>{html.escape(tag)}এই লিংকটি আগেও ডাউনলোড করা হয়েছে!</b>\n"
                f"{t_line}"
                f"🔗 <code>{html.escape(_url_short_preview(canonical_u))}</code>\n"
                f"আবার ডাউনলোড করবেন নাকি বাতিল করবেন? 👇"
            )
            dup_kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔄 আবার ডাউনলোড করুন (Re-Download)", callback_data=f"redlgo|{mode}|{quality}|{sid_dup}")],
                    [InlineKeyboardButton("❌ বাতিল করুন (Cancel)", callback_data="cancelmenu")],
                ]
            )
            try:
                await context.bot.send_message(
                    chat_id,
                    dup_txt,
                    parse_mode="HTML",
                    reply_markup=dup_kb,
                    reply_parameters=_reply_params(source_message_id),
                )
            except RetryAfter as e:
                await asyncio.sleep(float(getattr(e, "retry_after", 3)) + 1)
                await context.bot.send_message(
                    chat_id,
                    dup_txt,
                    parse_mode="HTML",
                    reply_markup=dup_kb,
                    reply_parameters=_reply_params(source_message_id),
                )
            dup_prompted += 1
            if idx < total:
                await asyncio.sleep(0.20)
            continue

        if _free_queue_slots() <= 0:
            deferred_urls.extend(urls[idx - 1 :])
            break

        try:
            card_msg = await context.bot.send_message(
                chat_id,
                f"⏳ {tag}কিউ প্রস্তুত হচ্ছে...\n🔗 {_url_short_preview(canonical_u)}",
                reply_parameters=_reply_params(source_message_id),
            )
        except RetryAfter as e:
            await asyncio.sleep(float(getattr(e, "retry_after", 3)) + 1)
            card_msg = await context.bot.send_message(
                chat_id,
                f"⏳ {tag}কিউ প্রস্তুত হচ্ছে...\n🔗 {_url_short_preview(canonical_u)}",
                reply_parameters=_reply_params(source_message_id),
            )
        await _enqueue_download_job(
            context,
            chat_id,
            card_msg.message_id,
            canonical_u,
            mode,
            quality,
            item_tag=tag,
            source_message_id=source_message_id,
        )
        enqueued += 1
        if idx < total:
            await asyncio.sleep(0.20)

    extra_dup_note = f"\n⚠️ <i>{dup_prompted}টি পূর্ব-ডাউনলোডকৃত লিংকের জন্য নিচে Re-Download / Cancel বাটন দেওয়া হয়েছে।</i>" if dup_prompted else ""
    kb_rows: list[list[InlineKeyboardButton]] = []
    deferred_note = ""
    if deferred_urls:
        rem_bid = BATCHES.put(deferred_urls, source_message_id=source_message_id)
        deferred_note = (
            f"\n⏸️ <b>কিউ স্লট সীমা ({MAX_QUEUE_SLOTS}/{MAX_QUEUE_SLOTS} পূর্ণ):</b> ওভারলোড এড়াতে বাকি "
            f"<b>{len(deferred_urls)}টি</b> লিংক অপেক্ষায় রাখা হয়েছে। চলমান ভিডিও শেষ হলে নিচের বাটনে চাপলেই বাকিগুলো যুক্ত হবে!"
        )
        kb_rows.append(
            [InlineKeyboardButton(f"➕ বাকি {len(deferred_urls)}টি লিংক কিউ করুন (স্লট খালি হলে)", callback_data=f"bulk|{mode}|{quality}|{rem_bid}")]
        )
    if enqueued > 0:
        kb_rows.append(cancel_all_row)

    used_now = _active_queue_count()
    with contextlib.suppress(TelegramError):
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=header_msg_id,
            text=(
                f"✅ <b>মোট {enqueued}টি নতুন ভিডিও কিউতে যুক্ত হয়েছে! (স্লট: {used_now}/{MAX_QUEUE_SLOTS})</b>"
                f"{extra_dup_note}{deferred_note}\n"
                "একের পর এক সর্বোচ্চ মাল্টি-থ্রেড স্পিডে ডাউনলোড ও আপলোড হবে।"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb_rows) if kb_rows else None,
        )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    data = query.data or ""

    if data == "cancelmenu":
        await query.answer("বাতিল করা হয়েছে")
        with contextlib.suppress(TelegramError):
            await query.message.delete()
        return

    if data.startswith("cancel|"):
        _, job_id = data.split("|", 1)
        ok, note = await _cancel_job_by_id(job_id, context.bot)
        await query.answer(note)
        if not ok:
            with contextlib.suppress(TelegramError):
                await query.edit_message_reply_markup(reply_markup=None)
        return

    if data.startswith("resumejob|"):
        _, job_id = data.split("|", 1)
        if job_id not in FAILED_RESUMABLE_JOBS:
            await query.answer("এই আইটেমটি ইতিমধ্যে রিজিউম বা সম্পন্ন হয়েছে।")
            return
        if _free_queue_slots() <= 0:
            await query.answer(_queue_full_alert_text(), show_alert=True)
            return
        fjob = FAILED_RESUMABLE_JOBS.pop(job_id, None)
        if fjob is None:
            await query.answer("এই আইটেমটি ইতিমধ্যে রিজিউম বা সম্পন্ন হয়েছে।")
            return
        await query.answer("♻️ যেখান থেকে আটকেছিল সেখান থেকেই শুরু হচ্ছে!")
        await _enqueue_download_job(
            context,
            fjob.chat_id,
            fjob.status_message_id,
            fjob.url,
            fjob.mode,
            fjob.quality,
            item_tag=fjob.item_tag,
            source_message_id=fjob.source_message_id,
            job_id=fjob.job_id,
            file_id=fjob.file_id,
            title=fjob.title,
        )
        return

    if data == "cancelall":
        chat_id = query.message.chat_id
        n = await _cancel_all_for_chat(chat_id, context.bot)
        await query.answer(f"{n}টি ডাউনলোড বাতিল করা হয়েছে" if n else "কোনো সক্রিয় কিউ নেই")
        with contextlib.suppress(TelegramError):
            await query.edit_message_text(
                f"🚫 <b>সব ডাউনলোড কিউ বাতিল করা হয়েছে ({n}টি)।</b>",
                parse_mode="HTML",
                reply_markup=None,
            )
        return

    if data.startswith("redl|"):
        _, short_id = data.split("|", 1)
        entry = await _resolve_link_entry_or_recover(query, short_id)
        if not entry:
            await query.answer("লিংকের মেয়াদ শেষ হয়ে গেছে", show_alert=True)
            await query.edit_message_text("লিংকের মেয়াদ শেষ হয়ে গেছে, নতুন করে লিংক পাঠান।")
            return
        await query.answer("🔄 পুনরায় ডাউনলোড মেনু খোলা হয়েছে")
        await query.edit_message_text(
            "🔄 কী ধরনের ডাউনলোড চান বেছে নিন 👇",
            reply_markup=_single_download_menu_kb(short_id),
        )
        return

    if data.startswith("redlgo|"):
        if _free_queue_slots() <= 0:
            await query.answer(_queue_full_alert_text(), show_alert=True)
            return
        _, mode, quality, short_id = data.split("|", 3)
        entry = await _resolve_link_entry_or_recover(query, short_id)
        if not entry:
            await query.answer("লিংকের মেয়াদ শেষ হয়ে গেছে", show_alert=True)
            await query.edit_message_text("লিংকের মেয়াদ শেষ হয়ে গেছে, নতুন করে লিংক পাঠান।")
            return
        if _free_queue_slots() <= 0:
            await query.answer(_queue_full_alert_text(), show_alert=True)
            return
        await query.answer("🔄 আবার ডাউনলোড কিউতে যুক্ত হচ্ছে!")
        src_mid = entry.source_message_id or getattr(getattr(query.message, "reply_to_message", None), "message_id", None)
        await _enqueue_download_job(
            context,
            query.message.chat_id,
            query.message.message_id,
            entry.url,
            mode,
            quality,
            source_message_id=src_mid,
        )
        return

    if data.startswith("bulk|"):
        if _free_queue_slots() <= 0:
            await query.answer(_queue_full_alert_text(), show_alert=True)
            return
        _, mode, quality, batch_id = data.split("|", 3)
        b_entry = _resolve_batch_entry_or_recover(query, batch_id)
        if not b_entry:
            await query.answer("লিংক তালিকার মেয়াদ শেষ হয়ে গেছে", show_alert=True)
            await query.edit_message_text("লিংক তালিকার মেয়াদ শেষ হয়ে গেছে, নতুন করে লিংক পাঠান।")
            return
        await query.answer("⚡ কিউতে যুক্ত হচ্ছে!")
        src_mid = b_entry.source_message_id or getattr(getattr(query.message, "reply_to_message", None), "message_id", None)
        asyncio.create_task(
            _enqueue_bulk_batch(
                context,
                query.message.chat_id,
                query.message.message_id,
                b_entry.urls,
                mode,
                quality,
                source_message_id=src_mid,
            )
        )
        return

    if data.startswith("dl|"):
        if _free_queue_slots() <= 0:
            await query.answer(_queue_full_alert_text(), show_alert=True)
            return
        _, mode, quality, short_id = data.split("|", 3)
        entry = await _resolve_link_entry_or_recover(query, short_id)
        if not entry:
            await query.answer("লিংকের মেয়াদ শেষ হয়ে গেছে", show_alert=True)
            await query.edit_message_text("লিংকের মেয়াদ শেষ হয়ে গেছে, নতুন করে লিংক পাঠান।")
            return
        if _free_queue_slots() <= 0:
            await query.answer(_queue_full_alert_text(), show_alert=True)
            return
        await query.answer("⚡ ডাউনলোড কিউতে যুক্ত হচ্ছে!")
        src_mid = entry.source_message_id or getattr(getattr(query.message, "reply_to_message", None), "message_id", None)
        await _enqueue_download_job(
            context,
            query.message.chat_id,
            query.message.message_id,
            entry.url,
            mode,
            quality,
            source_message_id=src_mid,
        )
        return

    await query.answer()

    if data.startswith("menu|"):
        _, short_id = data.split("|", 1)
        await show_quality_menu(update, short_id)
        return

    if data.startswith("thumb|"):
        _, short_id = data.split("|", 1)
        await handle_thumbnail(update, context, short_id)
        return

    if data.startswith("bulksplit|"):
        _, batch_id = data.split("|", 1)
        b_entry = _resolve_batch_entry_or_recover(query, batch_id)
        if not b_entry:
            await query.edit_message_text("লিংক তালিকার মেয়াদ শেষ হয়ে গেছে, নতুন করে লিংক পাঠান।")
            return
        batch_urls = b_entry.urls
        src_mid = b_entry.source_message_id or getattr(getattr(query.message, "reply_to_message", None), "message_id", None)
        total = len(batch_urls)
        with contextlib.suppress(TelegramError):
            await query.edit_message_text(f"📋 মোট {total}টি লিংকের আলাদা ডাউনলোড মেনু পাঠানো হচ্ছে...")
        for idx, raw_u in enumerate(batch_urls, start=1):
            await _send_single_link_menu(
                query.message.chat_id,
                context,
                raw_u,
                source_message_id=src_mid,
                prefix_label=f"[{idx}/{total}] {_url_short_preview(raw_u)}\n",
            )
            if idx < total:
                await asyncio.sleep(0.25)
        with contextlib.suppress(TelegramError):
            await query.message.delete()
        return


async def _execute_job_safe(job: DownloadJob) -> None:
    """একটি DownloadJob চালায় এবং ক্যানসেল বা ক্ষণস্থায়ী এরর রিজিউমযোগ্যভাবে হ্যান্ডেল করে।"""
    try:
        await process_download(
            job.context,
            job.chat_id,
            job.status_message_id,
            job.url,
            job.mode,
            job.quality,
            job=job,
        )
    except (DownloadCancelledError, asyncio.CancelledError):
        logger.info("🚫 Job %s ইউজার কর্তৃক বাতিল হয়েছে।", job.job_id)
        PROTECTED_FILE_IDS.discard(job.file_id)
        for leftover in DOWNLOAD_DIR.glob(f"{job.file_id}*"):
            with contextlib.suppress(OSError):
                leftover.unlink(missing_ok=True)
        with contextlib.suppress(TelegramError):
            await job.context.bot.edit_message_text(
                chat_id=job.chat_id,
                message_id=job.status_message_id,
                text=f"🚫 {job.item_tag}ডাউনলোড বাতিল করা হয়েছে।",
                reply_markup=None,
            )
    except Exception as e:
        logger.exception("process_download এ অপ্রত্যাশিত সমস্যা (রিজিউম স্টেট সংরক্ষিত): %s", e)
        job.created_at = time.time()
        FAILED_RESUMABLE_JOBS[job.job_id] = job
        PROTECTED_FILE_IDS.add(job.file_id)
        if len(FAILED_RESUMABLE_JOBS) > 10:
            oldest_f = sorted(FAILED_RESUMABLE_JOBS.items(), key=lambda kv: getattr(kv[1], "created_at", 0.0))[: len(FAILED_RESUMABLE_JOBS) - 10]
            for old_jid, old_fj in oldest_f:
                FAILED_RESUMABLE_JOBS.pop(old_jid, None)
                PROTECTED_FILE_IDS.discard(old_fj.file_id)
                for leftover in DOWNLOAD_DIR.glob(f"{old_fj.file_id}*"):
                    with contextlib.suppress(OSError):
                        if leftover.is_dir():
                            shutil.rmtree(leftover, ignore_errors=True)
                        else:
                            leftover.unlink(missing_ok=True)
        resume_kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("♻️ এখান থেকেই পুনরায় শুরু করুন (Resume)", callback_data=f"resumejob|{job.job_id}")],
                [InlineKeyboardButton("❌ বাতিল করুন (Cancel)", callback_data=f"cancel|{job.job_id}")],
            ]
        )
        with contextlib.suppress(TelegramError):
            await job.context.bot.edit_message_text(
                chat_id=job.chat_id,
                message_id=job.status_message_id,
                text=(
                    f"⚠️ {job.item_tag}সাময়িক সংযোগ বিঘ্নে আপলোড পজ হয়েছে।\n"
                    f"💾 ডাউনলোড ও আপলোড চেকপয়েন্ট সংরক্ষিত আছে — নিচের <b>Resume</b> বাটনে চাপলে বা <code>/resume</code> লিখলে ঠিক এখান থেকেই শুরু হবে!\n"
                    f"🔗 {_url_short_preview(job.url)}"
                ),
                parse_mode="HTML",
                reply_markup=resume_kb,
            )


async def process_download(
    context: object,
    chat_id: int,
    status_message_id: int,
    url: str,
    mode: str,
    quality: str,
    job: Optional[DownloadJob] = None,
) -> None:
    cancel_check = job.is_cancelled if job else None
    cancel_markup = _cancel_kb(job.job_id) if job else None
    tag = job.item_tag if job else ""

    if cancel_check and cancel_check():
        raise DownloadCancelledError()

    if not await _ensure_disk_space(chat_id, context):
        return

    file_id = job.file_id if job else uuid.uuid4().hex[:8]
    ACTIVE_FILE_IDS.add(file_id)
    cleanup_all_on_exit = False
    try:
        loop = asyncio.get_running_loop()
        existing_ready = [
            f
            for f in DOWNLOAD_DIR.glob(f"{file_id}.*")
            if f.suffix.lower() not in (".part", ".ytdl", ".temp", ".json", ".tmp", ".jpg")
            and not f.name.endswith(".ulckpt.json")
            and ".frag" not in f.name
            and f.stat().st_size > 0
        ]

        if existing_ready:
            title = (job.title if (job and job.title) else existing_ready[0].stem)
            logger.info(
                "♻️ ডিস্কে পূর্ব-ডাউনলোডকৃত ফাইল পাওয়া গেছে (%s, %.1f MB) — ডাউনলোড স্কিপ করে সরাসরি আপলোড রিজিউম হচ্ছে!",
                existing_ready[0].name,
                existing_ready[0].stat().st_size / (1024 * 1024),
            )
            if _chat_flood_remaining(chat_id) <= 0:
                with contextlib.suppress(TelegramError):
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message_id,
                        text=f"♻️ {tag}আগের ডাউনলোড অক্ষত আছে — যেখান থেকে আপলোড আটকেছিল সেখান থেকেই শুরু হচ্ছে...",
                        reply_markup=cancel_markup,
                    )
        else:
            if _chat_flood_remaining(chat_id) <= 0:
                with contextlib.suppress(TelegramError):
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message_id,
                        text=f"⬇️ {tag}ডাউনলোড শুরু হচ্ছে...\n🔗 {_url_short_preview(url)}",
                        reply_markup=cancel_markup,
                    )

            ydl_opts = {"outtmpl": str(DOWNLOAD_DIR / f"{file_id}.%(ext)s"), **_format_opts_for(mode, quality)}
            tracker = ProgressTracker(
                loop,
                context.bot,
                chat_id,
                status_message_id,
                f"⬇️ {tag}ডাউনলোড",
                reply_markup=cancel_markup,
                cancel_check=cancel_check,
            )
            ydl_opts["progress_hooks"] = [_make_ytdlp_progress_hook(tracker, cancel_check=cancel_check)]

            try:
                title = await loop.run_in_executor(None, _download_sync, url, ydl_opts)
                if job is not None and title:
                    job.title = title
                    _persist_queue_state()
            except (DownloadCancelledError, asyncio.CancelledError):
                cleanup_all_on_exit = True
                raise
            except Exception as e:
                tracker.close()
                if cancel_check and cancel_check():
                    cleanup_all_on_exit = True
                    raise DownloadCancelledError()
                logger.warning("download failed for %s: %s", url, e)
                cleanup_all_on_exit = True
                if isinstance(e, AyloAuthError):
                    with contextlib.suppress(TelegramError):
                        await context.bot.edit_message_text(chat_id=chat_id, message_id=status_message_id, text=str(e), parse_mode="HTML")
                    return
                msg_str = str(e)
                ip_lock_msg = _ip_locked_stream_message(url, msg_str)
                if ip_lock_msg:
                    with contextlib.suppress(TelegramError):
                        await context.bot.edit_message_text(chat_id=chat_id, message_id=status_message_id, text=ip_lock_msg, parse_mode="HTML")
                    return
                if "not made this video available in your country" in msg_str.lower() or "geographic restriction" in msg_str.lower():
                    err_text = (
                        "❌ <b>ভিডিওটি কান্ট্রি-লকড (Geo-Restricted)!</b>\n\n"
                        "🌍 আপলোডার এই ভিডিওটি ক্লাউড সার্ভারের দেশে ব্লক করে রেখেছে।\n"
                        "💡 ওনার চ্যাটে <code>/proxy http://user:pass@ip:port</code> কমান্ড দিয়ে এশিয়া/ইউরোপের প্রক্সি যুক্ত করলে এই ভিডিওও ডাউনলোড হবে।"
                    )
                    with contextlib.suppress(TelegramError):
                        await context.bot.edit_message_text(chat_id=chat_id, message_id=status_message_id, text=err_text, parse_mode="HTML")
                    return
                hint = ""
                if "confirm you" in msg_str.lower() or "not a bot" in msg_str.lower():
                    hint = (
                        "\n\n💡 <b>ইউটিউব নোটিশ:</b> সার্ভার আইপিতে ইউটিউব বট প্রটেকশন দিয়েছে।\n"
                        "বট ওনার সরাসরি এই টেলিগ্রাম চ্যাটে নতুন <code>cookies.txt</code> ফাইল পাঠালে বট অবিলম্বে সক্রিয় করে নেবে!"
                    )
                with contextlib.suppress(TelegramError):
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message_id,
                        text=f"ডাউনলোড ব্যর্থ হয়েছে:\n{e}{hint}",
                        parse_mode="HTML" if hint else None,
                    )
                return
            finally:
                tracker.close()

        if cancel_check and cancel_check():
            cleanup_all_on_exit = True
            raise DownloadCancelledError()

        matches = [
            f
            for f in DOWNLOAD_DIR.glob(f"{file_id}.*")
            if f.suffix.lower() not in (".part", ".ytdl", ".temp", ".json", ".tmp", ".jpg")
            and not f.name.endswith(".ulckpt.json")
            and ".frag" not in f.name
        ]
        if not matches:
            cleanup_all_on_exit = True
            await context.bot.send_message(
                chat_id,
                "ফাইল পাওয়া যায়নি, আবার চেষ্টা করুন।",
                reply_parameters=_reply_params(job.source_message_id if job else None),
            )
            return
        filepath = sorted(matches, key=lambda p: p.stat().st_size, reverse=True)[0]

        if _chat_flood_remaining(chat_id) <= 0:
            async def _fire_upload_start_status() -> None:
                with contextlib.suppress(TelegramError):
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message_id,
                        text=f"⚡ {tag}ডাউনলোড সম্পন্ন — সরাসরি ম্যাক্স স্পিডে আপলোড শুরু হচ্ছে...",
                        reply_markup=cancel_markup,
                    )
            asyncio.create_task(_fire_upload_start_status())

        existing_split_parts = sorted(
            [
                p
                for p in DOWNLOAD_DIR.glob(f"{file_id}.part*.*")
                if not p.name.endswith(".ulckpt.json") and p.suffix.lower() not in (".tmp", ".json", ".jpg")
            ]
        )
        parts: list[Path]
        if existing_split_parts:
            parts = existing_split_parts
        elif mode != "audio" and filepath.stat().st_size > MAX_TELEGRAM_FILE_SIZE:
            size_mb = filepath.stat().st_size // (1024 * 1024)
            limit_mb = MAX_TELEGRAM_FILE_SIZE // (1024 * 1024)
            src_rp = _reply_params(job.source_message_id if job else None)
            await context.bot.send_message(
                chat_id,
                f"ফাইলটি {size_mb}MB — টেলিগ্রামের {limit_mb}MB সীমার জন্য কোয়ালিটি ঠিক রেখে একাধিক "
                "পার্টে ভাগ করে পাঠানো হচ্ছে 📦, একটু সময় লাগতে পারে...",
                reply_parameters=src_rp,
            )
            try:
                parts = await loop.run_in_executor(None, _split_video_sync, filepath, SPLIT_TARGET_SIZE)
            except Exception as split_err:
                if cancel_check and cancel_check():
                    cleanup_all_on_exit = True
                    raise DownloadCancelledError()
                logger.warning("split failed, falling back to compression: %s", split_err)
                await context.bot.send_message(
                    chat_id,
                    "স্প্লিট করা যায়নি, তাই কমপ্রেস করার চেষ্টা করা হচ্ছে 🗜️...",
                    reply_parameters=src_rp,
                )
                try:
                    compressed = await loop.run_in_executor(None, _compress_video_sync, filepath, COMPRESS_TARGET_SIZE)
                    parts = [compressed]
                except Exception as compress_err:
                    logger.warning("compression fallback also failed: %s", compress_err)
                    await context.bot.send_message(chat_id, f"দুঃখিত, ফাইলটি পাঠানো সম্ভব হয়নি:\n{compress_err}")
                    cleanup_all_on_exit = True
                    return
        else:
            parts = [filepath]

        if cancel_check and cancel_check():
            cleanup_all_on_exit = True
            raise DownloadCancelledError()

        await _deliver_parts(context, chat_id, status_message_id, parts, mode, title, job=job)
        cleanup_all_on_exit = True
    finally:
        if cleanup_all_on_exit or (cancel_check and cancel_check()):
            PROTECTED_FILE_IDS.discard(file_id)
            for leftover in DOWNLOAD_DIR.glob(f"{file_id}*"):
                with contextlib.suppress(OSError):
                    if leftover.is_dir():
                        shutil.rmtree(leftover, ignore_errors=True)
                    else:
                        leftover.unlink(missing_ok=True)
        ACTIVE_FILE_IDS.discard(file_id)


def _build_caption(title: Optional[str], idx: int, total: int) -> Optional[str]:
    pieces = []
    if title:
        pieces.append(title[:900])
    if total > 1:
        pieces.append(f"Part {idx}/{total}")
    return "\n".join(pieces) if pieces else None


SEND_MAX_ATTEMPTS = 3  # ক্ষণস্থায়ী নেটওয়ার্ক/ফ্লাড-কন্ট্রোল এরর হলে কতবার retry হবে


# ---------------------------------------------------------------------------
# FastTelethon v6 — ৩২x৫ মাল্টি-AuthKey পুল + Dedicated Media-Only DC5 IPs
#                  + ৪-থ্রেড Off-Loop os.pread ও ২MB ইনস্ট্যান্ট ফার্স্ট-ব্লক প্রি-সিরিয়ালাইজার
#                  + ৪MB সকেট বাফার (জিরো মিড-স্ট্রিম রিসেট) + জিরো-ওয়েট ফ্লাড ওয়ার্ক-স্টিলিং
#                  + ৩০০ms হেজড টেইল স্পেকুলেশন + বাউন্ডেড InputPeer ক্যাশ
# ---------------------------------------------------------------------------
TELETHON_POOL_KEYS_FILE = Path("telethon_pool_keys.json")
MTPROTO_POOL_SIZE = int(os.environ.get("MTPROTO_POOL_SIZE", "32"))
SLOTS_PER_AUTH_KEY = int(os.environ.get("SLOTS_PER_AUTH_KEY", "5"))
MTPROTO_WARM_POOL: Optional["_MTProtoWorkerPool"] = None
_UPLOAD_PREPACK_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ul-prepack")
_TELETHON_PEER_CACHE: dict[int, object] = {}


class _NullUpdatesQueue:
    """আপলোড সকেটে পুশ হওয়া অপ্রয়োজনীয় সার্ভার আপডেট নিঃশব্দে ড্রপ করে যাতে _recv_loop কখনোই ক্র্যাশ না করে।"""

    def put_nowait(self, _item: object) -> None:
        pass


_NULL_UPDATES_QUEUE = _NullUpdatesQueue()


class _WorkerFloodStealError(Exception):
    """কোনো একক AuthKey-তে ক্ষণস্থায়ী ফ্লাড-ওয়েট এলে চাঙ্কটি ০ms অপেক্ষায় অন্য ৩১টি ওয়ার্কারে স্থানান্তরের সিগন্যাল।"""


if TELETHON_AVAILABLE:
    class _PrebuiltBigPartRequest(SaveBigFilePartRequest):
        """ব্যাকগ্রাউন্ড থ্রেডে প্রি-সিরিয়ালাইজ করা TL রিকোয়েস্ট — মেইন uvloop থ্রেডে ০ বাইট কপি!"""

        def __init__(self, file_id: int, file_part: int, file_total_parts: int, raw_tl: bytes) -> None:
            self.file_id = file_id
            self.file_part = file_part
            self.file_total_parts = file_total_parts
            self.bytes = b""
            self._raw_tl = raw_tl

        def _bytes(self) -> bytes:
            return self._raw_tl

        def __bytes__(self) -> bytes:
            return self._raw_tl

    class _PrebuiltSmallPartRequest(SaveFilePartRequest):
        """১০MB-এর ছোট ফাইলের জন্য ব্যাকগ্রাউন্ড থ্রেডে প্রি-সিরিয়ালাইজ করা TL রিকোয়েস্ট।"""

        def __init__(self, file_id: int, file_part: int, raw_tl: bytes) -> None:
            self.file_id = file_id
            self.file_part = file_part
            self.bytes = b""
            self._raw_tl = raw_tl

        def _bytes(self) -> bytes:
            return self._raw_tl

        def __bytes__(self) -> bytes:
            return self._raw_tl


def _build_prebuilt_part_req(file_id: int, p_idx: int, part_count: int, chunk: bytes, is_big: bool):
    if is_big:
        tmp_req = SaveBigFilePartRequest(file_id, p_idx, part_count, chunk)
        raw_tl = bytes(tmp_req)
        tmp_req.bytes = b""
        return _PrebuiltBigPartRequest(file_id, p_idx, part_count, raw_tl)
    tmp_s = SaveFilePartRequest(file_id, p_idx, chunk)
    raw_tl = bytes(tmp_s)
    tmp_s.bytes = b""
    return _PrebuiltSmallPartRequest(file_id, p_idx, raw_tl)


def _tune_mtproto_socket(conn) -> None:
    """কানেক্টের সময় একবারই TCP_NODELAY, TCP_QUICKACK, SO_KEEPALIVE এবং 4MB রিং বাফার সক্রিয় করে (মিড-স্ট্রিম বাফার রিসেট ছাড়াই!)।"""
    with contextlib.suppress(Exception):
        writer = getattr(conn, "_writer", None)
        sock = writer.get_extra_info("socket") if writer is not None else None
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if hasattr(socket, "TCP_QUICKACK"):
                with contextlib.suppress(OSError):
                    sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, "TCP_QUICKACK"), 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 512 * 1024)


class _IndependentMTProtoWorker:
    """
    একটি সম্পূর্ণ স্বতন্ত্র 2048-bit AuthKey সহ ডেডিকেটেড MTProto সকেট ওয়ার্কার (Regular + Media-Only DC5 গেটওয়ে)।
    প্রতিটি আলাদা AuthKey টেলিগ্রামের সার্ভারে নিজস্ব আলাদা RPC রেট-লিমিট বাকেট পায় এবং
    এক ওয়ার্কারে ফ্লাড-ওয়েট এলে চাঙ্কটি ০ms অপেক্ষায় অন্য ওয়ার্কারে চলে যায় (সকেট রিসেট ছাড়াই)!
    """

    def __init__(
        self,
        worker_id: int,
        client: "TelegramClient",
        dc_id: int,
        ip: str,
        port: int,
        saved_key_bytes: Optional[bytes] = None,
        auth_ip: Optional[str] = None,
        auth_port: Optional[int] = None,
    ) -> None:
        self.worker_id = worker_id
        self.client = client
        self.dc_id = dc_id
        self.ip = ip
        self.port = port
        self.auth_ip = auth_ip or ip
        self.auth_port = auth_port or port
        self.auth_key: Optional["AuthKey"] = AuthKey(data=saved_key_bytes) if (saved_key_bytes and len(saved_key_bytes) == 256) else None
        self.sender: Optional["MTProtoSender"] = None
        self.is_independent_key: bool = bool(self.auth_key is not None)
        # ৪ms পেসিং: ৫x512KB পাইপলাইন ও MessagePacker স্বয়ংক্রিয় msg_container ব্যাচিং!
        self.min_interval: float = 0.004
        self._next_allowed_ts: float = 0.0
        self._cooldown_until: float = 0.0
        self._pace_lock = asyncio.Lock()
        self._conn_lock = asyncio.Lock()

    def cooldown_remaining(self) -> float:
        rem = self._cooldown_until - time.monotonic()
        return rem if rem > 0.0 else 0.0

    def _sender_alive(self) -> bool:
        if self.sender is None:
            return False
        fn = getattr(self.sender, "is_connected", None)
        return bool(fn() if callable(fn) else fn)

    async def ensure_connected(self, force_reconnect: bool = False) -> bool:
        async with self._conn_lock:
            if not force_reconnect and self._sender_alive():
                return False
            if self.sender is not None:
                old_sender = self.sender
                self.sender = None
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(old_sender.disconnect(), timeout=4.0)

            conn_cls = ConnectionTcpAbridged if ConnectionTcpAbridged is not None else self.client._connection

            # ১. সেভ করা AuthKey থাকলে সরাসরি টার্গেট (Media-Only বা Regular) DC5 IP-তে কানেক্ট (ফলব্যাকসহ)
            if self.auth_key is not None:
                cand_eps = [(self.ip, self.port)]
                if (self.auth_ip, self.auth_port) not in cand_eps:
                    cand_eps.append((self.auth_ip, self.auth_port))
                for ep_ip, ep_port in cand_eps:
                    s_try = MTProtoSender(self.auth_key, loggers=self.client._log)
                    setattr(s_try, "_updates_queue", _NULL_UPDATES_QUEUE)
                    try:
                        c = conn_cls(ep_ip, ep_port, self.dc_id, loggers=self.client._log, proxy=self.client._proxy)
                        await asyncio.wait_for(s_try.connect(c), timeout=8.0)
                        _tune_mtproto_socket(c)
                        with contextlib.suppress(Exception):
                            ping_init = copy.copy(self.client._init_request)
                            ping_init.query = PingRequest(ping_id=random.getrandbits(62))
                            await asyncio.wait_for(
                                s_try.send(InvokeWithLayerRequest(TL_LAYER, InvokeWithoutUpdatesRequest(ping_init))),
                                timeout=5.0,
                            )
                        self.ip, self.port = ep_ip, ep_port
                        self.sender = s_try
                        return False
                    except Exception as e:
                        with contextlib.suppress(Exception):
                            await asyncio.wait_for(s_try.disconnect(), timeout=2.0)
                        logger.debug("Worker #%d saved AuthKey connect warning on %s (%s)", self.worker_id, ep_ip, e)
                self.auth_key = None

            # ২. নতুন স্বতন্ত্র 2048-bit DH AuthKey তৈরি + ImportBotAuthorization (Regular RPC IP-তে মিন্ট)
            s = MTProtoSender(None, loggers=self.client._log)
            setattr(s, "_updates_queue", _NULL_UPDATES_QUEUE)
            try:
                c = conn_cls(self.auth_ip, self.auth_port, self.dc_id, loggers=self.client._log, proxy=self.client._proxy)
                await asyncio.wait_for(s.connect(c), timeout=12.0)
                _tune_mtproto_socket(c)
                init_req = copy.copy(self.client._init_request)
                if self.dc_id == self.client.session.dc_id:
                    init_req.query = ImportBotAuthorizationRequest(
                        flags=0,
                        api_id=int(TELEGRAM_API_ID),
                        api_hash=TELEGRAM_API_HASH,
                        bot_auth_token=BOT_TOKEN,
                    )
                else:
                    auth = await self.client(ExportAuthorizationRequest(self.dc_id))
                    init_req.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
                await asyncio.wait_for(
                    s.send(InvokeWithLayerRequest(TL_LAYER, InvokeWithoutUpdatesRequest(init_req))),
                    timeout=12.0,
                )
                self.auth_key = s.auth_key
                self.sender = s
                self.is_independent_key = True
                return True
            except Exception as exp_err:
                logger.warning("Worker #%d independent AuthKey fallback (%s)", self.worker_id, exp_err)
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(s.disconnect(), timeout=4.0)
                base_k = self.client.session.auth_key
                s_fb = MTProtoSender(base_k, loggers=self.client._log)
                setattr(s_fb, "_updates_queue", _NULL_UPDATES_QUEUE)
                c_fb = conn_cls(self.auth_ip, self.auth_port, self.dc_id, loggers=self.client._log, proxy=self.client._proxy)
                await asyncio.wait_for(s_fb.connect(c_fb), timeout=10.0)
                _tune_mtproto_socket(c_fb)
                self.auth_key = base_k
                self.sender = s_fb
                self.is_independent_key = False
                return False

    async def send_part(self, req, cancel_check: Optional[Callable[[], bool]] = None) -> None:
        for attempt in range(4):
            if cancel_check and cancel_check():
                raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")

            async with self._pace_lock:
                now_m = time.monotonic()
                eff_ready = max(self._next_allowed_ts, self._cooldown_until)
                wait_m = eff_ready - now_m
                if self.min_interval > 0.0:
                    self._next_allowed_ts = max(now_m, eff_ready) + self.min_interval
            if wait_m > 0.002:
                if wait_m > 0.35:
                    # এই ওয়ার্কার কুলডাউনে থাকলে চাঙ্ক আটকে না রেখে সাথে সাথে অন্য ফ্রি ওয়ার্কারে দিয়ে দেওয়া!
                    raise _WorkerFloodStealError()
                if cancel_check and cancel_check():
                    raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                await asyncio.sleep(wait_m)

            if not self._sender_alive():
                await self.ensure_connected(force_reconnect=True)

            try:
                assert self.sender is not None
                fut = self.sender.send(req)
                await asyncio.wait_for(fut, timeout=16.0)
                if self.min_interval > 0.003:
                    self.min_interval = max(0.003, self.min_interval * 0.992)
                return
            except (DownloadCancelledError, asyncio.CancelledError):
                raise
            except Exception as e:
                if cancel_check and cancel_check():
                    raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                err_up = str(e).upper()
                flood_sec = int(getattr(e, "seconds", 0) or 0)
                if flood_sec > 0 or "FLOOD" in err_up or "WAIT OF" in err_up:
                    wait_f = max(1.0, float(flood_sec or _extract_flood_seconds(e) or 1)) + random.uniform(0.05, 0.20)
                    self.min_interval = min(0.045, max(0.014, self.min_interval + 0.006))
                    async with self._pace_lock:
                        now_f = time.monotonic()
                        self._cooldown_until = max(self._cooldown_until, now_f + wait_f)
                        self._next_allowed_ts = max(self._next_allowed_ts, self._cooldown_until)
                    # TCP সকেট ডিসকানেক্ট না করে (cwnd অক্ষুণ্ণ রেখে!) চাঙ্কটি ০ms-এ অন্য সুস্থ AuthKey ওয়ার্কারে পাঠানো
                    raise _WorkerFloodStealError() from e
                if "AUTH_KEY_UNREGISTERED" in err_up or "AUTH_KEY_INVALID" in err_up:
                    self.auth_key = None
                    await self.ensure_connected(force_reconnect=True)
                elif not self._sender_alive() or isinstance(e, (ConnectionError, OSError, asyncio.TimeoutError)):
                    await self.ensure_connected(force_reconnect=True)
                if attempt == 3:
                    raise
                await asyncio.sleep(0.15 * (attempt + 1))

    async def ping_if_idle(self) -> None:
        if not self._sender_alive() or self._conn_lock.locked():
            return
        with contextlib.suppress(Exception):
            assert self.sender is not None
            await asyncio.wait_for(self.sender.send(PingRequest(ping_id=random.getrandbits(62))), timeout=6.0)

    async def close(self) -> None:
        if self.sender is not None:
            old_s = self.sender
            self.sender = None
            with contextlib.suppress(Exception):
                await asyncio.wait_for(old_s.disconnect(), timeout=4.0)


class _MTProtoWorkerPool:
    """স্টার্টআপে ১২টি স্বতন্ত্র AuthKey তৈরি ও কানেক্ট করে ২৪/৭ ওয়ার্ম রাখে যাতে আপলোড ০ms এ শুরু হয়।"""

    def __init__(self, client: "TelegramClient", size: int = MTPROTO_POOL_SIZE) -> None:
        self.client = client
        self.dc_id = client.session.dc_id
        self.size = max(4, size)
        self.workers: list[_IndependentMTProtoWorker] = []
        self._ping_task: Optional[asyncio.Task] = None
        self._active_uploads = 0

    def _load_saved_keys(self) -> list[bytes]:
        raw_json = os.environ.get("TELETHON_POOL_KEYS", "").strip()
        if not raw_json and TELETHON_POOL_KEYS_FILE.exists():
            with contextlib.suppress(Exception):
                raw_json = TELETHON_POOL_KEYS_FILE.read_text(encoding="utf-8").strip()
        if not raw_json:
            return []
        out: list[bytes] = []
        with contextlib.suppress(Exception):
            arr = json.loads(raw_json)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, str):
                        b = base64.b64decode(item)
                        if len(b) == 256:
                            out.append(b)
        return out

    def _persist_keys(self) -> None:
        b64_list = [
            base64.b64encode(w.auth_key.key).decode("ascii")
            for w in self.workers
            if w.is_independent_key and w.auth_key is not None and len(w.auth_key.key) == 256
        ]
        if not b64_list:
            return
        serialized = json.dumps(b64_list)
        with contextlib.suppress(OSError):
            TELETHON_POOL_KEYS_FILE.write_text(serialized, encoding="utf-8")
        _sync_cloud_env_var("TELETHON_POOL_KEYS", serialized)

    async def _discover_dc_endpoints(self) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
        regular_eps: list[tuple[str, int]] = []
        media_eps: list[tuple[str, int]] = []
        sess_ip = getattr(self.client.session, "server_address", None)
        sess_port = int(getattr(self.client.session, "port", 443) or 443)
        if sess_ip and ":" not in str(sess_ip):
            regular_eps.append((str(sess_ip), sess_port))
        with contextlib.suppress(Exception):
            cfg = await self.client(GetConfigRequest())
            for opt in (getattr(cfg, "dc_options", None) or []):
                if (
                    getattr(opt, "id", None) == self.dc_id
                    and not getattr(opt, "ipv6", False)
                    and not getattr(opt, "cdn", False)
                    and not getattr(opt, "tcpo_only", False)
                    and getattr(opt, "ip_address", None)
                ):
                    ep = (str(opt.ip_address), int(opt.port or 443))
                    if getattr(opt, "media_only", False):
                        if ep not in media_eps:
                            media_eps.append(ep)
                    else:
                        if ep not in regular_eps:
                            regular_eps.append(ep)
        if not regular_eps:
            dc = await self.client._get_dc(self.dc_id)
            regular_eps.append((dc.ip_address, int(dc.port or 443)))
        # হাই-ব্যান্ডউইথ Media-Only রাউটার + Regular DC5 রাউটার উভয়ের সমন্বিত পুল!
        combined_eps: list[tuple[str, int]] = []
        for ep in [*media_eps, *regular_eps]:
            if ep not in combined_eps:
                combined_eps.append(ep)
        return combined_eps, regular_eps

    async def start(self) -> None:
        all_endpoints, regular_endpoints = await self._discover_dc_endpoints()
        saved_keys = self._load_saved_keys()
        created_fresh = False
        mint_budget = 16  # এক বুটে সর্বোচ্চ ১৬টি নতুন বট-অথ কী মিন্ট (সেভ করা কী-র সাথে মিলিয়ে ৩২টি পূর্ণ!)

        for idx in range(self.size):
            ip, port = all_endpoints[idx % len(all_endpoints)]
            auth_ip, auth_port = regular_endpoints[idx % len(regular_endpoints)]
            sk = saved_keys[idx] if idx < len(saved_keys) else None
            if sk is None and mint_budget <= 0:
                if len(self.workers) >= 8:
                    break
                sk = self.client.session.auth_key.key
            w = _IndependentMTProtoWorker(
                idx,
                self.client,
                self.dc_id,
                ip,
                port,
                saved_key_bytes=sk,
                auth_ip=auth_ip,
                auth_port=auth_port,
            )
            if sk == self.client.session.auth_key.key:
                w.is_independent_key = False
            try:
                is_new = await w.ensure_connected()
                if is_new:
                    created_fresh = True
                    mint_budget -= 1
                    await asyncio.sleep(0.25)
                elif sk is None and not w.is_independent_key:
                    mint_budget = 0
                    if len(self.workers) >= 8:
                        await w.close()
                        break
                self.workers.append(w)
            except Exception as e:
                logger.warning("MTProto pool worker #%d init warning: %s", idx, e)

        if created_fresh:
            asyncio.get_running_loop().run_in_executor(None, self._persist_keys)

        indep_count = sum(1 for w in self.workers if w.is_independent_key)
        logger.info(
            "🔥 MTProto v5 Warm Pool প্রস্তুত: %d/%d ওয়ার্কার সচল (%dটি স্বতন্ত্র AuthKey, DC%d all_endpoints=%s, TcpAbridged=%s)",
            len(self.workers),
            self.size,
            indep_count,
            self.dc_id,
            [f"{ip}:{p}" for ip, p in all_endpoints],
            ConnectionTcpAbridged is not None,
        )
        self._ping_task = asyncio.create_task(self._keepalive_loop())

    async def _keepalive_loop(self) -> None:
        while True:
            await asyncio.sleep(45)
            if self._active_uploads == 0:
                await asyncio.gather(*[w.ping_if_idle() for w in self.workers], return_exceptions=True)

    async def shutdown(self) -> None:
        if self._ping_task is not None:
            self._ping_task.cancel()
            with contextlib.suppress(BaseException):
                await self._ping_task
            self._ping_task = None
        await asyncio.gather(*[w.close() for w in self.workers], return_exceptions=True)
        self.workers.clear()


def _upload_ckpt_path(part: Path) -> Path:
    return part.parent / f"{part.name}.ulckpt.json"


def _load_upload_ckpt(part: Path, file_size: int, part_size: int, part_count: int) -> Optional[dict]:
    ckpt_file = _upload_ckpt_path(part)
    if not ckpt_file.exists():
        return None
    try:
        raw = json.loads(ckpt_file.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return None
        if time.time() - float(raw.get("ts") or 0) > 43200:  # ১২ ঘণ্টা পর্যন্ত Telegram file_id বৈধ থাকে
            return None
        if (
            int(raw.get("file_size") or -1) != file_size
            or int(raw.get("part_size") or -1) != part_size
            or int(raw.get("part_count") or -1) != part_count
            or not raw.get("file_id")
        ):
            return None
        return raw
    except Exception:
        return None


def _save_upload_ckpt_sync(ckpt_file: Path, payload: dict) -> None:
    tmp_file = ckpt_file.with_suffix(".tmp")
    with contextlib.suppress(OSError):
        tmp_file.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        tmp_file.replace(ckpt_file)


class _ParallelMTProtoUploader:
    """লক-ফ্রি ওয়ার্ক-স্টিলিং মাল্টি-AuthKey আপলোডার + চাঙ্ক-লেভেল চেকপয়েন্ট ও অটো-রিজিউম।"""

    def __init__(self, client: "TelegramClient") -> None:
        self.client = client
        self.loop = client.loop

    async def upload_path(
        self,
        path: Path,
        on_progress: Optional[Callable[[int, int], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        persist_checkpoint: bool = True,
    ):
        file_size = path.stat().st_size
        part_size = 512 * 1024  # 512 KB (সর্বোচ্চ MTProto পার্ট সাইজ)
        block_parts = 16
        read_block_size = part_size * block_parts  # এক কলে 8 MB ডিস্ক প্রি-ফেচ + ব্যাকগ্রাউন্ড TL প্যাকিং
        part_count = max(1, (file_size + part_size - 1) // part_size)
        is_big = file_size > 10 * 1024 * 1024
        ckpt_file = _upload_ckpt_path(path)

        saved_ckpt = _load_upload_ckpt(path, file_size, part_size, part_count) if persist_checkpoint else None
        done_parts: set[int] = set()
        saved_md5_hex = ""
        if saved_ckpt is not None:
            file_id = int(saved_ckpt["file_id"])
            saved_md5_hex = str(saved_ckpt.get("md5_hex") or "")
            for pidx in (saved_ckpt.get("done_parts") or []):
                ipidx = int(pidx)
                if 0 <= ipidx < part_count:
                    done_parts.add(ipidx)
            if (saved_ckpt.get("upload_complete") or len(done_parts) >= part_count) and (is_big or saved_md5_hex):
                if on_progress:
                    with contextlib.suppress(Exception):
                        on_progress(file_size, file_size)
                logger.info(
                    "♻️ ১০০%% আপলোড চেকপয়েন্ট পাওয়া গেছে (%s) — ০ বাইট রি-আপলোডে সরাসরি ডেলিভারি হচ্ছে!",
                    path.name,
                )
                if is_big:
                    return InputFileBig(file_id, part_count, path.name)
                return InputFile(file_id, part_count, path.name, saved_md5_hex)
        else:
            file_id = tl_helpers.generate_random_long()

        pool = MTPROTO_WARM_POOL
        if pool is None or not pool.workers:
            raise RuntimeError("MTProto warm pool প্রস্তুত নয়")

        def _part_byte_len(idx: int) -> int:
            if idx < part_count - 1:
                return part_size
            return file_size - (part_count - 1) * part_size

        sent_bytes = sum(_part_byte_len(i) for i in done_parts)
        if done_parts:
            logger.info(
                "♻️ আংশিক আপলোড রিজিউম হচ্ছে (%s): %d/%d চাঙ্ক (%.1f%%) আগে থেকেই সম্পন্ন — বাকি অংশ থেকে শুরু!",
                path.name,
                len(done_parts),
                part_count,
                (sent_bytes / file_size * 100.0) if file_size else 0.0,
            )
            if on_progress:
                with contextlib.suppress(Exception):
                    on_progress(min(sent_bytes, file_size), file_size)

        missing_count = max(1, part_count - len(done_parts))
        active_workers = pool.workers[: min(len(pool.workers), missing_count)]
        slots_per_w = 1 if missing_count <= len(active_workers) else SLOTS_PER_AUTH_KEY
        total_consumers = min(missing_count, len(active_workers) * slots_per_w)

        # প্রতিটি আইটেম (part_idx, prebuilt_tl_req, byte_len, tries) — মেইন থ্রেডে ০ বাইট কপি!
        work_queue: asyncio.Queue[tuple[int, object, int, int]] = asyncio.Queue(maxsize=96)
        retry_deque: collections.deque[tuple[int, object, int, int]] = collections.deque()
        tail_reqs: dict[int, tuple[object, int]] = {}
        done_event = asyncio.Event()
        first_error: Optional[BaseException] = None
        stop_flag = False
        md5_hash = hashlib.md5() if not is_big else None
        last_ckpt_ts = time.monotonic()
        chunks_since_ckpt = 0
        ckpt_writing = False

        def _build_ckpt_payload(complete: bool = False) -> dict:
            return {
                "file_id": file_id,
                "file_size": file_size,
                "part_size": part_size,
                "part_count": part_count,
                "is_big": is_big,
                "md5_hex": (md5_hash.hexdigest() if (complete and md5_hash is not None) else saved_md5_hex),
                "done_parts": sorted(done_parts),
                "upload_complete": complete,
                "ts": time.time(),
            }

        def _maybe_flush_ckpt_async() -> None:
            nonlocal last_ckpt_ts, chunks_since_ckpt, ckpt_writing
            if not persist_checkpoint or ckpt_writing:
                return
            now_m = time.monotonic()
            if chunks_since_ckpt < 64 and (now_m - last_ckpt_ts) < 8.0:
                return
            chunks_since_ckpt = 0
            last_ckpt_ts = now_m
            ckpt_writing = True
            snap = _build_ckpt_payload(complete=False)

            def _write_and_clear() -> None:
                nonlocal ckpt_writing
                try:
                    _save_upload_ckpt_sync(ckpt_file, snap)
                finally:
                    ckpt_writing = False

            self.loop.run_in_executor(None, _write_and_clear)

        def _on_part_done(p_idx: int, n_bytes: int) -> None:
            nonlocal sent_bytes, chunks_since_ckpt
            tail_reqs.pop(p_idx, None)
            if p_idx in done_parts:
                return
            done_parts.add(p_idx)
            sent_bytes += n_bytes
            chunks_since_ckpt += 1
            if on_progress:
                with contextlib.suppress(Exception):
                    on_progress(min(sent_bytes, file_size), file_size)
            if len(done_parts) >= part_count:
                done_event.set()
            else:
                _maybe_flush_ckpt_async()

        async def _consumer(slot_idx: int, worker: _IndependentMTProtoWorker) -> None:
            nonlocal first_error, stop_flag
            if slot_idx > 0:
                await asyncio.sleep(0.0005 * min(slot_idx, 24))
            while not stop_flag and not done_event.is_set():
                if cancel_check and cancel_check():
                    stop_flag = True
                    first_error = DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                    done_event.set()
                    return
                cd_rem = worker.cooldown_remaining()
                if cd_rem > 0.02:
                    await asyncio.sleep(min(cd_rem, 0.4))
                    continue
                if retry_deque:
                    p_idx, pre_req, n_len, tries = retry_deque.popleft()
                else:
                    try:
                        p_idx, pre_req, n_len, tries = await asyncio.wait_for(work_queue.get(), timeout=0.35)
                    except asyncio.TimeoutError:
                        continue
                try:
                    if p_idx in done_parts:
                        continue
                    await worker.send_part(pre_req, cancel_check=cancel_check)
                    _on_part_done(p_idx, n_len)
                except _WorkerFloodStealError:
                    # ০ms বিলম্বে চাঙ্কটি অন্য ফ্রি ওয়ার্কারে হস্তান্তর (চেষ্টা সংখ্যা না বাড়িয়েই!)
                    if p_idx not in done_parts:
                        retry_deque.appendleft((p_idx, pre_req, n_len, tries))
                    await asyncio.sleep(min(max(0.05, worker.cooldown_remaining()), 0.5))
                except BaseException as exc:
                    if isinstance(exc, (DownloadCancelledError, asyncio.CancelledError)) or (cancel_check and cancel_check()):
                        stop_flag = True
                        first_error = DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                        done_event.set()
                        return
                    if tries < 5:
                        logger.warning(
                            "Chunk #%d worker #%d ক্ষণস্থায়ী ত্রুটি (%s) — অন্য সুস্থ ওয়ার্কারে রি-কিউ করা হচ্ছে (%d/5)",
                            p_idx, worker.worker_id, exc, tries + 1,
                        )
                        retry_deque.append((p_idx, pre_req, n_len, tries + 1))
                        if "wait of" not in str(exc).lower():
                            with contextlib.suppress(Exception):
                                await worker.ensure_connected(force_reconnect=True)
                        await asyncio.sleep(0.4)
                        continue
                    stop_flag = True
                    if first_error is None:
                        first_error = exc
                    done_event.set()
                    return

        has_pread = hasattr(os, "pread")
        fd_lock = threading.Lock()

        def _read_and_prepack_block(raw_fd: int, block_start_part: int, span_parts: int, need_parts: list[int], calc_md5: bool):
            byte_offset = block_start_part * part_size
            this_block_bytes = span_parts * part_size
            if has_pread:
                mega = os.pread(raw_fd, this_block_bytes, byte_offset)  # type: ignore[attr-defined]
            else:
                with fd_lock:
                    os.lseek(raw_fd, byte_offset, os.SEEK_SET)
                    mega = os.read(raw_fd, this_block_bytes)
            if not mega:
                return b"", []
            _drop_file_page_cache(raw_fd, byte_offset, len(mega), sync_first=False)
            packed_items: list[tuple[int, object, int]] = []
            if need_parts:
                need_set = set(need_parts)
                mv = memoryview(mega)
                for sub_idx, off in enumerate(range(0, len(mega), part_size)):
                    p_i = block_start_part + sub_idx
                    if p_i not in need_set:
                        continue
                    c_bytes = bytes(mv[off : off + part_size])
                    pre_r = _build_prebuilt_part_req(file_id, p_i, part_count, c_bytes, is_big)
                    packed_items.append((p_i, pre_r, len(c_bytes)))
                mv.release()
            return (mega if calc_md5 else b""), packed_items

        async def _disk_producer(raw_fd: int) -> None:
            nonlocal stop_flag, first_error
            blocks_plan: list[tuple[int, int, list[int]]] = []
            b_cur = 0
            first_span = 4  # প্রথম ২MB ব্লক মাত্র ~১.৫ms-এ প্রি-প্যাক হয়ে তাৎক্ষণিক ওয়্যার স্পিড শুরু করে!
            while b_cur < part_count:
                span = first_span if not blocks_plan else block_parts
                b_end = min(part_count, b_cur + span)
                need_p = [p for p in range(b_cur, b_end) if p not in done_parts]
                if need_p or md5_hash is not None:
                    blocks_plan.append((b_cur, b_end - b_cur, need_p))
                b_cur = b_end

            if not blocks_plan:
                return

            # সবসময় ৩টি ব্লক প্যারালালে প্রি-ফেচ করে রাখা যাতে নেটওয়ার্ক ০ms অপেক্ষা করে!
            pending_futs: collections.deque[asyncio.Future] = collections.deque()
            plan_idx = 0
            while plan_idx < min(3, len(blocks_plan)):
                bs, spn, np_list = blocks_plan[plan_idx]
                pending_futs.append(
                    self.loop.run_in_executor(
                        _UPLOAD_PREPACK_POOL, _read_and_prepack_block, raw_fd, bs, spn, np_list, md5_hash is not None
                    )
                )
                plan_idx += 1

            while pending_futs and not stop_flag and not done_event.is_set():
                if cancel_check and cancel_check():
                    stop_flag = True
                    if first_error is None:
                        first_error = DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                    done_event.set()
                    break
                cur_fut = pending_futs.popleft()
                if plan_idx < len(blocks_plan):
                    bs, spn, np_list = blocks_plan[plan_idx]
                    pending_futs.append(
                        self.loop.run_in_executor(
                            _UPLOAD_PREPACK_POOL, _read_and_prepack_block, raw_fd, bs, spn, np_list, md5_hash is not None
                        )
                    )
                    plan_idx += 1

                raw_for_md5, packed_list = await cur_fut
                if md5_hash is not None and raw_for_md5:
                    md5_hash.update(raw_for_md5)
                del raw_for_md5

                for p_idx, pre_req, n_len in packed_list:
                    if p_idx in done_parts:
                        continue
                    if stop_flag or done_event.is_set() or (cancel_check and cancel_check()):
                        if cancel_check and cancel_check() and first_error is None:
                            first_error = DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                        stop_flag = True
                        done_event.set()
                        break
                    if p_idx >= part_count - 16:
                        tail_reqs[p_idx] = (pre_req, n_len)
                    while not stop_flag and not done_event.is_set():
                        try:
                            await asyncio.wait_for(work_queue.put((p_idx, pre_req, n_len, 0)), timeout=0.35)
                            break
                        except asyncio.TimeoutError:
                            if cancel_check and cancel_check():
                                stop_flag = True
                                first_error = DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                                done_event.set()
                                break
                            if consumers and all(t.done() for t in consumers):
                                stop_flag = True
                                if first_error is None:
                                    first_error = RuntimeError("আপলোড ওয়ার্কারগুলো থেমে গেছে")
                                done_event.set()
                                break

        pool._active_uploads += 1
        consumers: list[asyncio.Task] = []
        producer_task: Optional[asyncio.Task] = None
        raw_fd_opt: Optional[int] = None
        try:
            if len(done_parts) >= part_count:
                done_event.set()
            else:
                raw_fd_opt = os.open(str(path), os.O_RDONLY | getattr(os, "O_BINARY", 0))
                producer_task = self.loop.create_task(_disk_producer(raw_fd_opt))
                for c_idx in range(total_consumers):
                    w = active_workers[c_idx % len(active_workers)]
                    consumers.append(self.loop.create_task(_consumer(c_idx, w)))

                await producer_task

                # অ্যান্টি-হ্যাং সুপারভাইজার + ৪৫০ms আগ্রাসী হেজড টেইল স্পেকুলেশন (৯৯% শেষে ০ সেকেন্ড স্লো-সকেট ড্র্যাগ!)
                last_done_cnt = len(done_parts)
                last_prog_m = time.monotonic()
                hedged_once: set[int] = set()
                while not done_event.is_set() and not stop_flag:
                    if cancel_check and cancel_check():
                        stop_flag = True
                        first_error = DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                        done_event.set()
                        break
                    if consumers and all(t.done() for t in consumers):
                        if len(done_parts) < part_count and first_error is None:
                            first_error = RuntimeError("সব MTProto আপলোড ওয়ার্কার অপ্রত্যাশিতভাবে বন্ধ হয়েছে")
                        break
                    try:
                        await asyncio.wait_for(done_event.wait(), timeout=0.30)
                        break
                    except asyncio.TimeoutError:
                        cur_done = len(done_parts)
                        now_m = time.monotonic()
                        if cur_done > last_done_cnt:
                            last_done_cnt = cur_done
                            last_prog_m = now_m
                        else:
                            rem_parts = part_count - cur_done
                            idle_sec = now_m - last_prog_m
                            if 0 < rem_parts <= 16 and idle_sec >= 0.30:
                                for miss_p, (miss_req, miss_len) in list(tail_reqs.items()):
                                    if miss_p not in done_parts and (miss_p not in hedged_once or idle_sec >= 1.5):
                                        hedged_once.add(miss_p)
                                        retry_deque.appendleft((miss_p, miss_req, miss_len, 1))
                                if idle_sec >= 1.5:
                                    last_prog_m = now_m
                            elif idle_sec >= 45.0:
                                stop_flag = True
                                if first_error is None:
                                    first_error = RuntimeError("আপলোড ৪৫ সেকেন্ড স্থবির থাকায় চেকপয়েন্টসহ রিস্টার্ট হচ্ছে")
                                break
                tail_reqs.clear()

            for c_task in consumers:
                if not c_task.done():
                    c_task.cancel()
            if consumers:
                await asyncio.gather(*consumers, return_exceptions=True)

            if first_error is not None:
                raise first_error
        finally:
            pool._active_uploads = max(0, pool._active_uploads - 1)
            if producer_task is not None and not producer_task.done():
                producer_task.cancel()
                with contextlib.suppress(BaseException):
                    await producer_task
            for c_task in consumers:
                if not c_task.done():
                    c_task.cancel()
            if raw_fd_opt is not None:
                with contextlib.suppress(OSError):
                    os.close(raw_fd_opt)
            if persist_checkpoint and done_parts and not (cancel_check and cancel_check()):
                is_full = len(done_parts) >= part_count
                await self.loop.run_in_executor(None, _save_upload_ckpt_sync, ckpt_file, _build_ckpt_payload(complete=is_full))
            _trim_process_memory()

        final_md5 = md5_hash.hexdigest() if md5_hash is not None else saved_md5_hex
        if is_big:
            return InputFileBig(file_id, part_count, path.name)
        return InputFile(file_id, part_count, path.name, final_md5)


def _sync_telethon_session_to_cloud(sess_str: str) -> None:
    if sess_str:
        _sync_cloud_env_var("TELETHON_SESSION", sess_str)


async def _run_dc_upload_benchmark() -> None:
    """স্টার্টআপে ৯৬MB সাসটেইনড টেস্ট বাফার সরাসরি Telegram DC5-এ পাঠিয়ে আসল লাইভ আপলোড স্পিড (MB/s) লগে প্রিন্ট করে।"""
    await asyncio.sleep(2)
    if TELETHON_CLIENT is None or MTPROTO_WARM_POOL is None or not MTPROTO_WARM_POOL.workers:
        return
    bench_path = DOWNLOAD_DIR / "_startup_ul_bench.bin"
    try:
        bench_mb = 96
        bench_path.write_bytes(b"\x5a" * (bench_mb * 1024 * 1024))
        t0 = time.perf_counter()
        uploader = _ParallelMTProtoUploader(TELETHON_CLIENT)
        await uploader.upload_path(bench_path, persist_checkpoint=False)
        dt = max(0.05, time.perf_counter() - t0)
        indep = sum(1 for w in MTPROTO_WARM_POOL.workers if w.is_independent_key)
        logger.info(
            "🚀 [UPLOAD BENCHMARK] %d MB uploaded to Telegram DC%d in %.2fs -> %.1f MB/s! (independent_auth_keys=%d)",
            bench_mb, MTPROTO_WARM_POOL.dc_id, dt, bench_mb / dt, indep,
        )
    except Exception as e:
        logger.warning("Upload benchmark warning: %s", e)
    finally:
        bench_path.unlink(missing_ok=True)


def _raw_key_to_string_session(key_bytes: bytes, dc_id: int = 5, ip: str = "91.108.56.133", port: int = 443) -> str:
    packed = struct.pack(">B4sH256s", dc_id, socket.inet_aton(ip), port, key_bytes)
    return "1" + base64.urlsafe_b64encode(packed).decode("ascii")


def _load_raw_pool_keys_static() -> list[bytes]:
    raw_json = os.environ.get("TELETHON_POOL_KEYS", "").strip()
    if not raw_json and TELETHON_POOL_KEYS_FILE.exists():
        with contextlib.suppress(Exception):
            raw_json = TELETHON_POOL_KEYS_FILE.read_text(encoding="utf-8").strip()
    if not raw_json:
        return []
    out: list[bytes] = []
    with contextlib.suppress(Exception):
        arr = json.loads(raw_json)
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, str):
                    b = base64.b64decode(item)
                    if len(b) == 256:
                        out.append(b)
    return out


async def _init_telethon_client(app: Application) -> None:
    global TELETHON_CLIENT, MTPROTO_WARM_POOL
    if not MTPROTO_UPLOAD_ENABLED:
        return
    saved_sess = TELETHON_SESSION_ENV
    if not saved_sess and TELETHON_SESSION_FILE.exists():
        with contextlib.suppress(Exception):
            saved_sess = TELETHON_SESSION_FILE.read_text(encoding="utf-8").strip()

    pool_keys = _load_raw_pool_keys_static()
    # ক্যান্ডিডেট সেশন তালিকা: ১) সেভ করা মেইন সেশন, ২) পুলের সিঙ্গাপুর-অনলি প্রি-অথরাইজড কীগুলো (জিরো ImportBotAuth কোটা!), ৩) ফ্রেশ লগইন
    sess_candidates: list[tuple[str, str, int]] = []
    if saved_sess:
        sess_candidates.append(("saved", saved_sess, -1))
    for k_idx, kb in enumerate(pool_keys):
        sess_candidates.append((f"pool#{k_idx}", _raw_key_to_string_session(kb), k_idx))
    sess_candidates.append(("fresh", "", -1))

    conn_cls = ConnectionTcpAbridged if ConnectionTcpAbridged is not None else None
    for label, sess_arg, promoted_idx in sess_candidates:
        client: Optional[TelegramClient] = None
        try:
            client_kwargs: dict = {"flood_sleep_threshold": 60, "receive_updates": False}
            if conn_cls is not None:
                client_kwargs["connection"] = conn_cls
            client = TelegramClient(
                StringSession(sess_arg),
                int(TELEGRAM_API_ID),
                TELEGRAM_API_HASH,
                **client_kwargs,
            )
            if sess_arg:
                await client.connect()
                if not await client.is_user_authorized():
                    await client.start(bot_token=BOT_TOKEN)
            else:
                await client.start(bot_token=BOT_TOKEN)

            TELETHON_CLIENT = client
            new_sess = client.session.save()
            if new_sess:
                with contextlib.suppress(OSError):
                    TELETHON_SESSION_FILE.write_text(new_sess, encoding="utf-8")
                if new_sess != saved_sess:
                    asyncio.get_running_loop().run_in_executor(None, _sync_telethon_session_to_cloud, new_sess)

            # যদি পুলের কোনো কী মেইন ক্লায়েন্টে প্রমোট হয়ে থাকে, পুল থেকে সেটি বাদ দিয়ে বাকি স্বতন্ত্র কীগুলো পুলে রাখা
            if promoted_idx >= 0:
                rem_b64 = [base64.b64encode(k).decode("ascii") for i, k in enumerate(pool_keys) if i != promoted_idx]
                os.environ["TELETHON_POOL_KEYS"] = json.dumps(rem_b64)
                with contextlib.suppress(OSError):
                    TELETHON_POOL_KEYS_FILE.write_text(os.environ["TELETHON_POOL_KEYS"], encoding="utf-8")

            MTPROTO_WARM_POOL = _MTProtoWorkerPool(client, size=MTPROTO_POOL_SIZE)
            await MTPROTO_WARM_POOL.start()
            logger.info(
                "⚡ FastTelethon v3 Multi-AuthKey আপলোডার চালু হয়েছে! (src=%s, dc=%s, auth_keys=%d, slots=%dx%d, cryptg=%s, uvloop=%s)",
                label,
                getattr(client.session, "dc_id", "?"),
                len(MTPROTO_WARM_POOL.workers),
                len(MTPROTO_WARM_POOL.workers),
                SLOTS_PER_AUTH_KEY,
                "enabled" if CRYPTG_AVAILABLE else "fallback",
                "enabled" if UVLOOP_AVAILABLE else "off",
            )
            if os.environ.get("RUN_UPLOAD_BENCH", "0") == "1":
                asyncio.create_task(_run_dc_upload_benchmark())
            return
        except Exception as e:
            logger.warning("FastTelethon init attempt (%s) সতর্কতা: %s", label, e)
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.disconnect()


async def _shutdown_telethon_client(app: Application) -> None:
    global TELETHON_CLIENT, MTPROTO_WARM_POOL
    _TELETHON_PEER_CACHE.clear()
    if MTPROTO_WARM_POOL is not None:
        with contextlib.suppress(Exception):
            await MTPROTO_WARM_POOL.shutdown()
        MTPROTO_WARM_POOL = None
    if TELETHON_CLIENT is not None:
        with contextlib.suppress(Exception):
            await TELETHON_CLIENT.disconnect()
        TELETHON_CLIENT = None


async def _resolve_telethon_peer(client: "TelegramClient", chat_id: int, status_message_id: int, refresh: bool = False):
    if not refresh and chat_id in _TELETHON_PEER_CACHE:
        return _TELETHON_PEER_CACHE[chat_id]
    try:
        peer = await client.get_input_entity(chat_id)
    except ValueError:
        if chat_id > 0 and status_message_id > 0:
            with contextlib.suppress(Exception):
                await client(GetMessagesRequest(id=[InputMessageID(id=status_message_id)]))
                peer = await client.get_input_entity(chat_id)
            if "peer" in locals():
                if len(_TELETHON_PEER_CACHE) >= 128:
                    _TELETHON_PEER_CACHE.pop(next(iter(_TELETHON_PEER_CACHE)), None)
                _TELETHON_PEER_CACHE[chat_id] = peer
                return peer
        raise
    if len(_TELETHON_PEER_CACHE) >= 128:
        _TELETHON_PEER_CACHE.pop(next(iter(_TELETHON_PEER_CACHE)), None)
    _TELETHON_PEER_CACHE[chat_id] = peer
    return peer


async def _upload_media_fast_telethon(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    status_message_id: int,
    mode: str,
    part: Path,
    caption: Optional[str],
    label: str,
    loop: asyncio.AbstractEventLoop,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
    reply_to_message_id: Optional[int] = None,
) -> None:
    assert TELETHON_CLIENT is not None
    if cancel_check and cancel_check():
        raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
    entity = await _resolve_telethon_peer(TELETHON_CLIENT, chat_id, status_message_id)
    total_size = part.stat().st_size
    tracker = ProgressTracker(
        loop,
        context.bot,
        chat_id,
        status_message_id,
        f"⚡ {label}",
        reply_markup=reply_markup,
        cancel_check=cancel_check,
    )
    start_t = time.time()

    def _on_prog(done_b: int, total_b: int) -> None:
        elapsed = max(0.1, time.time() - start_t)
        spd = done_b / elapsed
        rem_eta = ((total_b - done_b) / spd) if spd > 0 else None
        tracker.report(done_b, total_b, speed=spd, eta=rem_eta)

    thumb_path: Optional[Path] = None
    try:
        uploader = _ParallelMTProtoUploader(TELETHON_CLIENT)
        if mode != "audio":
            # প্যারালাল এক্সিকিউশন: নেটওয়ার্ক আপলোড চলার সাথে সাথেই ffprobe ও থাম্বনেইল তৈরি হয়ে যাবে (০ সেকেন্ড বাড়তি সময়!)
            meta_fut = loop.run_in_executor(None, _ffprobe_video_meta, part)
            thumb_fut = loop.run_in_executor(None, _extract_video_thumb_sync, part)
            input_file, (w, h, dur), thumb_path = await asyncio.gather(
                uploader.upload_path(part, on_progress=_on_prog, cancel_check=cancel_check),
                meta_fut,
                thumb_fut,
            )
        else:
            w, h = 0, 0

            async def _safe_dur() -> int:
                with contextlib.suppress(Exception):
                    return int(await loop.run_in_executor(None, _ffprobe_duration_seconds, part))
                return 0

            input_file, dur = await asyncio.gather(
                uploader.upload_path(part, on_progress=_on_prog, cancel_check=cancel_check),
                _safe_dur(),
            )

        if cancel_check and cancel_check():
            raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")

        # SendMediaRequest-এ চ্যাট ফ্লাড-ওয়েট বা সাময়িক নেটওয়ার্ক বিঘ্ন এলে ১০০% আপলোডকৃত input_file কখনোই ফেলে না দিয়ে
        # ঠিক ওই অবস্থান থেকেই ০ বাইট রি-আপলোডে ডেলিভারি সম্পন্ন করা!
        eff_reply_to = int(reply_to_message_id) if (reply_to_message_id and int(reply_to_message_id) > 0) else None
        for send_attempt in range(1, 7):
            if cancel_check and cancel_check():
                raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
            try:
                if not TELETHON_CLIENT.is_connected():
                    await TELETHON_CLIENT.connect()
                if mode == "audio":
                    attrs = [DocumentAttributeAudio(duration=dur, title=part.stem, performer="")]
                    await TELETHON_CLIENT.send_file(
                        entity,
                        file=input_file,
                        caption=caption or "",
                        attributes=attrs,
                        force_document=False,
                        reply_to=eff_reply_to,
                    )
                else:
                    attrs = [DocumentAttributeVideo(duration=dur, w=w, h=h, supports_streaming=True)]
                    await TELETHON_CLIENT.send_file(
                        entity,
                        file=input_file,
                        thumb=str(thumb_path) if thumb_path else None,
                        caption=caption or "",
                        attributes=attrs,
                        supports_streaming=True,
                        force_document=False,
                        reply_to=eff_reply_to,
                    )
                break
            except (DownloadCancelledError, asyncio.CancelledError):
                raise
            except Exception as send_err:
                if eff_reply_to is not None and "MESSAGE_ID_INVALID" in str(send_err).upper():
                    eff_reply_to = None
                    continue
                flood_sec = _extract_flood_seconds(send_err)
                if flood_sec > 0:
                    _note_chat_flood(chat_id, flood_sec)
                    wait_total = flood_sec + 2
                    logger.warning(
                        "⏳ SendMediaRequest এ Telegram চ্যাট ফ্লাড-ওয়েট (%ds) — ১০০%% আপলোডকৃত input_file সংরক্ষিত আছে! "
                        "%ds পর ০ বাইট রি-আপলোডে তাৎক্ষণিক পাঠানো হবে (attempt %d/6)",
                        flood_sec, wait_total, send_attempt,
                    )
                    for _ in range(wait_total):
                        if cancel_check and cancel_check():
                            raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                        await asyncio.sleep(1.0)
                    entity = await _resolve_telethon_peer(TELETHON_CLIENT, chat_id, status_message_id, refresh=True)
                    continue
                if send_attempt < 6:
                    logger.warning(
                        "SendMediaRequest ক্ষণস্থায়ী ত্রুটি (%s) — %ds পর পুনরায় ডেলিভারি চেষ্টা (%d/6)",
                        send_err, send_attempt * 2, send_attempt,
                    )
                    for _ in range(send_attempt * 2):
                        if cancel_check and cancel_check():
                            raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                        await asyncio.sleep(1.0)
                    with contextlib.suppress(Exception):
                        if not TELETHON_CLIENT.is_connected():
                            await TELETHON_CLIENT.connect()
                        entity = await _resolve_telethon_peer(TELETHON_CLIENT, chat_id, status_message_id, refresh=True)
                    continue
                raise

        _upload_ckpt_path(part).unlink(missing_ok=True)
        elapsed_total = max(0.1, time.time() - start_t)
        mb = total_size / (1024 * 1024)
        logger.info("⚡ FastTelethon আপলোড সম্পন্ন: %.1f MB in %.1fs (%.1f MB/s)", mb, elapsed_total, mb / elapsed_total)
    finally:
        tracker.close()
        if thumb_path is not None:
            thumb_path.unlink(missing_ok=True)


async def _send_media_with_retry(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    status_message_id: int,
    mode: str,
    part: Path,
    caption: Optional[str],
    label: str,
    loop: asyncio.AbstractEventLoop,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
    reply_to_message_id: Optional[int] = None,
) -> None:
    """
    প্রথমে মাল্টি-AuthKey প্যারালাল FastTelethon MTProto দিয়ে চাঙ্ক-লেভেল চেকপয়েন্ট ও রিজিউমসহ পাঠায়;
    সেটি অনুপলব্ধ থাকলে স্ট্যান্ডার্ড Telethon / Local Bot API / Cloud Bot API (<=50MB) তে ফলব্যাক করে।
    """
    if cancel_check and cancel_check():
        raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")

    if TELETHON_CLIENT is not None:
        if not TELETHON_CLIENT.is_connected():
            with contextlib.suppress(Exception):
                await TELETHON_CLIENT.connect()
        if TELETHON_CLIENT.is_connected():
            try:
                await _upload_media_fast_telethon(
                    context, chat_id, status_message_id, mode, part, caption, label, loop,
                    reply_markup=reply_markup, cancel_check=cancel_check,
                    reply_to_message_id=reply_to_message_id,
                )
                return
            except (DownloadCancelledError, asyncio.CancelledError):
                raise
            except Exception as ft_err:
                if cancel_check and cancel_check():
                    raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                logger.warning("FastTelethon প্যারালাল আপলোড ফলব্যাক (%s) — স্ট্যান্ডার্ড Telethon MTProto দিয়ে চেষ্টা হচ্ছে...", ft_err)
                try:
                    entity = await _resolve_telethon_peer(TELETHON_CLIENT, chat_id, status_message_id)
                    total_sz = part.stat().st_size
                    part_sz = 512 * 1024
                    part_cnt = max(1, (total_sz + part_sz - 1) // part_sz)
                    ckpt = _load_upload_ckpt(part, total_sz, part_sz, part_cnt)
                    reuse_input = None
                    if ckpt and ckpt.get("upload_complete") and (total_sz > 10 * 1024 * 1024 or ckpt.get("md5_hex")):
                        reuse_input = (
                            InputFileBig(int(ckpt["file_id"]), part_cnt, part.name)
                            if total_sz > 10 * 1024 * 1024
                            else InputFile(int(ckpt["file_id"]), part_cnt, part.name, str(ckpt.get("md5_hex") or ""))
                        )
                    fb_tracker = ProgressTracker(
                        loop, context.bot, chat_id, status_message_id, label, reply_markup=reply_markup, cancel_check=cancel_check
                    )
                    t0 = time.time()

                    def _tl_prog(sent: int, tot: int) -> None:
                        if cancel_check and cancel_check():
                            raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                        el = max(0.1, time.time() - t0)
                        sp = sent / el
                        fb_tracker.report(sent, tot, speed=sp, eta=((tot - sent) / sp) if sp > 0 else None)

                    eff_rep = int(reply_to_message_id) if (reply_to_message_id and int(reply_to_message_id) > 0) else None
                    try:
                        for std_try in range(1, 4):
                            try:
                                await TELETHON_CLIENT.send_file(
                                    entity,
                                    file=reuse_input if reuse_input is not None else str(part),
                                    caption=caption or "",
                                    supports_streaming=(mode != "audio"),
                                    force_document=False,
                                    progress_callback=None if reuse_input is not None else _tl_prog,
                                    reply_to=eff_rep,
                                )
                                _upload_ckpt_path(part).unlink(missing_ok=True)
                                logger.info("✅ Telethon স্ট্যান্ডার্ড MTProto আপলোড সম্পন্ন (%.1f MB)", total_sz / (1024 * 1024))
                                return
                            except Exception as inner_tl_err:
                                if eff_rep is not None and "MESSAGE_ID_INVALID" in str(inner_tl_err).upper():
                                    eff_rep = None
                                    continue
                                f_sec = _extract_flood_seconds(inner_tl_err)
                                if f_sec > 0 and std_try < 3:
                                    _note_chat_flood(chat_id, f_sec)
                                    for _ in range(f_sec + 2):
                                        if cancel_check and cancel_check():
                                            raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
                                        await asyncio.sleep(1.0)
                                    continue
                                raise
                    finally:
                        fb_tracker.close()
                except (DownloadCancelledError, asyncio.CancelledError):
                    raise
                except Exception as tl_std_err:
                    logger.warning("Telethon স্ট্যান্ডার্ড ফলব্যাক ব্যর্থ (%s)", tl_std_err)
                    if not LOCAL_API_ENABLED and part.stat().st_size > 49 * 1024 * 1024:
                        raise RuntimeError(f"MTProto বড় ফাইল আপলোড সাময়িকভাবে ব্যর্থ হয়েছে ({tl_std_err})") from tl_std_err

    if not LOCAL_API_ENABLED and part.stat().st_size > 49 * 1024 * 1024:
        raise RuntimeError("৫০MB-এর বড় ফাইল ক্লাউড Bot API দিয়ে পাঠানো সম্ভব নয় (MTProto সংযোগ অনুপলব্ধ)।")

    last_error: Optional[Exception] = None
    for attempt in range(1, SEND_MAX_ATTEMPTS + 1):
        if cancel_check and cancel_check():
            raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
        ticker_task = None
        reader = None
        cloud_tracker: Optional[ProgressTracker] = None
        try:
            r_params = _reply_params(reply_to_message_id)
            if LOCAL_API_ENABLED:
                ticker_task = await _run_indeterminate_ticker(
                    context, chat_id, status_message_id, label, reply_markup=reply_markup, cancel_check=cancel_check
                )
                if mode == "audio":
                    await context.bot.send_audio(chat_id, audio=str(part), caption=caption, reply_parameters=r_params)
                else:
                    await context.bot.send_video(chat_id, video=str(part), supports_streaming=True, caption=caption, reply_parameters=r_params)
            else:
                total_size = part.stat().st_size
                cloud_tracker = ProgressTracker(
                    loop, context.bot, chat_id, status_message_id, label, reply_markup=reply_markup, cancel_check=cancel_check
                )
                reader = _ProgressFile(part, total_size, cloud_tracker.report, cancel_check=cancel_check)
                if mode == "audio":
                    await context.bot.send_audio(chat_id, audio=reader, filename=part.name, caption=caption, reply_parameters=r_params)
                else:
                    await context.bot.send_video(
                        chat_id, video=reader, filename=part.name, supports_streaming=True, caption=caption, reply_parameters=r_params
                    )
            return
        except (DownloadCancelledError, asyncio.CancelledError):
            raise
        except RetryAfter as e:
            last_error = e
            wait = float(getattr(e, "retry_after", 3)) + 1
            logger.warning("Telegram flood control: %.1fs অপেক্ষা করে retry (%d/%d)", wait, attempt, SEND_MAX_ATTEMPTS)
            await asyncio.sleep(wait)
        except (TimedOut, NetworkError) as e:
            last_error = e
            if attempt == SEND_MAX_ATTEMPTS:
                break
            wait = 2 ** attempt
            logger.warning("নেটওয়ার্ক সমস্যা (%s), %ds পর retry (%d/%d)", e, wait, attempt, SEND_MAX_ATTEMPTS)
            await asyncio.sleep(wait)
        finally:
            if cloud_tracker is not None:
                cloud_tracker.close()
            if ticker_task is not None:
                await _cancel_task(ticker_task)
            if reader is not None:
                reader.close()

    raise last_error or RuntimeError("পাঠানো ব্যর্থ হয়েছে (অজানা কারণ)")


async def _deliver_parts(
    context: object,
    chat_id: int,
    status_message_id: int,
    parts: list[Path],
    mode: str,
    title: Optional[str],
    job: Optional[DownloadJob] = None,
) -> None:
    total = len(parts)
    loop = asyncio.get_running_loop()
    cancel_check = job.is_cancelled if job else None
    cancel_markup = _cancel_kb(job.job_id) if job else None
    tag = job.item_tag if job else ""
    src_msg_id = job.source_message_id if job else None
    delivered_ok = 0

    for idx, part in enumerate(parts, start=1):
        if cancel_check and cancel_check():
            raise DownloadCancelledError("আপলোড বাতিল করা হয়েছে")
        if not part.exists():
            continue
        try:
            if part.stat().st_size > MAX_TELEGRAM_FILE_SIZE:
                try:
                    part = await loop.run_in_executor(None, _compress_video_sync, part, COMPRESS_TARGET_SIZE)
                except Exception as e:
                    logger.warning("part %d compress failed: %s", idx, e)
                    await context.bot.send_message(
                        chat_id,
                        f"Part {idx}/{total} পাঠানো সম্ভব হয়নি (সাইজ সমস্যা)।",
                        reply_parameters=_reply_params(src_msg_id),
                    )
                    part.unlink(missing_ok=True)
                    continue

            caption = _build_caption(title, idx, total)
            label = f"{tag}⬆️ আপলোড হচ্ছে{f' (Part {idx}/{total})' if total > 1 else ''}"
            await _send_media_with_retry(
                context, chat_id, status_message_id, mode, part, caption, label, loop,
                reply_markup=cancel_markup, cancel_check=cancel_check,
                reply_to_message_id=src_msg_id,
            )
            delivered_ok += 1
            # সফলভাবে পাঠানো শেষ হলেই কেবল ফাইল ও চেকপয়েন্ট মোছা হয় (যাতে বিঘ্ন ঘটলে ঠিক সেখান থেকেই রিজিউম হতে পারে)
            _upload_ckpt_path(part).unlink(missing_ok=True)
            part.unlink(missing_ok=True)
        except (DownloadCancelledError, asyncio.CancelledError):
            _upload_ckpt_path(part).unlink(missing_ok=True)
            part.unlink(missing_ok=True)
            raise
        except TelegramError as e:
            logger.warning("telegram send failed (part %d/%d): %s", idx, total, e)
            await context.bot.send_message(
                chat_id,
                f"Part {idx}/{total} পাঠাতে সমস্যা হয়েছে:\n{e}",
                reply_parameters=_reply_params(src_msg_id),
            )
            _upload_ckpt_path(part).unlink(missing_ok=True)
            part.unlink(missing_ok=True)

    if delivered_ok > 0:
        if job is not None:
            _record_completed_download(chat_id, job.url, title, mode, job.quality)
        with contextlib.suppress(TelegramError):
            await context.bot.delete_message(chat_id=chat_id, message_id=status_message_id)

    if total > 1 and delivered_ok > 0:
        await context.bot.send_message(
            chat_id,
            f"✅ সবগুলো পার্ট ({total}টি) পাঠানো হয়ে গেছে।",
            reply_parameters=_reply_params(src_msg_id),
        )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception", exc_info=context.error)


async def cookies_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if OWNER_ID and user and str(user.id) != str(OWNER_ID):
        await update.message.reply_text("এই কমান্ডটি কেবল বট ওনারের জন্য নির্ধারিত।")
        return

    global ACTIVE_COOKIES_FILE, YOUTUBE_COOKIES_EXPIRED
    args = context.args or []
    if args and args[0].strip().lower() in ("renew", "refresh"):
        ok = await asyncio.get_running_loop().run_in_executor(None, _try_renew_aylo_tokens, True)
        if ok and ACTIVE_COOKIES_FILE and os.path.exists(ACTIVE_COOKIES_FILE):
            raw_now = Path(ACTIVE_COOKIES_FILE).read_text(encoding="utf-8", errors="ignore")
            await update.message.reply_html(f"✅ <b>মেম্বার সেশন টোকেন তাৎক্ষণিক রিনিউ হয়েছে!</b>{_aylo_cookies_status_summary(raw_now)}")
        else:
            await update.message.reply_html(
                "⚠️ রিনিউ করার মতো সক্রিয় <code>refresh_token_ma</code> পাওয়া যায়নি। "
                "একবার ব্রাউজারের Incognito উইন্ডো থেকে নতুন কুকিজ পাঠালে এরপর ২৪/৭ অটো-রিনিউ চলবে।"
            )
        return
    if args and args[0].strip().lower() in ("clear", "reset", "delete"):
        # নির্দিষ্ট একটি ডোমেইনের কুকি মুছতে: /cookies clear spicevids.com
        if len(args) >= 2 and ACTIVE_COOKIES_FILE and os.path.exists(ACTIVE_COOKIES_FILE):
            target_del = _host_base_domain(args[1].strip())
            cur_raw = Path(ACTIVE_COOKIES_FILE).read_text(encoding="utf-8", errors="ignore")
            kept = [c for c in _parse_netscape_cookies(cur_raw) if _host_base_domain(c["domain"]) != target_del]
            if kept:
                new_serialized = _serialize_netscape_cookies(kept)
                await asyncio.get_running_loop().run_in_executor(None, _save_and_sync_cookies, new_serialized, True)
                await update.message.reply_html(f"🗑️ শুধুমাত্র <code>{html.escape(target_del)}</code> ডোমেইনের কুকিজ মুছে ফেলা হয়েছে।")
                return
        for p in (Path("cookies.txt"), COOKIES_RUNTIME_PATH):
            p.unlink(missing_ok=True)
        ACTIVE_COOKIES_FILE = None
        YOUTUBE_COOKIES_EXPIRED = False
        await asyncio.get_running_loop().run_in_executor(None, _sync_cloud_env_var, "COOKIES_DATA", "")
        await update.message.reply_html("🗑️ সব সংরক্ষিত কুকিজ মুছে ফেলা হয়েছে।")
        return

    has_cookies = bool(ACTIVE_COOKIES_FILE and os.path.exists(ACTIVE_COOKIES_FILE) and os.path.getsize(ACTIVE_COOKIES_FILE) > 0)
    size = os.path.getsize(ACTIVE_COOKIES_FILE) if has_cookies else 0
    per_domain_block = ""
    if has_cookies:
        raw_txt = Path(ACTIVE_COOKIES_FILE).read_text(encoding="utf-8", errors="ignore")
        d_lines = _per_domain_cookie_status_lines(raw_txt)
        if d_lines:
            per_domain_block = "\n📊 <b>ডোমেইনভিত্তিক মেয়াদ ও স্ট্যাটাস:</b>\n" + "\n".join(d_lines) + "\n"
    text = (
        f"🍪 <b>কুকিজ স্ট্যাটাস:</b> {'✅ সক্রিয়' if has_cookies else '❌ নিষ্ক্রিয়'}\n"
        f"📁 সাইজ: {size} বাইট\n"
        f"{per_domain_block}\n"
        f"💡 <b>কমান্ড ও নিয়ম:</b>\n"
        f"• যেকোনো সাইটের Netscape কুকিজ চ্যাটে পেস্ট বা <code>.txt</code> ফাইলে পাঠালে ঠিক সেই ডোমেইনের জন্যই সেট হবে এবং মেয়াদ দেখাবে।\n"
        f"• টোকেন তাৎক্ষণিক রিনিউ করতে: <code>/cookies renew</code>\n"
        f"• নির্দিষ্ট ডোমেইন মুছতে: <code>/cookies clear domain.com</code> | সব মুছতে: <code>/cookies clear</code>"
    )
    await update.message.reply_html(text)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    if OWNER_ID and str(user.id) != str(OWNER_ID):
        await update.message.reply_text("ফাইল পাঠানো সমর্থিত নয়। দয়া করে ভিডিওর লিংক পাঠান।")
        return

    doc = update.message.document
    if not doc:
        return

    file_name = (doc.file_name or "").lower()
    if not file_name.endswith(".txt"):
        await update.message.reply_text("দয়া করে একটি .txt কুকিজ ফাইল পাঠান (যেমন <code>cookies.txt</code>)।", parse_mode="HTML")
        return

    status_msg = await context.bot.send_message(update.effective_chat.id, "কুকিজ ফাইল প্রসেস ও যাচাই করা হচ্ছে... ⏳")
    try:
        tg_file = await doc.get_file()
        if tg_file.file_path and os.path.exists(tg_file.file_path):
            content_bytes = Path(tg_file.file_path).read_bytes()
        else:
            content_bytes = bytes(await tg_file.download_as_bytearray())
        content = content_bytes.decode("utf-8", errors="ignore").strip()

        if not _is_netscape_cookie_text(content):
            await _safe_status_edit(
                status_msg,
                "⚠️ সতর্কবার্তা: এটি সঠিক Netscape কুকিজ ফরম্যাট বলে মনে হচ্ছে না।\nCookie-Editor এক্সটেনশন দিয়ে Export -> Netscape ফরম্যাটে কুকিজ এক্সপোর্ট করে পাঠান।",
                parse_mode=None,
            )
            return

        await _apply_incoming_cookie_text(content, status_msg)
        with contextlib.suppress(TelegramError):
            await update.message.delete()
    except Exception as e:
        logger.exception("Error saving uploaded cookies: %s", e)
        await _safe_status_edit(status_msg, f"❌ কুকিজ সংরক্ষণে ত্রুটি: {e}", parse_mode=None)


async def proxy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if OWNER_ID and user and str(user.id) != str(OWNER_ID):
        await update.message.reply_text("এই কমান্ডটি কেবল বট ওনারের জন্য নির্ধারিত।")
        return

    global PROXY_URL
    args = context.args or []
    if not args:
        cur = f"<code>{html.escape(PROXY_URL)}</code>" if PROXY_URL else "❌ সেট করা নেই"
        await update.message.reply_html(
            f"🌐 <b>প্রক্সি স্ট্যাটাস:</b> {cur}\n\n"
            f"💡 <b>প্রক্সি সেট করতে:</b>\n"
            f"<code>/proxy http://user:pass@ip:port</code>\n"
            f"অথবা <code>/proxy socks5://user:pass@ip:port</code>\n"
            f"বন্ধ করতে: <code>/proxy off</code>"
        )
        return

    val = args[0].strip()
    if val.lower() in ("off", "none", "clear", "disable"):
        PROXY_URL = None
        await asyncio.get_running_loop().run_in_executor(None, _sync_cloud_env_var, "PROXY_URL", "")
        await update.message.reply_html("✅ প্রক্সি বন্ধ করা হয়েছে।")
        return

    PROXY_URL = val
    synced = await asyncio.get_running_loop().run_in_executor(None, _sync_cloud_env_var, "PROXY_URL", val)

    await update.message.reply_html(
        f"✅ <b>প্রক্সি সক্রিয় করা হয়েছে!</b>\n"
        f"{'☁️ ক্লাউড সার্ভারেও স্থায়ীভাবে সেভ হয়েছে।' if synced else '💡 রানটাইমে সক্রিয় হয়েছে।'}"
    )


def _load_persisted_queue_items() -> list[dict]:
    raw = ""
    if QUEUE_STATE_FILE.exists():
        with contextlib.suppress(Exception):
            raw = QUEUE_STATE_FILE.read_text(encoding="utf-8").strip()
    if not raw:
        raw = os.environ.get("PERSISTED_QUEUE", "").strip()
    if not raw:
        return []
    with contextlib.suppress(Exception):
        arr = json.loads(raw)
        if isinstance(arr, list):
            return [x for x in arr if isinstance(x, dict) and x.get("url") and x.get("chat_id") and x.get("status_message_id")]
    return []


def _extract_clean_urls_from_text(msg_text: str) -> list[str]:
    non_cookie_lines = []
    for ln in (msg_text or "").splitlines():
        s = _clean_cookie_markdown(ln.strip())
        if not s or s.startswith("#") or "\tTRUE\t" in s or "\tFALSE\t" in s or NETSCAPE_SPACE_LINE_RE.match(s):
            continue
        non_cookie_lines.append(s)
    found = [
        u.rstrip(",;)")
        for u in URL_REGEX.findall("\n".join(non_cookie_lines))
        if u.rstrip(",;)") and "curl.haxx.se" not in u.lower()
    ]
    return list(dict.fromkeys(found))


def _pair_or_first_url(urls: list[str]) -> Optional[str]:
    if not urls:
        return None
    if len(urls) == 2:
        u1, u2 = urls[0], urls[1]
        u1_man = any(ext in u1.lower() for ext in (".m3u8", ".mpd"))
        u2_man = any(ext in u2.lower() for ext in (".m3u8", ".mpd"))
        social_domains = ("youtube.com", "youtu.be", "facebook.com", "fb.watch", "instagram.com", "tiktok.com", "x.com", "twitter.com", "site-ma.")
        if u2_man and not u1_man and not any(d in u1.lower() for d in social_domains) and ".mp4" not in u1.lower():
            return f"{u2}\n{u1}"
        if u1_man and not u2_man and not any(d in u2.lower() for d in social_domains) and ".mp4" not in u2.lower():
            return f"{u1}\n{u2}"
    return urls[0]


async def _scan_and_recover_chat_stuck_jobs(
    context: object,
    chat_id: int,
    max_age_sec: int = 43200,
    probe_top_id: Optional[int] = None,
) -> int:
    """
    চ্যাটে আটকে থাকা ("আপলোড হচ্ছে", "ডাউনলোড...", "কিউতে অপেক্ষমাণ") মেসেজগুলো শনাক্ত করে
    হুবহু আগের ক্রমে এবং আগের মেসেজ কার্ডেই স্বয়ংক্রিয়ভাবে রিজিউম করে!
    """
    if TELETHON_CLIENT is None or not TELETHON_CLIENT.is_connected():
        return 0

    top_id = probe_top_id or 0
    if top_id <= 0:
        for _ in range(2):
            try:
                probe = await context.bot.send_message(
                    chat_id,
                    "🔄 আটকে থাকা আপলোড ও কিউ রিজিউম করা হচ্ছে...",
                    disable_notification=True,
                )
                top_id = probe.message_id
                with contextlib.suppress(TelegramError):
                    await context.bot.delete_message(chat_id=chat_id, message_id=top_id)
                break
            except RetryAfter as ra:
                sec = _extract_flood_seconds(ra) or 5
                _note_chat_flood(chat_id, sec)
                await asyncio.sleep(sec + 2)
            except Exception as e:
                logger.warning("Recovery probe message warning: %s", e)
                return 0

    if top_id <= 1:
        return 0

    start_id = max(1, top_id - 360)
    all_ids = list(range(start_id, top_id))
    fetched_by_id: dict[int, object] = {}

    for off in range(0, len(all_ids), 100):
        batch = [InputMessageID(id=mid) for mid in all_ids[off : off + 100]]
        try:
            res = await TELETHON_CLIENT(GetMessagesRequest(id=batch))
            for m in (getattr(res, "messages", None) or []):
                mid = getattr(m, "id", None)
                if mid:
                    fetched_by_id[int(mid)] = m
        except Exception as e:
            logger.warning("GetMessagesRequest batch warning: %s", e)

    # যেকোনো reply_to_msg_id যদি রেঞ্জের বাইরে থাকে, সেটিও একবারে ফেচ করে নেওয়া
    missing_reply_ids: set[int] = set()
    for m in fetched_by_id.values():
        peer_u = getattr(getattr(m, "peer_id", None), "user_id", None)
        if peer_u != chat_id or not getattr(m, "out", False):
            continue
        rep_id = getattr(getattr(m, "reply_to", None), "reply_to_msg_id", None)
        if rep_id and int(rep_id) not in fetched_by_id:
            missing_reply_ids.add(int(rep_id))

    if missing_reply_ids:
        rep_list = sorted(missing_reply_ids)
        for off in range(0, len(rep_list), 100):
            batch = [InputMessageID(id=mid) for mid in rep_list[off : off + 100]]
            with contextlib.suppress(Exception):
                res = await TELETHON_CLIENT(GetMessagesRequest(id=batch))
                for m in (getattr(res, "messages", None) or []):
                    mid = getattr(m, "id", None)
                    if mid:
                        fetched_by_id[int(mid)] = m

    active_status_ids: set[int] = set()
    if CURRENT_RUNNING_JOB is not None and not CURRENT_RUNNING_JOB.is_cancelled():
        active_status_ids.add(CURRENT_RUNNING_JOB.status_message_id)
    for wj in WAITING_QUEUE:
        if not wj.is_cancelled():
            active_status_ids.add(wj.status_message_id)

    user_msg_urls: dict[int, list[str]] = {}
    all_user_urls: list[str] = []
    chat_msgs_sorted = sorted(
        [m for m in fetched_by_id.values() if getattr(getattr(m, "peer_id", None), "user_id", None) == chat_id],
        key=lambda x: int(getattr(x, "id", 0)),
    )

    for m in chat_msgs_sorted:
        if not getattr(m, "out", False):
            u_list = _extract_clean_urls_from_text(getattr(m, "message", None) or "")
            if u_list:
                user_msg_urls[int(m.id)] = u_list
                for u in u_list:
                    if u not in all_user_urls:
                        all_user_urls.append(u)

    unfinished_keywords = (
        "আপলোড হচ্ছে",
        "ডাউনলোড...",
        "ডাউনলোড শুরু হচ্ছে",
        "ডাউনলোড/আপলোড শুরু হচ্ছে",
        "প্রসেসিং ও আপলোড",
        "কিউতে অপেক্ষমাণ",
        "কিউতে যুক্ত হয়েছে",
        "কিউ প্রস্তুত হচ্ছে",
    )
    skip_prefixes = ("🚫", "🛑", "❌", "🍪", "স্বাগতম", "🛠️", "🌐", "🎉")

    claimed_single_user_msgs: set[int] = set()
    to_recover: list[tuple[int, str, str, str, str]] = []
    now_ts = time.time()

    for m in chat_msgs_sorted:
        mid = int(getattr(m, "id", 0))
        if not getattr(m, "out", False) or mid in active_status_ids:
            continue
        if getattr(m, "document", None) is not None or getattr(m, "photo", None) is not None:
            continue
        txt = (getattr(m, "message", None) or "").strip()
        if not txt or txt.startswith(skip_prefixes):
            continue
        if "কী ধরনের ডাউনলোড চান" in txt or "কোয়ালিটি বেছে নিন" in txt or "ডাউনলোড ব্যর্থ হয়েছে" in txt:
            continue
        if "মোট " in txt and "কিউতে যুক্ত হয়েছে!" in txt:
            continue
        m_date = getattr(m, "date", None)
        if m_date is not None and (now_ts - m_date.timestamp()) > max_age_sec:
            continue
        if not any(kw in txt for kw in unfinished_keywords):
            continue

        resolved_url: Optional[str] = None
        rep_id = getattr(getattr(m, "reply_to", None), "reply_to_msg_id", None)
        if rep_id and int(rep_id) in user_msg_urls:
            u_cands = user_msg_urls[int(rep_id)]
            if len(u_cands) in (1, 2):
                resolved_url = _pair_or_first_url(u_cands)
                claimed_single_user_msgs.add(int(rep_id))

        tag_match = re.search(r"\[(\d+)/(\d+)\]", txt)
        item_tag = f"[{tag_match.group(1)}/{tag_match.group(2)}] " if tag_match else ""
        if resolved_url is None and tag_match:
            idx_num = int(tag_match.group(1))
            tot_num = int(tag_match.group(2))
            for uid in sorted(user_msg_urls.keys(), reverse=True):
                if uid < mid and len(user_msg_urls[uid]) == tot_num and 1 <= idx_num <= tot_num:
                    resolved_url = user_msg_urls[uid][idx_num - 1]
                    break

        if resolved_url is None:
            link_m = re.search(r"🔗\s*(https?://\S+)", txt)
            if link_m:
                raw_prev = link_m.group(1).strip()
                if raw_prev.endswith("..."):
                    pref = raw_prev[:-3]
                    scene_m = re.search(r"site-ma\.([a-z0-9-]+)\.com/scene/(\d+)", pref, re.IGNORECASE)
                    for full_u in reversed(all_user_urls):
                        if full_u.startswith(pref):
                            resolved_url = full_u
                            break
                        if scene_m and f"/scene/{scene_m.group(2)}" in full_u:
                            resolved_url = full_u
                            break
                    if resolved_url is None and pref.startswith("http"):
                        resolved_url = pref
                else:
                    resolved_url = raw_prev

        if resolved_url is None:
            for back_uid in range(mid - 1, max(0, mid - 5), -1):
                if back_uid in user_msg_urls and back_uid not in claimed_single_user_msgs and len(user_msg_urls[back_uid]) == 1:
                    resolved_url = user_msg_urls[back_uid][0]
                    claimed_single_user_msgs.add(back_uid)
                    break

        if not resolved_url:
            logger.warning("Stuck status msg #%d পাওয়া গেছে কিন্তু URL উদ্ধার করা যায়নি: %r", mid, txt[:80])
            continue

        mode = "audio" if "MP3" in txt else "video"
        quality = "1080" if "site-ma." in resolved_url.lower() else "best"
        to_recover.append((mid, resolved_url, mode, quality, item_tag))

    if not to_recover:
        logger.info("🔍 চ্যাট #%d স্ক্যান সম্পন্ন (%dটি মেসেজ চেক হয়েছে) — কোনো আটকে থাকা জব পাওয়া যায়নি।", chat_id, len(chat_msgs_sorted))
        return 0

    logger.info("♻️ চ্যাট #%d এ মোট %dটি আটকে থাকা ডাউনলোড/আপলোড শনাক্ত হয়েছে — স্বয়ংক্রিয়ভাবে রিজিউম করা হচ্ছে!", chat_id, len(to_recover))
    loop = asyncio.get_running_loop()
    recovered_n = 0
    for idx_r, (status_mid, raw_u, mode, quality, item_tag) in enumerate(to_recover):
        if _free_queue_slots() <= 0:
            break
        if "\n" in raw_u:
            canonical_u = raw_u
        else:
            canonical_u = await loop.run_in_executor(None, _resolve_share_url, raw_u)
        if _free_queue_slots() <= 0:
            break
        await _enqueue_download_job(
            context,
            chat_id,
            status_mid,
            canonical_u,
            mode,
            quality,
            item_tag=item_tag,
            quiet_enqueue_edit=(idx_r >= 2),  # মাত্র প্রথম ২টি মেসেজ এডিট করে ফ্লাড এড়ানো
        )
        recovered_n += 1
    return recovered_n


async def _startup_recovery_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    global _STARTUP_RECOVERY_DONE
    if _STARTUP_RECOVERY_DONE:
        return
    _STARTUP_RECOVERY_DONE = True

    for _ in range(25):
        if not MTPROTO_UPLOAD_ENABLED or (TELETHON_CLIENT is not None and TELETHON_CLIENT.is_connected()):
            break
        await asyncio.sleep(0.5)

    restored_count = 0
    persisted = _load_persisted_queue_items()
    for idx_p, item in enumerate(persisted):
        if _free_queue_slots() <= 0:
            break
        try:
            src_m = item.get("source_message_id")
            await _enqueue_download_job(
                context,
                int(item["chat_id"]),
                int(item["status_message_id"]),
                str(item["url"]),
                str(item.get("mode") or "video"),
                str(item.get("quality") or "best"),
                item_tag=str(item.get("item_tag") or ""),
                source_message_id=int(src_m) if src_m else None,
                job_id=str(item.get("job_id") or uuid.uuid4().hex[:8]),
                file_id=str(item.get("file_id") or uuid.uuid4().hex[:8]),
                title=item.get("title"),
                quiet_enqueue_edit=(idx_p >= 2),
            )
            restored_count += 1
        except Exception as e:
            logger.warning("Persisted queue item restore warning: %s", e)

    if restored_count > 0:
        logger.info("♻️ পারসিস্টেন্ট কিউ থেকে %dটি অসমাপ্ত জব রিস্টোর হয়েছে!", restored_count)

    if os.environ.get("AUTO_SCAN_OLD_CHAT_ON_BOOT", "0").strip() == "1" and OWNER_ID and str(OWNER_ID).lstrip("-").isdigit():
        try:
            scanned = await _scan_and_recover_chat_stuck_jobs(context, int(OWNER_ID), max_age_sec=43200)
            if scanned > 0:
                logger.info("♻️ চ্যাট হিস্ট্রি স্ক্যান থেকে আরও %dটি আটকে থাকা জব রিজিউম কিউতে যুক্ত হয়েছে!", scanned)
        except Exception as scan_err:
            logger.warning("Startup chat scan recovery warning: %s", scan_err)


async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if _free_queue_slots() <= 0:
        await update.message.reply_html(
            f"⚠️ <b>কিউ পূর্ণ ({MAX_QUEUE_SLOTS}/{MAX_QUEUE_SLOTS} স্লট বুকড)!</b> চলমান একটি ভিডিও শেষ হলে আবার <code>/resume</code> দিন।"
        )
        return
    resumed_failed = 0
    for jid, fjob in list(FAILED_RESUMABLE_JOBS.items()):
        if _free_queue_slots() <= 0:
            break
        if fjob.chat_id == chat_id:
            FAILED_RESUMABLE_JOBS.pop(jid, None)
            await _enqueue_download_job(
                context,
                fjob.chat_id,
                fjob.status_message_id,
                fjob.url,
                fjob.mode,
                fjob.quality,
                item_tag=fjob.item_tag,
                source_message_id=fjob.source_message_id,
                job_id=fjob.job_id,
                file_id=fjob.file_id,
                title=fjob.title,
                quiet_enqueue_edit=(resumed_failed >= 2),
            )
            resumed_failed += 1

    scanned = 0
    if _free_queue_slots() > 0:
        scanned = await _scan_and_recover_chat_stuck_jobs(
            context,
            chat_id,
            max_age_sec=172800,
            probe_top_id=update.message.message_id,
        )
    total_active = _active_queue_count()
    newly = resumed_failed + scanned
    if newly > 0:
        await update.message.reply_html(
            f"♻️ <b>মোট {newly}টি আটকে থাকা ডাউনলোড/আপলোড পুনরায় চালু (Resume) হয়েছে!</b>\n"
            f"📊 বর্তমানে সক্রিয় কিউ স্লট: <b>{total_active}/{MAX_QUEUE_SLOTS}</b>।"
        )
    elif total_active > 0:
        await update.message.reply_html(
            f"✅ কিউ ইতিমধ্যেই সচল আছে! বর্তমানে <b>{total_active}/{MAX_QUEUE_SLOTS}</b> স্লটে প্রসেস চলছে।"
        )
    else:
        await update.message.reply_text("এই মুহূর্তে কোনো আটকে থাকা বা অসমাপ্ত আপলোড/ডাউনলোড পাওয়া যায়নি।")


def _startup_self_test() -> None:
    """স্টার্টআপের ৪ সেকেন্ড পর ব্যাকগ্রাউন্ডে FB, YT ও সক্রিয় মেম্বার সেশন সংযোগ যাচাই করে লগে প্রিন্ট করে।"""
    time.sleep(4)
    tests = [
        ("BUNNY-HLS", "https://vz-a6f58c33-be8.b-cdn.net/5c50e1dc-64d2-4c5c-95b1-6d6b279d4382/playlist.m3u8"),
        ("FB", "https://www.facebook.com/reel/1135567922561604"),
        ("YT", "https://www.youtube.com/watch?v=YEgaXkTqbIM"),
    ]
    if COOKIES_RUNTIME_PATH.exists():
        raw_c = COOKIES_RUNTIME_PATH.read_text(encoding="utf-8", errors="ignore")
        if "brazzers.com" in raw_c:
            tests.append(("AYLO", "https://site-ma.brazzers.com/scene/4413015/party-like-a-fingers-up-your-ass"))

    for label, test_url in tests:
        try:
            info = _extract_info_sync(test_url)
            h = _max_format_height(info)
            logger.info("✅ [SELF-TEST %s] সফল! title=%r max_height=%dp", label, (info.get("title") or "")[:50], h)
            if label == "AYLO" and info.get("direct_url"):
                r_hls = requests.get(
                    info["direct_url"],
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                        "Origin": "https://site-ma.brazzers.com",
                        "Referer": "https://site-ma.brazzers.com/",
                    },
                    timeout=10,
                )
                logger.info("✅ [SELF-TEST AYLO-CDN] master.m3u8 HTTP %d (%d bytes)", r_hls.status_code, len(r_hls.text))
        except Exception as e:
            logger.warning("⚠️ [SELF-TEST %s] ব্যর্থ: %s", label, str(e)[:240])


# ---------------------------------------------------------------------------
# এন্ট্রি পয়েন্ট
# ---------------------------------------------------------------------------
def main() -> None:
    global LOCAL_API_ENABLED, MAX_TELEGRAM_FILE_SIZE, SPLIT_TARGET_SIZE, COMPRESS_TARGET_SIZE, POT_PROVIDER_URL

    # নতুন ডিপ্লয় শুরু হলে পুরনো কন্টেইনার যাতে ১ সেকেন্ডের মধ্যে বন্ধ হয়ে যায় (ডুপ্লিকেট getUpdates কনফ্লিক্ট রোধে)
    def _instant_sigterm(signum, frame) -> None:
        logger.info("🛑 SIGTERM রিসিভ হয়েছে — পুরনো কন্টেইনার তাৎক্ষণিক বন্ধ হচ্ছে...")
        os._exit(0)

    with contextlib.suppress(Exception):
        signal.signal(signal.SIGTERM, _instant_sigterm)

    _preload_protected_file_ids_from_disk()
    _load_download_history_on_boot()
    _load_links_cache_on_boot()
    _cleanup_stale_files(max_age_seconds=7200)  # সাম্প্রতিক রিজিউমযোগ্য ফাইল ও চেকপয়েন্ট অক্ষত রেখে ২ ঘণ্টার পুরনো অনাথ ফাইল পরিষ্কার
    _check_cookies_file()
    with contextlib.suppress(Exception):
        _try_renew_aylo_tokens(force=False)

    # yt-dlp এর অফিসিয়াল idempotent লোডার দিয়ে সব প্লাগইন (bgutil-pot সহ) একবারই লোড করা
    try:
        from yt_dlp.plugins import load_all_plugins
        load_all_plugins()
        logger.info("✅ yt-dlp plugins (সহ bgutil-pot) সঠিকভাবে লোড হয়েছে।")
    except Exception as e:
        logger.warning("⚠️ yt-dlp plugin লোড চেক সতর্কতা: %s", e)

    start_keepalive(interval=120)

    # start.sh মাত্র ২০ সেকেন্ড অপেক্ষা করে PO Token Provider এর জন্য — ধীর
    # cold-start এ এর চেয়ে বেশি সময় লাগলে start.sh আগেই POT_PROVIDER_URL খালি
    # রেখে দেয়, আর bot.py এরপর আর কখনো রিচেক করত না (এটাই race condition ছিল)।
    # তাই এখানে বট নিজে, আরও ধৈর্য ধরে (৩০ সেকেন্ড পর্যন্ত), একবার নিজে যাচাই করে নেয়।
    local_pot_url = "http://127.0.0.1:4416"
    if not DISABLE_POT_PROVIDER and POT_PROVIDER_URL in (None, local_pot_url):
        if _url_reachable(local_pot_url, timeout=3.0, retries=10, delay=3.0):
            POT_PROVIDER_URL = local_pot_url
            logger.info("PO Token Provider রিচেবল নিশ্চিত হলো: %s", POT_PROVIDER_URL)
        elif POT_PROVIDER_URL is not None:
            logger.warning("PO Token Provider (%s) এ কানেক্ট করা গেল না — PO Token ছাড়াই চলবে।", local_pot_url)
            POT_PROVIDER_URL = None

    def _switch_to_cloud_api(reason: str) -> None:
        global LOCAL_API_ENABLED, MAX_TELEGRAM_FILE_SIZE, SPLIT_TARGET_SIZE, COMPRESS_TARGET_SIZE
        logger.warning(
            "⚠️ Local Bot API সাময়িকভাবে প্রস্তুত নয় (%s) — তাৎক্ষণিকভাবে অফিসিয়াল ক্লাউড Bot API মোডে সুইচ করা হচ্ছে যাতে বট সাথে সাথে রেসপন্স করে।",
            reason,
        )
        LOCAL_API_ENABLED = False
        if not MTPROTO_UPLOAD_ENABLED:
            MAX_TELEGRAM_FILE_SIZE = 49 * 1024 * 1024
            SPLIT_TARGET_SIZE = int(MAX_TELEGRAM_FILE_SIZE * 0.92)
            COMPRESS_TARGET_SIZE = int(MAX_TELEGRAM_FILE_SIZE * 0.93)

    if LOCAL_API_ENABLED:
        if not _local_api_reachable():
            _switch_to_cloud_api("সার্ভার আনরিচেবল")
        else:
            # TDLib কন্টেইনারের প্রথম কোল্ড getMe কলে MTProto হ্যান্ডশেক করতে ~১০-১৫ সেকেন্ড নিতে পারে;
            # তাই ২২ সেকেন্ড টাইমআউট দেওয়া হচ্ছে যাতে ২GB সিঙ্গেল-ফাইল মোড চালু হয় (ফ্লাড-ওয়েট থাকলে ক্লাউড মোডে সুইচ করবে)
            try:
                auth_probe = requests.get(f"{LOCAL_BOT_API_URL}/bot{BOT_TOKEN}/getMe", timeout=22)
                if auth_probe.status_code != 200 or not auth_probe.json().get("ok"):
                    _switch_to_cloud_api(f"HTTP {auth_probe.status_code}")
            except Exception as probe_err:
                _switch_to_cloud_api(f"TDLib login cooldown: {probe_err}")

    def _build_app() -> Application:
        builder = (
            Application.builder()
            .token(BOT_TOKEN)
            .concurrent_updates(8)
            .connection_pool_size(32)
            .read_timeout(1800)
            .write_timeout(1800)
            .connect_timeout(60)
            .pool_timeout(60)
            .post_init(_init_telethon_client)
            .post_shutdown(_shutdown_telethon_client)
        )
        if LOCAL_API_ENABLED:
            builder = (
                builder.base_url(f"{LOCAL_BOT_API_URL}/bot")
                .base_file_url(f"{LOCAL_BOT_API_URL}/file/bot")
                .local_mode(True)
            )
        app = builder.build()
        app.add_handler(CommandHandler("start", start_cmd))
        app.add_handler(CommandHandler("help", help_cmd))
        app.add_handler(CommandHandler("resume", resume_cmd))
        app.add_handler(CommandHandler("cancelall", cancelall_cmd))
        app.add_handler(CommandHandler("cookies", cookies_cmd))
        app.add_handler(CommandHandler("proxy", proxy_cmd))
        app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
        app.add_handler(CallbackQueryHandler(button_handler))
        app.add_error_handler(error_handler)

        if app.job_queue is not None:
            app.job_queue.run_once(
                _startup_recovery_job,
                when=5.0,
                job_kwargs={"misfire_grace_time": 180},
            )
            app.job_queue.run_repeating(
                _maintenance_job, interval=MAINTENANCE_INTERVAL_SECONDS, first=30
            )
        return app

    if LOCAL_API_ENABLED:
        logger.info(
            "Local Bot API সার্ভার মোড চালু (%s) — সিঙ্গেল ফাইল লিমিট ~%dMB",
            LOCAL_BOT_API_URL, MAX_TELEGRAM_FILE_SIZE // (1024 * 1024),
        )
    elif MTPROTO_UPLOAD_ENABLED:
        logger.info(
            "FastTelethon MTProto প্যারালাল আপলোড মোড চালু — সিঙ্গেল ফাইল লিমিট ~%dMB",
            MAX_TELEGRAM_FILE_SIZE // (1024 * 1024),
        )
    else:
        logger.info("সাধারণ ক্লাউড টেলিগ্রাম Bot API ব্যবহার হচ্ছে — সিঙ্গেল ফাইল লিমিট ৫০MB (বড় ফাইল অটো-স্প্লিট সচল)")

    logger.info(
        "বট চালু হচ্ছে... (max_slots=%d, dl_threads=%d, range_threads=%d, ul_slots=%dx%d, uvloop=%s, cookies=%s, pot=%s)",
        MAX_QUEUE_SLOTS,
        MAX_DL_THREADS,
        MAX_HTTP_RANGE_THREADS,
        MTPROTO_POOL_SIZE,
        SLOTS_PER_AUTH_KEY,
        "yes" if UVLOOP_AVAILABLE else "no",
        "yes" if COOKIES_FILE else "no",
        POT_PROVIDER_URL or "disabled",
    )
    if os.environ.get("RUN_STARTUP_SELF_TEST", "0").strip() == "1":
        threading.Thread(target=_startup_self_test, daemon=True, name="startup-self-test").start()

    global DOWNLOAD_SEMAPHORE, QUEUE_WAKE_EVENT, QUEUE_WORKER_TASK
    rlwy_dom = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    render_ext = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    webhook_base = (
        os.environ.get("WEBHOOK_URL", "").strip()
        or render_ext
        or (f"https://{rlwy_dom}" if rlwy_dom else "")
    ).rstrip("/")
    use_webhook = bool(
        webhook_base.startswith("https://")
        and not LOCAL_API_ENABLED
        and os.environ.get("DISABLE_WEBHOOK", "").strip().lower() not in ("1", "true", "yes")
    )

    if use_webhook:
        async def _serve_webhook_forever() -> None:
            global DOWNLOAD_SEMAPHORE, QUEUE_WAKE_EVENT, QUEUE_WORKER_TASK
            loop = asyncio.get_running_loop()
            DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
            QUEUE_WAKE_EVENT = None
            QUEUE_WORKER_TASK = None
            application = _build_app()
            slug = hashlib.sha256(f"slug:{BOT_TOKEN}".encode("utf-8")).hexdigest()[:20]
            secret_tok = hashlib.sha256(f"sec:{BOT_TOKEN}".encode("utf-8")).hexdigest()[:32]
            wh_url = f"{webhook_base}/tg-webhook/{slug}"

            def _on_raw_update(raw_data: dict) -> None:
                upd = Update.de_json(raw_data, application.bot)
                if upd is not None:
                    loop.call_soon_threadsafe(application.update_queue.put_nowait, upd)

            register_telegram_webhook(slug, secret_tok, _on_raw_update)
            await application.initialize()
            await _init_telethon_client(application)
            await application.start()
            try:
                await application.bot.set_webhook(
                    url=wh_url,
                    secret_token=secret_tok,
                    drop_pending_updates=False,
                    allowed_updates=Update.ALL_TYPES,
                )
                logger.info("🌐 Telegram HTTPS Webhook সক্রিয় হয়েছে: %s (জিরো কনফ্লিক্ট ও তাৎক্ষণিক রেসপন্স!)", wh_url)
                asyncio.create_task(_startup_recovery_job(types.SimpleNamespace(bot=application.bot)))

                async def _webhook_guardian() -> None:
                    """অন্য কোথাও চলা পুরনো বট (getUpdates পোলার) webhook মুছে দিলে ৩০ সেকেন্ডের মধ্যে আবার লক করে দেয়।"""
                    while True:
                        await asyncio.sleep(30)
                        try:
                            info = await application.bot.get_webhook_info()
                            if (info.url or "") != wh_url:
                                logger.warning(
                                    "🛡️ Webhook সরিয়ে ফেলা হয়েছিল (বর্তমান url=%r) — সম্ভবত অন্য কোথাও পুরনো বট চলছে! পুনরায় লক করা হচ্ছে...",
                                    info.url,
                                )
                                await application.bot.set_webhook(
                                    url=wh_url,
                                    secret_token=secret_tok,
                                    drop_pending_updates=False,
                                    allowed_updates=Update.ALL_TYPES,
                                )
                        except Exception as g_err:
                            logger.debug("Webhook guardian warning: %s", g_err)

                asyncio.create_task(_webhook_guardian())
                stop_ev = asyncio.Event()
                await stop_ev.wait()
            finally:
                with contextlib.suppress(Exception):
                    await application.stop()
                await _shutdown_telethon_client(application)
                with contextlib.suppress(Exception):
                    await application.shutdown()

        while True:
            default_exec = ThreadPoolExecutor(max_workers=12, thread_name_prefix="bot-io")
            try:
                loop = asyncio.new_event_loop()
                loop.set_default_executor(default_exec)
                asyncio.set_event_loop(loop)
                loop.run_until_complete(_serve_webhook_forever())
                break
            except Exception as wh_err:
                logger.warning("⚠️ Webhook রানটাইম বিঘ্ন (%s) — ৫ সেকেন্ড পর পুনরায় চালু হচ্ছে...", wh_err)
                time.sleep(5)
            finally:
                default_exec.shutdown(wait=False)
        return

    while True:
        default_exec = ThreadPoolExecutor(max_workers=12, thread_name_prefix="bot-io")
        try:
            loop = asyncio.new_event_loop()
            loop.set_default_executor(default_exec)
            asyncio.set_event_loop(loop)
            DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
            QUEUE_WAKE_EVENT = None
            QUEUE_WORKER_TASK = None
            application = _build_app()
            application.run_polling(drop_pending_updates=False)
            break
        except RetryAfter as e:
            if LOCAL_API_ENABLED:
                _switch_to_cloud_api(f"RetryAfter {getattr(e, 'retry_after', '?')}s")
                continue
            wait_s = int(getattr(e, "retry_after", 35)) + 5
            logger.warning("⏳ Telegram flood control — %ds অপেক্ষা করা হচ্ছে...", wait_s)
            time.sleep(wait_s)
        except (TimedOut, NetworkError, TelegramError) as e:
            if LOCAL_API_ENABLED:
                _switch_to_cloud_api(str(e))
                continue
            logger.warning("⏳ Telegram পোলিং সংযোগ বিঘ্ন (%s) — ১০ সেকেন্ড পর পুনরায় চেষ্টা হচ্ছে...", e)
            time.sleep(10)
        finally:
            default_exec.shutdown(wait=False)


if __name__ == "__main__":
    main()
