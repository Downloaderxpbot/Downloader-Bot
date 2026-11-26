"""
keepalive.py
------------
হালকা Flask হেলথচেক, কিপ-অ্যালাইভ ও আল্ট্রা-লো ল্যাটেন্সি Telegram Webhook রিসিভার।
"""

from __future__ import annotations

import os
import time
import logging
import threading
from typing import Callable, Optional

import requests
from flask import Flask, abort, request

log = logging.getLogger("keepalive")
logging.getLogger("werkzeug").setLevel(logging.ERROR)

app = Flask(__name__)
_started = False
_webhook_slug: Optional[str] = None
_webhook_secret: Optional[str] = None
_webhook_callback: Optional[Callable[[dict], None]] = None


def register_telegram_webhook(
    slug: str,
    secret_token: str,
    on_update: Callable[[dict], None],
) -> None:
    global _webhook_slug, _webhook_secret, _webhook_callback
    _webhook_slug = slug
    _webhook_secret = secret_token
    _webhook_callback = on_update


@app.get("/")
def root():
    return "Bot is alive on Render Pro ✅"


@app.get("/health")
def health():
    return {"status": "ok", "ts": int(time.time())}


@app.post("/tg-webhook/<slug>")
def telegram_webhook(slug: str):
    if not _webhook_callback or not _webhook_slug or slug != _webhook_slug:
        abort(404)
    if _webhook_secret:
        hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if hdr != _webhook_secret:
            abort(403)
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        try:
            _webhook_callback(payload)
        except Exception as e:
            log.warning("Webhook dispatch error: %s", e)
    return "ok", 200


def _run_flask(port: int) -> None:
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)


def _self_ping_loop(url: str, interval: int) -> None:
    with requests.Session() as sess:
        while True:
            time.sleep(interval)
            try:
                with sess.get(url, timeout=10) as resp:
                    log.debug("self-ping ok (%s) -> %s", resp.status_code, url)
            except Exception as e:
                log.debug("self-ping failed -> %s: %s", url, e)


def start_keepalive(interval: int = 180) -> None:
    global _started
    if _started:
        return
    _started = True

    port = int(os.environ.get("PORT", "8080"))
    threading.Thread(target=_run_flask, args=(port,), daemon=True, name="keepalive-flask").start()

    rlwy_dom = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    external_url = (
        (f"https://{rlwy_dom}" if rlwy_dom else "")
        or os.environ.get("SELF_URL", "").strip()
        or os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    )
    if external_url:
        ping_url = external_url.rstrip("/") + "/health"
        threading.Thread(
            target=_self_ping_loop,
            args=(ping_url, interval),
            daemon=True,
            name="keepalive-selfping",
        ).start()
        log.info("Healthcheck & Webhook HTTP সার্ভার সক্রিয়: %s (port=%d)", ping_url, port)
    else:
        log.info("Healthcheck HTTP সার্ভার সক্রিয় (port=%d)", port)
