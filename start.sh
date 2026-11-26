#!/bin/sh
# হাই-স্পিড কন্টেইনার বুটস্ট্র্যাপ:
#   ১. bgutil-pot (PO Token Provider) — 127.0.0.1:4416
#   ২. ঐচ্ছিক telegram-bot-api (ENABLE_LOCAL_BOT_API=1 ও বাইনারি থাকলে)
#   ৩. bot.py (uvloop + FastTelethon 2GB MTProto 20x2 আপলোডার + 48x/24x ডাউনলোডার)
set -e

# ১GB কন্টেইনার cgroup-এ মাল্টি-থ্রেড glibc র‍্যাম ফ্র্যাগমেন্টেশন রোধ করতে ২টি অ্যারেনা ও ২MB থ্রেড স্ট্যাক সীমা
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"
ulimit -s 2048 2>/dev/null || true

mkdir -p /app/tdlib-data /app/downloads

TELEGRAM_API_ID="${TELEGRAM_API_ID:-$API_ID}"
TELEGRAM_API_HASH="${TELEGRAM_API_HASH:-$API_HASH}"
export TELEGRAM_API_ID
export TELEGRAM_API_HASH

if [ -n "$COOKIES_DATA" ]; then
    echo "🍪 COOKIES_DATA পরিবেশ ভ্যারিয়েবল পাওয়া গেছে — /app/cookies.txt তৈরি হচ্ছে..."
    printf "%s\n" "$COOKIES_DATA" > /app/cookies.txt
    export COOKIES_FILE="${COOKIES_FILE:-/app/cookies.txt}"
elif [ -z "$COOKIES_FILE" ] && [ -f "/app/cookies.txt" ]; then
    export COOKIES_FILE="/app/cookies.txt"
elif [ -z "$COOKIES_FILE" ] && [ -f "cookies.txt" ]; then
    export COOKIES_FILE="cookies.txt"
fi

if [ "$DISABLE_POT_PROVIDER" = "1" ] || [ "$DISABLE_POT_PROVIDER" = "true" ]; then
    echo "ℹ️  DISABLE_POT_PROVIDER সেট করা আছে — bgutil-pot চালু হবে না।"
    export POT_PROVIDER_URL=""
elif command -v bgutil-pot >/dev/null 2>&1; then
    echo "🔧 PO Token Provider (bgutil-pot) চালু হচ্ছে..."
    bgutil-pot server --host 127.0.0.1 --port 4416 &
    for i in $(seq 1 16); do
        if curl -s -o /dev/null "http://127.0.0.1:4416/ping"; then
            echo "✅ PO Token Provider প্রস্তুত।"
            break
        fi
        sleep 0.4
    done
    export POT_PROVIDER_URL="${POT_PROVIDER_URL:-http://127.0.0.1:4416}"
else
    export POT_PROVIDER_URL=""
fi

if [ "$ENABLE_LOCAL_BOT_API" = "1" ] && command -v telegram-bot-api >/dev/null 2>&1 && [ -n "$TELEGRAM_API_ID" ] && [ -n "$TELEGRAM_API_HASH" ]; then
    echo "🔧 Local Bot API সার্ভার চালু হচ্ছে..."
    telegram-bot-api \
        --api-id="$TELEGRAM_API_ID" \
        --api-hash="$TELEGRAM_API_HASH" \
        --local \
        --verbosity=1 \
        --http-port=8081 \
        --dir=/app/tdlib-data &
    for i in $(seq 1 20); do
        if curl -s -o /dev/null "http://127.0.0.1:8081/"; then
            echo "✅ Local Bot API সার্ভার প্রস্তুত।"
            break
        fi
        sleep 0.5
    done
fi

exec python bot.py
