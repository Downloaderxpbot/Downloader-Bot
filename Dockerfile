# ---------------------------------------------------------------------------
# আল্ট্রা-ফাস্ট সিঙ্গেল-স্টেজ রানটাইম ইমেজ (Railway Bare-Metal Optimized)
#   - uvloop + cryptg + pycryptodomex + FastTelethon (2GB MTProto 20x2 প্যারালাল আপলোড)
#   - প্রি-বিল্ট bgutil-pot বাইনারি + Deno JS রানটাইম + FFmpeg
# ---------------------------------------------------------------------------
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/usr/local/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg curl aria2 unzip ca-certificates openssl && \
    rm -rf /var/lib/apt/lists/*

# Deno (JS runtime) — ইউটিউবের সিগনেচার/PO-token চ্যালেঞ্জ সলভ করতে yt-dlp ব্যবহার করে
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh

# প্রি-বিল্ট bgutil-pot বাইনারি + অফিসিয়াল yt-dlp প্লাগইন জিপ
RUN ARCH="$(uname -m)" && \
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
        POT_BIN="bgutil-pot-linux-aarch64"; \
    else \
        POT_BIN="bgutil-pot-linux-x86_64"; \
    fi && \
    curl -fsSL "https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/latest/download/${POT_BIN}" \
         -o /usr/local/bin/bgutil-pot && \
    chmod +x /usr/local/bin/bgutil-pot && \
    mkdir -p /etc/yt-dlp/plugins && \
    curl -fsSL "https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/latest/download/bgutil-ytdlp-pot-provider-rs.zip" \
         -o /etc/yt-dlp/plugins/bgutil-ytdlp-pot-provider-rs.zip && \
    chmod -R a+rX /etc/yt-dlp

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --upgrade "yt-dlp[default,curl-cffi]"

COPY bot.py keepalive.py start.sh ./
RUN sed -i 's/\r$//' start.sh && chmod +x start.sh && python -m py_compile bot.py keepalive.py

RUN useradd -m botuser && \
    mkdir -p /app/tdlib-data /app/downloads && \
    chown -R botuser /app
USER botuser

CMD ["./start.sh"]
