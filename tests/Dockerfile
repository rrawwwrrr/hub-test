FROM node:20-slim

# curl is needed by the entrypoint health-check wait loop
RUN apt-get update -o "Acquire::https::Verify-Peer=false" && apt-get install -o "Acquire::https::Verify-Peer=false" -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first (layer cache friendly)
COPY package.json ./
RUN npm install --omit=dev

# Copy test sources
COPY wdio.conf.js ./
COPY test/ ./test/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Runtime env defaults (overridden by docker run -e)
ENV APPIUM_HOST=localhost \
    APPIUM_PORT=4723 \
    ANDROID_SERIAL="" \
    APPIUM_WAIT_SECONDS=60

ENTRYPOINT ["/entrypoint.sh"]
