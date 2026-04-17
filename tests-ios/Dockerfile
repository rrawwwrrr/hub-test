FROM node:20-slim

RUN apt-get update -o "Acquire::https::Verify-Peer=false" -y && apt-get install -o "Acquire::https::Verify-Peer=false" -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY wdio.conf.js ./
COPY test/ ./test/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV APPIUM_HOST=localhost \
    APPIUM_PORT=4723 \
    IOS_UDID="" \
    WDA_URL="" \
    IOS_IPA_URL="" \
    APPIUM_WAIT_SECONDS=90

ENTRYPOINT ["/entrypoint.sh"]
