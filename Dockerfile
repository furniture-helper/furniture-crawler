FROM node:24.12.0 AS builder
WORKDIR /app

ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

COPY package*.json ./
RUN npm install

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:24.12.0 AS runner
WORKDIR /app

ENV NODE_ENV=production

COPY package*.json ./
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
RUN npm install --omit=dev

RUN npx playwright install-deps firefox

ENV CAMOUFOX_INSTALL_DIR=/app/.cache/camoufox
ENV CAMOUFOX_INSTALL_DIR=/app/.cache/camoufox

RUN mkdir -p "$CAMOUFOX_INSTALL_DIR" \
 && PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=0 npx camoufox-js fetch \
 && find "$CAMOUFOX_INSTALL_DIR" -maxdepth 4 \( -type f -o -type l \) | sort

COPY --from=builder /app/dist ./dist
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=0
CMD ["node", "dist/index.js"]