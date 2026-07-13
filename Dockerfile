FROM node:24.12.0 AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:24.12.0 AS runner
WORKDIR /app
ENV CAMOUFOX_INSTALL_DIR=/tmp/camoufox
COPY package*.json ./
RUN npm ci --omit=dev
RUN mkdir -p "$CAMOUFOX_INSTALL_DIR"
RUN npx camoufox-js fetch
RUN test -f "$CAMOUFOX_INSTALL_DIR/version.json"
RUN npx camoufox-js version
RUN npx playwright install-deps firefox
COPY --from=builder /app/dist ./dist
CMD ["node", "dist/index.js"]
