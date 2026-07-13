FROM node:24.12.0 AS builder
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM node:24.12.0 AS runner
WORKDIR /app
ENV CAMOUFOX_INSTALL_DIR=/opt/camoufox
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package*.json ./
RUN npm install --omit=dev
RUN npx camoufox-js fetch
RUN test -f "$CAMOUFOX_INSTALL_DIR/version.json"
RUN npx camoufox-js version
RUN npm install playwright
RUN npx playwright install --with-deps
CMD ["node", "dist/index.js"]
