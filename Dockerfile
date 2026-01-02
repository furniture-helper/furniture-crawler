FROM node:24.12.0 AS builder
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM node:24.12.0 AS runner
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package*.json ./
RUN npm install --omit=dev
RUN npm install playwright
RUN npx playwright install --with-deps
CMD ["node", "dist/index.js"]