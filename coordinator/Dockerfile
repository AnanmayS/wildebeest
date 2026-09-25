# ---- build: compile TypeScript -------------------------------------------------------------
FROM node:22-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN npm run build

# ---- runtime: production deps + compiled JS + SQL migrations --------------------------------
FROM node:22-slim
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY --from=build /app/dist ./dist
COPY migrations ./migrations
EXPOSE 3000
# Runs as root so it can reach the mounted /var/run/docker.sock (Kill button, chaos mode).
# On start: apply pending migrations, ensure the MinIO bucket, then serve (see src/index.ts).
# otel-preload does nothing unless OTEL_EXPORTER_OTLP_ENDPOINT is set (then: tracing, see otel-sdk.ts).
CMD ["node", "--import", "./dist/otel-preload.js", "dist/index.js"]
