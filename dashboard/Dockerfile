# --- build: compile the React app ---------------------------------------
FROM node:22-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
# Optional: a Grafana link in the header (docker compose build --build-arg VITE_GRAFANA_URL=http://localhost:3300/d/wildebeest-overview dashboard).
ARG VITE_GRAFANA_URL=""
ENV VITE_GRAFANA_URL=$VITE_GRAFANA_URL
RUN npm run build

# --- serve: static files + /api reverse proxy ----------------------------
FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
