FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
COPY tsconfig.base.json ./
COPY proto/ ./proto/
COPY shared/ ./shared/
COPY proxy/ ./proxy/

RUN npm install
RUN npm run build --workspace=shared
RUN npm run build --workspace=proxy

FROM node:20-alpine
WORKDIR /app
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/shared/dist ./shared/dist
COPY --from=builder /app/shared/package.json ./shared/package.json
COPY --from=builder /app/proxy/dist ./proxy/dist
COPY --from=builder /app/proxy/package.json ./proxy/package.json
COPY --from=builder /app/node_modules ./node_modules

ENV PROXY_PORT=8080
ENV CONTROL_PLANE_API=http://control-plane:7000

EXPOSE 8080
CMD ["node", "proxy/dist/index.js"]
