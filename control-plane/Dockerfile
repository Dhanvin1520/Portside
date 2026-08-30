FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
COPY tsconfig.base.json ./
COPY proto/ ./proto/
COPY shared/ ./shared/
COPY control-plane/ ./control-plane/

RUN npm install
RUN npm run build --workspace=shared
RUN npm run build --workspace=control-plane

FROM node:20-alpine
WORKDIR /app
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/proto/ ./proto/
COPY --from=builder /app/shared/dist ./shared/dist
COPY --from=builder /app/shared/package.json ./shared/package.json
COPY --from=builder /app/control-plane/dist ./control-plane/dist
COPY --from=builder /app/control-plane/package.json ./control-plane/package.json
COPY --from=builder /app/node_modules ./node_modules

RUN mkdir -p /data
ENV DATA_DIR=/data
ENV REST_PORT=7000
ENV GRPC_PORT=7001

EXPOSE 7000 7001
CMD ["node", "control-plane/dist/index.js"]
