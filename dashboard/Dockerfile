FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
COPY tsconfig.base.json ./
COPY proto/ ./proto/
COPY shared/ ./shared/
COPY dashboard/ ./dashboard/

RUN npm install
RUN npm run build --workspace=shared
RUN npm run build --workspace=dashboard

FROM node:20-alpine
WORKDIR /app
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/shared/dist ./shared/dist
COPY --from=builder /app/shared/package.json ./shared/package.json
COPY --from=builder /app/dashboard/dist ./dashboard/dist
COPY --from=builder /app/dashboard/package.json ./dashboard/package.json
COPY --from=builder /app/dashboard/public ./dashboard/public
COPY --from=builder /app/node_modules ./node_modules

ENV DASHBOARD_PORT=3000
ENV CONTROL_PLANE_API=http://control-plane:7000

EXPOSE 3000
CMD ["node", "dashboard/dist/index.js"]
