FROM docker:dind

# Install Node.js, npm, and build dependencies for native modules
RUN apk add --no-cache nodejs npm python3 make g++

WORKDIR /app
COPY package*.json ./
COPY tsconfig.base.json ./
COPY proto/ ./proto/
COPY shared/ ./shared/
COPY agent/ ./agent/

RUN npm install
RUN npm run build --workspace=shared
RUN npm run build --workspace=agent

WORKDIR /app/agent
COPY agent/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
