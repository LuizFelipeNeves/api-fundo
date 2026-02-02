FROM node:20-bookworm-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-venv make g++ libxcb1 libx11-6 libxext6 libxrender1 libxkbcommon0 libgl1 libglib2.0-0 \
  && python3 -m venv /opt/venv \
  && /opt/venv/bin/pip install --no-cache-dir -U pip \
  && /opt/venv/bin/pip install --no-cache-dir docling \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --include=dev

COPY tsconfig.json ./
COPY src ./src

ENV NODE_ENV=production
ENV PORT=8080
ENV PATH="/opt/venv/bin:${PATH}"

EXPOSE 8080

CMD ["node", "--import", "tsx", "src/index.ts"]
