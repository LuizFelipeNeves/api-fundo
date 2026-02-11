FROM oven/bun:1.1.20

WORKDIR /app

# Copy package.json and lockfile (Bun uses bun.lockb)
COPY package.json bun.lockb* ./

# Install dependencies using Bun
RUN bun install --production

# Copy the rest of your code
COPY tsconfig.json ./
COPY apps ./apps
COPY database ./database

ENV NODE_ENV=production
ENV PORT=8080

EXPOSE 8080

# Run your API
CMD ["bun", "run", "apps/api/src/index.ts"]
