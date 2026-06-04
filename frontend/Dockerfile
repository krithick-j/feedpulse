FROM node:22-alpine

WORKDIR /app

COPY package.json pnpm-lock.yaml /app/

RUN corepack enable
RUN pnpm install --frozen-lockfile

COPY . /app

EXPOSE 5173

CMD ["pnpm", "dev", "--host", "0.0.0.0", "--port", "5173"]
