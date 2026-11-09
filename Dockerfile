FROM node:20-alpine AS builder

WORKDIR /app

# Copy dependency manifests and configs
COPY package*.json ./
COPY tsconfig.json ./

# Install dependencies and compile TypeScript
RUN npm install
COPY src ./src
RUN npm run build

# Production runtime stage
FROM node:20-alpine

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3000

COPY package*.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY index.js ./

EXPOSE 3000

CMD ["node", "index.js"]

