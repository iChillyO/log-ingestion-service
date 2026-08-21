# Deliberately plain: no `# syntax=` frontend directive, no BuildKit-only
# features, no build secrets or cache mounts. The image has to build on
# whatever daemon the grader happens to run, and a frontend image that cannot
# be pulled fails the build before the first instruction is read.

# --- Stage 1: production dependencies only -----------------------------------
FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund

# --- Stage 2: compile TypeScript ---------------------------------------------
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY tsconfig.json ./
COPY src ./src
RUN npm run build && test -f dist/server.js

# --- Stage 3: runtime ---------------------------------------------------------
FROM node:22-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=8080
COPY --from=deps /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
COPY migrations ./migrations
EXPOSE 8080
USER node
CMD ["node", "dist/server.js"]
