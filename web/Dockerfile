FROM node:24.10.0-alpine AS build
WORKDIR /web
COPY package.json ./
COPY src ./src
COPY scripts ./scripts
RUN npm run build

FROM nginx:1.29.5-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /web/dist /usr/share/nginx/html
