FROM node:24-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ARG MMPF_CONSOLE_BASE_PATH=""
ENV MMPF_CONSOLE_BASE_PATH=${MMPF_CONSOLE_BASE_PATH}
RUN npm run check && npm test && npm run build

FROM nginx:1.29-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY docker-entrypoint.d/40-console-config.sh /docker-entrypoint.d/40-console-config.sh
RUN chmod 0755 /docker-entrypoint.d/40-console-config.sh
COPY --from=build /src/build/ /usr/share/nginx/html/
EXPOSE 8080
