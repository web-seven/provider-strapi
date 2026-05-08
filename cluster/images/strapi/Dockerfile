# Builds a self-contained Strapi v4 dev/test image. The Strapi project itself
# is bootstrapped at build time via `create-strapi-app`, so the resulting
# image has the full app + admin panel build baked in — no scaffolding step
# at container start, just `npm start` after seeding the admin user.
#
# Storage is sqlite (file under .tmp/data.db). The image is intended for the
# `make serve` dev loop and `cluster/charts/strapi` Helm chart, NOT for
# production. Production-quality Strapi lives in the digando chart.

ARG NODE_VERSION=18-alpine
ARG STRAPI_VERSION=4.20.3

FROM node:${NODE_VERSION} AS bootstrap
WORKDIR /opt/strapi
# Native build deps for sharp / sqlite3 / etc.
RUN apk add --no-cache build-base gcompat python3 git
# Generate a complete Strapi project + run admin panel build. --no-run keeps
# the boot from happening here; we only want the artifacts.
RUN npx --yes create-strapi-app@${STRAPI_VERSION} . \
        --no-run \
        --skip-cloud \
        --use-npm \
        --dbclient=sqlite \
        --dbfile=./.tmp/data.db \
 && npm run build

FROM node:${NODE_VERSION}
WORKDIR /opt/strapi
RUN apk add --no-cache tini
COPY --from=bootstrap /opt/strapi /opt/strapi

ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=1337
EXPOSE 1337

# Admin user creation is the chart's responsibility (post-install Job hitting
# /admin/register-admin), not the image's. This image just runs the server.
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["npm", "start"]
