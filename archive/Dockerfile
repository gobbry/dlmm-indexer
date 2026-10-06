# syntax=docker/dockerfile:1
# Old Faithful archive server: the release binary over the hardcoded epoch configs. Only a
# linux/amd64 release exists, so Apple Silicon runs it under emulation; it is network-bound,
# so that costs nothing noticeable.
FROM debian:bookworm-slim
ARG FAITHFUL_VERSION=v0.7.28
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location --silent --show-error \
         --output /usr/local/bin/faithful-cli \
         "https://github.com/rpcpool/yellowstone-faithful/releases/download/${FAITHFUL_VERSION}/faithful-cli_linux_amd64" \
    && chmod +x /usr/local/bin/faithful-cli
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
EXPOSE 8899
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
