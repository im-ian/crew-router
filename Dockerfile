# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked \
    && mkdir /empty-data

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/crew-router /usr/local/bin/crew-router
COPY --from=build --chown=65532:65532 /empty-data /data
ENV CREW_ROUTER_DATA=/data \
    CREW_ROUTER_LISTEN=0.0.0.0:7420
VOLUME /data
EXPOSE 7420
ENTRYPOINT ["/usr/local/bin/crew-router"]
CMD ["serve"]
