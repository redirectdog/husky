FROM alpine:3.10 AS builder
RUN apk add --no-cache rust cargo
WORKDIR /usr/src/husky
COPY Cargo.* ./
COPY src ./src
RUN cargo build --release

FROM alpine:3.10
RUN apk add --no-cache libgcc
COPY --from=builder /usr/src/husky/target/release/husky /usr/bin/
CMD ["husky"]
