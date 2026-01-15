# NOTE: Match debian version from Fluent Bit image [Fluent Bit Debian version] to prevent glibc
# compatibility issues.
# [Fluent Bit Debian version]: https://github.com/fluent/fluent-bit/blob/master/dockerfiles/Dockerfile
FROM golang:1.24.3-bookworm AS builder

# Install task
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /bin

WORKDIR /root

ENV GOOS=linux \
    GOARCH=amd64

COPY . .

RUN git init && git submodule update --init --recursive

RUN go mod download

WORKDIR /root/plugins/out_clp_s3

RUN task build:s3

FROM fluent/fluent-bit:3.2.7

COPY --from=builder /root/pre-built/out_clp_s3_linux_amd64.so /fluent-bit/lib/out_clp_s3.so
COPY --from=builder /root/plugins/out_clp_s3/*.conf /fluent-bit/etc/

EXPOSE 2020

CMD ["/fluent-bit/bin/fluent-bit", "--config", "/fluent-bit/etc/fluent-bit.conf"]
