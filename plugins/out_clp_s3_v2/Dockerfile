# NOTE: Match debian version from Fluent Bit image [Fluent Bit Debian version] to prevent glibc
# compatibility issues.
# [Fluent Bit Debian version]: https://github.com/fluent/fluent-bit/blob/master/dockerfiles/Dockerfile
FROM golang:1.24.3-bookworm as builder

# install task
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /bin

WORKDIR /root

ENV GOOS=linux\
    GOARCH=amd64

COPY / /root/

RUN go mod download

WORKDIR /root/plugins/out_clp_ir_file

RUN task build

FROM fluent/fluent-bit:3.2.7

COPY --from=builder /root/plugins/out_clp_s3_v2/out_clp_s3_v2.so /fluent-bit/lib/
COPY --from=builder /root/plugins/out_clp_s3_v2/*.yaml /fluent-bit/etc/

CMD ["/fluent-bit/bin/fluent-bit", "--config", "/fluent-bit/etc/fluent-bit.yaml"]
