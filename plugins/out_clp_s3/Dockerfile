# Builds plugin binary in go container and then runs in fluent-bit container

# using bullseye tag to match debian version from fluent-bit image [fluent-bit Debian version]
# same debian version prevents glibc compatibility issues
# [fluent-bit Debian version]: https://github.com/fluent/fluent-bit/blob/master/dockerfiles/Dockerfile
FROM golang:1.22.3-bullseye as builder

# install task
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /bin

WORKDIR /root

ENV GOOS=linux\
    GOARCH=amd64

COPY / /root/

RUN go mod download

WORKDIR /root/plugins/out_clp_s3

RUN task build

FROM fluent/fluent-bit:3.0.6

#copy plugin binary to fluent-bit image
COPY --from=builder /root/plugins/out_clp_s3/out_clp_s3.so /fluent-bit/bin/
COPY --from=builder /root/plugins/out_clp_s3/*.conf /fluent-bit/etc/

#port for listening interface for HTTP Server
EXPOSE 2020

CMD ["/fluent-bit/bin/fluent-bit", "--config", "/fluent-bit/etc/fluent-bit.conf"]
