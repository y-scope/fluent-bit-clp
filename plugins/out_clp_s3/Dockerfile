# Builds plugin binary in go container and then runs in Fluent Bit container.

# Using bullseye tag to match debian version from Fluent Bit image [Fluent Bit Debian version].
# Matching debian versions prevents glibc compatibility issues.
# [Fluent Bit Debian version]: https://github.com/fluent/fluent-bit/blob/master/dockerfiles/Dockerfile
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

# Copy plugin binary to Fluent Bit image. 
COPY --from=builder /root/plugins/out_clp_s3/out_clp_s3.so /fluent-bit/bin/
COPY --from=builder /root/plugins/out_clp_s3/*.conf /fluent-bit/etc/

# Port for listening interface for HTTP Server.
EXPOSE 2020

CMD ["/fluent-bit/bin/fluent-bit", "--config", "/fluent-bit/etc/fluent-bit.conf"]
