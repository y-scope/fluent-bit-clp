# Build with manylinux for portable binaries compatible with Fluent Bit's runtime.
# manylinux_2_28 uses glibc 2.28, ensuring compatibility with most Linux distributions.
#
# Note: This plugin only supports amd64 architecture.
FROM quay.io/pypa/manylinux_2_28_x86_64 AS builder

# Install Go
ARG GO_VERSION=1.24.3
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" | tar -C /usr/local -xz
ENV PATH="/usr/local/go/bin:${PATH}"

# Install task
RUN curl -fsSL https://taskfile.dev/install.sh | sh -s -- -d -b /usr/local/bin

WORKDIR /src

ENV CGO_ENABLED=1

COPY . .

RUN git init && git submodule update --init --recursive

# Download pre-built clp-ffi-go library from GitHub releases
RUN bash third-party/clp-ffi-go/scripts/download-libs.sh

RUN task build:s3

FROM fluent/fluent-bit:3.2.7

COPY --from=builder /src/pre-built/out_clp_s3_linux_amd64.so /fluent-bit/lib/out_clp_s3.so
COPY --from=builder /src/plugins/out_clp_s3/*.conf /fluent-bit/etc/

EXPOSE 2020

CMD ["/fluent-bit/bin/fluent-bit", "--config", "/fluent-bit/etc/fluent-bit.conf"]
