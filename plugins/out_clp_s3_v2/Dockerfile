# Build with manylinux for portable binaries compatible with Fluent Bit's runtime.
# manylinux_2_28 uses glibc 2.28, ensuring compatibility with most Linux distributions.
FROM quay.io/pypa/manylinux_2_28_x86_64 AS builder-amd64
FROM quay.io/pypa/manylinux_2_28_aarch64 AS builder-arm64

# Select the right base image based on target platform
ARG TARGETARCH
FROM builder-${TARGETARCH} AS builder

# Install Go
ARG TARGETARCH
ARG GO_VERSION=1.24.3
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" | tar -C /usr/local -xz
ENV PATH="/usr/local/go/bin:${PATH}"

# Install task
RUN curl -fsSL https://taskfile.dev/install.sh | sh -s -- -d -b /usr/local/bin

WORKDIR /src

ENV CGO_ENABLED=1

COPY . .

RUN git init && git submodule update --init --recursive

# Download pre-built clp-ffi-go library from GitHub releases
RUN bash third-party/clp-ffi-go/scripts/download-libs.sh

RUN task build:s3v2

FROM fluent/fluent-bit:3.2.7

COPY --from=builder /src/pre-built/out_clp_s3_v2_linux_*.so /fluent-bit/plugins/out_clp_s3_v2.so
COPY --from=builder /src/plugins/out_clp_s3_v2/*.yaml /fluent-bit/etc/

CMD ["/fluent-bit/bin/fluent-bit", "--config", "/fluent-bit/etc/fluent-bit.yaml"]
