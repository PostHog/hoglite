# The build stage runs on the host platform and cross-compiles for
# the target platform. QEMU runs only the final stage for arm64.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH \
    go build -trimpath -o /out/hoglited ./cmd/hoglited
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH \
    go build -trimpath -o /out/hoglake-reimpl ./cmd/hoglake-reimpl

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/hoglited /usr/local/bin/hoglited
COPY --from=build /out/hoglake-reimpl /usr/local/bin/hoglake-reimpl
ENTRYPOINT ["/usr/local/bin/hoglited"]
