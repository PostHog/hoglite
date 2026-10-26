FROM gcr.io/distroless/static-debian12
# bin-$TARGETARCH is populated by build-image.sh for each platform.
ARG TARGETARCH
COPY bin-${TARGETARCH}/hoglited /usr/local/bin/hoglited
ENTRYPOINT ["/usr/local/bin/hoglited"]
