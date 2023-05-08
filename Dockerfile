FROM golang:1.19 as builder

RUN mkdir /src
WORKDIR /src
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.76.1
COPY otelcol-builder.yaml .
COPY exporter exporter
RUN CGO_ENABLED=0 builder --config otelcol-builder.yaml

FROM alpine:latest

RUN apk --update add ca-certificates

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=builder /src/dist/otelcol-custom /otelcol-custom
EXPOSE 4317
ENTRYPOINT ["/otelcol-custom"]
CMD ["--config", "/etc/otel/config.yaml"]
