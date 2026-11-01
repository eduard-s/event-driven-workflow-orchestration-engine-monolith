FROM golang:1.27.3-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /workflow ./cmd/workflow

FROM build AS test
CMD ["go", "test", "-race", "-count=1", "-v", "./..."]

FROM scratch AS runtime
COPY --from=build /workflow /workflow
USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["/workflow"]
