FROM golang:1.25.3-alpine AS build
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /tarstream

FROM alpine:latest
WORKDIR /backup
RUN mkdir -p /backup /archive
COPY --from=build /tarstream /tarstream
ENTRYPOINT ["/tarstream"]
CMD ["-src", "/backup", "-out", "/archive/backup-%Y-%m-%d-%H-%M-%S.tar.gz", "-keep", "3"]
