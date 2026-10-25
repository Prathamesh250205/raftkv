FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/...

FROM alpine:3.20
RUN apk add --no-cache curl
COPY --from=build /out/ /usr/local/bin/
VOLUME /data
EXPOSE 8000
ENTRYPOINT ["kvnode"]
