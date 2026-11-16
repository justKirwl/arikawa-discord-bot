FROM golang:1.27.4-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY bot ./bot
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bot ./bot

FROM alpine:3

RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 10001 bot
USER bot

COPY --from=build /out/bot /usr/local/bin/bot

ENTRYPOINT ["bot"]
