FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/receiver ./cmd/receiver && CGO_ENABLED=0 go build -o /out/dispatcher ./cmd/dispatcher && CGO_ENABLED=0 go build -o /out/worker ./cmd/worker && CGO_ENABLED=0 go build -o /out/simulator ./cmd/simulator
FROM alpine:3.20
RUN adduser -D -H app
USER app
COPY --from=build /out/ /app/
ENTRYPOINT ["/app/receiver"]
