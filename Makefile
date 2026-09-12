.PHONY: test build up down

test:
	go test ./...

build:
	go build ./cmd/receiver ./cmd/dispatcher ./cmd/worker ./cmd/simulator

up:
	docker compose up --build

down:
	docker compose down
