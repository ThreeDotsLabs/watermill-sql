up:
	docker compose up -d

test:
	go test -timeout=30m ./...

test_v:
	go test -v ./...

test_short:
	go test -timeout=30m -short ./...

test_race:
	go test ./... -short -race

test_stress:

test_codecov:


test_reconnect:
	go test -tags=reconnect ./...

wait:
	go run github.com/ThreeDotsLabs/wait-for@latest localhost:3306 localhost:5432
	go run ./internal/wait-for

build:
	go build ./...

fmt:
	go fmt ./...
	goimports -l -w .

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

mycli:
	@mycli -h 127.0.0.1 -u root watermill

pgcli:
	@pgcli postgres://watermill:password@localhost:5432/watermill?sslmode=disable
