up:
	docker-compose up

test:
	go test -parallel 20 ./...

test_v:
	go test -parallel 20 -v ./...

test_short:
	go test -parallel 20 ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -tags=stress -parallel 30 -timeout=45m ./...

test_reconnect:
	go test -tags=reconnect ./...

fmt:
	go fmt ./...
	goimports -l -w .

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

mycli:
	@mycli -h 127.0.0.1 -u root -p secret

