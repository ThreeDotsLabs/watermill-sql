up:
	docker-compose up

test:
	go test ./...

test_v:
	go test -v ./...

test_short:
	go test ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -tags=stress -timeout=30m ./...

test_reconnect:
	go test -tags=reconnect ./...

validate_examples:
	go run dev/update-examples-deps/main.go
	bash dev/validate_examples.sh


fmt:
	go fmt ./...
	goimports -l -w .

generate_gomod: fmt
	rm go.mod go.sum || true
	go mod init github.com/ThreeDotsLabs/watermill-sql
# todo - change to last release
	go get github.com/ThreeDotsLabs/watermill@moved-pubsubs

	find . -type f -iname '*.go' -exec sed -i -E "s/github\.com\/ThreeDotsLabs\/watermill\/message\/infrastructure\/(amqp|googlecloud|http|io|kafka|nats|sql)/github.com\/ThreeDotsLabs\/watermill-\1\/pkg\/\1/" "{}" +;

	go install ./...
	sed -i '\|go |d' go.mod
	go mod edit -fmt

mycli:
	@mycli -h 127.0.0.1 -u root -p secret

