.PHONY: vendor
vendor:
	go mod vendor
	go mod tidy

.PHONY: test
test:
	go test -v ./...

.PHONY: lint
lint:
	golangci-lint run

