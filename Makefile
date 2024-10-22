.PHONY: build test test-integration

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-marketo.version=${VERSION}'" -o conduit-connector-marketo cmd/connector/main.go

test:
	# -p 1 ensures that tests run one at a time.
	# Each test cases takes upto 1-2 minutes to finish, since marketo API is slow. Also it takes a while to prepare snapshots by Marketo. So 30 Mins as overall timeout period.
	# But most of the time tests will finish in 10-15 mins.
	go test $(GOTEST_FLAGS) -p 1 -timeout 1h -v -race ./...

lint:
	golangci-lint run ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy
