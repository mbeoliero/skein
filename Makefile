.PHONY: test test-ci race lint tidy sqlc sqlc-check pg-start

test:
	go test ./...

test-ci:
	SKEIN_TEST_REQUIRE_DB=1 go test -count=1 ./...

race:
	go test -race -count=3 ./...

lint: sqlc-check
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go vet ./...

sqlc:
	sqlc generate

sqlc-check:
	sqlc diff

tidy:
	go mod tidy

pg-start:
	brew services start postgresql@18
