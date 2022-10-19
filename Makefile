BRANCH=`git rev-parse --abbrev-ref HEAD`
COMMIT=`git rev-parse --short HEAD`
GOLDFLAGS="-X main.branch $(BRANCH) -X main.commit $(COMMIT)"

race:
	@TEST_FREELIST_TYPE=hashmap go test -v -race -test.run="TestSimulate_(100op|1000op)"
	@echo "array freelist test"
	@TEST_FREELIST_TYPE=array go test -v -race -test.run="TestSimulate_(100op|1000op)"

fmt:
	!(gofmt -l -s -d $(shell find . -name \*.go) | grep '[a-z]')

lint:
	golangci-lint run ./...

test:
	TEST_FREELIST_TYPE=hashmap go test -timeout 20m -v -coverprofile cover.out -covermode atomic
	# Note: gets "program not an importable package" in out of path builds
	TEST_FREELIST_TYPE=hashmap go test -v ./cmd/bbolt

	@echo "array freelist test"

	@TEST_FREELIST_TYPE=array go test -timeout 20m -v -coverprofile cover.out -covermode atomic
	# Note: gets "program not an importable package" in out of path builds
	@TEST_FREELIST_TYPE=array go test -v ./cmd/bbolt

.PHONY: race fmt test lint
