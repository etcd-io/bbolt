BRANCH=`git rev-parse --abbrev-ref HEAD`
COMMIT=`git rev-parse --short HEAD`
GOLDFLAGS="-X main.branch $(BRANCH) -X main.commit $(COMMIT)"

TESTFLAGS_RACE=-race=false
ifdef ENABLE_RACE
	TESTFLAGS_RACE=-race=true
endif

TESTFLAGS_CPU=
ifdef CPU
	TESTFLAGS_CPU=-cpu=$(CPU)
endif

TESTFLAGS = $(TESTFLAGS_RACE) $(TESTFLAGS_CPU)

fmt:
	!(gofmt -l -s -d $(shell find . -name \*.go) | grep '[a-z]')

lint:
	golangci-lint run ./...

test:
	@echo "hashmap freelist test"
	TEST_FREELIST_TYPE=hashmap go test -v ${TESTFLAGS} -timeout 30m
	TEST_FREELIST_TYPE=hashmap go test -v ${TESTFLAGS} ./cmd/bbolt

	@echo "array freelist test"
	TEST_FREELIST_TYPE=array go test -v ${TESTFLAGS} -timeout 30m
	TEST_FREELIST_TYPE=array go test -v ${TESTFLAGS} ./cmd/bbolt

test-simulate:
	@echo "hashmap freelist test"
	TEST_FREELIST_TYPE=hashmap go test -v ${TESTFLAGS} -test.run="TestSimulate_(100op|1000op)"

	@echo "array freelist test"
	TEST_FREELIST_TYPE=array go test -v ${TESTFLAGS} -test.run="TestSimulate_(100op|1000op)"

coverage:
	@echo "hashmap freelist test"
	TEST_FREELIST_TYPE=hashmap go test -v -timeout 30m \
		-coverprofile cover-freelist-hashmap.out -covermode atomic

	@echo "array freelist test"
	TEST_FREELIST_TYPE=array go test -v -timeout 30m \
		-coverprofile cover-freelist-array.out -covermode atomic

.PHONY: fmt test test-simulate lint
