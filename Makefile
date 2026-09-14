.PHONY: check docs-check generate generate-check protoc-check test race

MODULE := github.com/mytecor/r1s
PROTO_FILES := api/proto/r1s/v1/control.proto
PROTOC_GEN_GO := bin/protoc-gen-go
PROTOC_GEN_GO_VERSION := v1.36.11
PROTOC_VERSION := 36.0

check: generate-check race docs-check

generate: protoc-check $(PROTOC_GEN_GO)
	protoc --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --go_out=. --go_opt=module=$(MODULE) $(PROTO_FILES)

generate-check: protoc-check $(PROTOC_GEN_GO)
	@tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	protoc --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --go_out="$$tmp" --go_opt=module=$(MODULE) $(PROTO_FILES); \
	diff -ru api/gen "$$tmp/api/gen"

protoc-check:
	@test "$$(protoc --version)" = "libprotoc $(PROTOC_VERSION)" || { \
		echo "protoc $(PROTOC_VERSION) is required"; \
		exit 1; \
	}

$(PROTOC_GEN_GO): Makefile
	mkdir -p bin
	GOBIN=$(CURDIR)/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

test:
	go test ./...

race:
	go test -race ./...

docs-check:
	lychee --no-progress --offline --include-fragments=full .
