.PHONY: check docs-check fmt generate generate-check install-hooks protoc-check test race

MODULE := github.com/mytecor/r1s
PROTO_DIR := api/proto
PROTO_FILES := r1s/v1/control.proto
PROTOC_INCLUDES := -I$(PROTO_DIR) -I/opt/homebrew/include
PROTOC_GEN_GO := bin/protoc-gen-go
PROTOC_GEN_GO_VERSION := v1.36.11
PROTOC_GEN_GO_GRPC := bin/protoc-gen-go-grpc
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
PROTOC_VERSION := 36.0

check: generate-check race docs-check

generate: protoc-check $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC)
	protoc $(PROTOC_INCLUDES) --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --go_out=. --go_opt=module=$(MODULE) $(PROTO_FILES)
	protoc $(PROTOC_INCLUDES) --plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC) --go-grpc_out=. --go-grpc_opt=module=$(MODULE) $(PROTO_FILES)

generate-check: protoc-check $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC)
	@tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	protoc $(PROTOC_INCLUDES) --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --go_out="$$tmp" --go_opt=module=$(MODULE) $(PROTO_FILES); \
	protoc $(PROTOC_INCLUDES) --plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC) --go-grpc_out="$$tmp" --go-grpc_opt=module=$(MODULE) $(PROTO_FILES); \
	diff -ru api/gen "$$tmp/api/gen"

protoc-check:
	@test "$$(protoc --version)" = "libprotoc $(PROTOC_VERSION)" || { \
		echo "protoc $(PROTOC_VERSION) is required"; \
		exit 1; \
	}

$(PROTOC_GEN_GO): Makefile
	mkdir -p bin
	GOBIN=$(CURDIR)/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

$(PROTOC_GEN_GO_GRPC): Makefile
	mkdir -p bin
	GOBIN=$(CURDIR)/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './**/testdata/*' -not -path './.git/*')
	@echo "gofmt applied"

# Install the repository pre-commit hook that verifies staged Go files are
# gofmt-clean, without needing a third-party hook manager.
install-hooks:
	git config core.hooksPath .githooks
	@echo "pre-commit hook installed (core.hooksPath=.githooks)"

race:
	go test -race ./...

test:
	go test ./...

docs-check:
	lychee --no-progress --offline --include-fragments=full .
