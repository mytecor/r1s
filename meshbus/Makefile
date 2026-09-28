.PHONY: check fmt race test vet

check: vet race

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './**/testdata/*')

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...
