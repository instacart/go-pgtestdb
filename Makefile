.PHONY: test vet

# `go test` already runs a subset of vet (printf, tests, ...) against every
# package it builds. `go vet` adds the rest -- copylocks, lostcancel,
# unusedresult -- which do not run during a test build.
test: vet
	go test ./...

vet:
	go vet ./...
