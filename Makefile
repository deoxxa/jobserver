all: jobserverc jobserverd

INTERNAL_SOURCES := $(shell find internal -name '*.go')
JOBSERVERC_SOURCES := cmd/jobserverc/main.go jobserver.go $(INTERNAL_SOURCES)
JOBSERVERD_SOURCES := cmd/jobserverd/main.go $(INTERNAL_SOURCES)

jobserverc: $(JOBSERVERC_SOURCES)
	GOPATH="$(shell pwd)/Godeps/_workspace:${GOPATH}" go build ./cmd/jobserverc

jobserverd: $(JOBSERVERD_SOURCES)
	GOPATH="$(shell pwd)/Godeps/_workspace:${GOPATH}" go build ./cmd/jobserverd

clean:
	rm -rf jobserverc jobserverd
