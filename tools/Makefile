TOOLS = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

all: enterprise

clean:
	rm -rf "${TOOLS}/go/src/github.com"
	rm -rf "${TOOLS}/go/src/golang.org"
	rm -rf "${TOOLS}/go/src/v.io"

godeps:
	GOPATH="${TOOLS}/go" go get -d -v v.io/x/lib/...

enterprise: godeps
	GOPATH="${TOOLS}/go" go install alluxio.org/alluxio/enterprise

lint: enterprise
	echo "Checking enterprise annotations ..."
	${TOOLS}/go/bin/enterprise -repo ${REPO} lint -fail-on-warning
