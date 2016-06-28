ifndef TOOLS
# NOTE: The following does not work if the path to the Makefile contains spaces.
TOOLS = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
endif

all: enterprise

clean:
	rm -rf "${TOOLS}/go/src/github.com"
	rm -rf "${TOOLS}/go/src/golang.org"
	rm -rf "${TOOLS}/go/src/v.io"

test: godeps
	GOPATH="${TOOLS}/go" go test alluxio.org/alluxio/...

godeps:
	GOPATH="${TOOLS}/go" go get -d -v v.io/x/lib/...

enterprise: godeps
	GOPATH="${TOOLS}/go" go install alluxio.org/alluxio/enterprise

lint: enterprise
	echo "Checking enterprise annotations ..."
	"${TOOLS}/go/bin/enterprise" -repo "${TOOLS}/.." lint -fail-on-warning
