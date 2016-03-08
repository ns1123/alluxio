ALLUXIO_SCRIPTS_HOME = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

all: enterprise

clean:
	rm -rf "${ALLUXIO_SCRIPTS_HOME}/go/src/github.com"
	rm -rf "${ALLUXIO_SCRIPTS_HOME}/go/src/golang.org"
	rm -rf "${ALLUXIO_SCRIPTS_HOME}/go/src/v.io"

godeps:
	GOPATH="${ALLUXIO_SCRIPTS_HOME}/go" go get -d -v v.io/x/lib/...

enterprise: godeps
	GOPATH="${ALLUXIO_SCRIPTS_HOME}/go" go install alluxio.org/alluxio/enterprise
