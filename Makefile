GOPATH:=$(PWD):${GOPATH}
export GOPATH

all: build

build:
	go clean; rm -rf pkg; go build

build_osx:
	go clean; rm -rf pkg; GOOS=darwin go build

build_linux:
	go clean; rm -rf pkg; GOOS=linux go build

install:
	go install

clean:
	go clean; rm -rf pkg

test : test1

test1:
	cd test; go test
