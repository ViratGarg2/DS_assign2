.PHONY: build_proto clean

build_proto:
	protoc --go_out=paths=source_relative:. \
	       --go-grpc_out=paths=source_relative:. \
	       proto/task_queue.proto

clean:
	rm -f proto/*.pb.go
