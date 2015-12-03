package msgpb

// make sure you have gogofast installed:
//    go get github.com/gogo/protobuf/protoc-gen-gofast

//go:generate protoc --gogofaster_out=import_path=msgpb:.  -I. msg.proto
