package datapb

// make sure you have gogofast installed:
//    go get github.com/gogo/protobuf/protoc-gen-gofast

//go:generate protoc --gogofast_out=import_path=datapb:.  -I../.. ../../data.proto
