@0xca20eaaf0cd45507;

using Go = import "/zombiezen.com/go/capnproto2/go.capnp";
$Go.package("api");
$Go.import("github.com/jackdoe/no/go/grpc/indexer/format");

struct Tag {
    name @0: Text;
    offsets @1: List(Int64);
}

struct Index {
    tags @0: List(Tag);
}
