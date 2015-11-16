// Code generated by protoc-gen-go.
// source: data.proto
// DO NOT EDIT!

/*
Package data is a generated protocol buffer package.

It is generated from these files:
	data.proto

It has these top-level messages:
	Payload
	Header
	Data
*/
package datapb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type Payload struct {
	Data             []byte  `protobuf:"bytes,1,req,name=data" json:"data,omitempty"`
	Id               *string `protobuf:"bytes,2,opt,name=id" json:"id,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Payload) Reset()         { *m = Payload{} }
func (m *Payload) String() string { return proto.CompactTextString(m) }
func (*Payload) ProtoMessage()    {}

func (m *Payload) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Payload) GetId() string {
	if m != nil && m.Id != nil {
		return *m.Id
	}
	return ""
}

type Header struct {
	TimeId           *uint64  `protobuf:"varint,1,req,name=time_id" json:"time_id,omitempty"`
	Offset           *uint64  `protobuf:"varint,2,req,name=offset" json:"offset,omitempty"`
	NodeId           *uint64  `protobuf:"varint,3,req,name=node_id" json:"node_id,omitempty"`
	Tags             []string `protobuf:"bytes,4,rep,name=tags" json:"tags,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Header) Reset()         { *m = Header{} }
func (m *Header) String() string { return proto.CompactTextString(m) }
func (*Header) ProtoMessage()    {}

func (m *Header) GetTimeId() uint64 {
	if m != nil && m.TimeId != nil {
		return *m.TimeId
	}
	return 0
}

func (m *Header) GetOffset() uint64 {
	if m != nil && m.Offset != nil {
		return *m.Offset
	}
	return 0
}

func (m *Header) GetNodeId() uint64 {
	if m != nil && m.NodeId != nil {
		return *m.NodeId
	}
	return 0
}

func (m *Header) GetTags() []string {
	if m != nil {
		return m.Tags
	}
	return nil
}

type Data struct {
	Header           *Header    `protobuf:"bytes,1,req,name=header" json:"header,omitempty"`
	Frames           []*Payload `protobuf:"bytes,2,rep,name=frames" json:"frames,omitempty"`
	XXX_unrecognized []byte     `json:"-"`
}

func (m *Data) Reset()         { *m = Data{} }
func (m *Data) String() string { return proto.CompactTextString(m) }
func (*Data) ProtoMessage()    {}

func (m *Data) GetHeader() *Header {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *Data) GetFrames() []*Payload {
	if m != nil {
		return m.Frames
	}
	return nil
}

func init() {
	proto.RegisterType((*Payload)(nil), "Payload")
	proto.RegisterType((*Header)(nil), "Header")
	proto.RegisterType((*Data)(nil), "Data")
}