// automatically generated, do not modify

package datafbs

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Payload struct {
	_tab flatbuffers.Table
}

func (rcv *Payload) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Payload) Data() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Payload) Id() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func PayloadStart(builder *flatbuffers.Builder) { builder.StartObject(2) }
func PayloadAddData(builder *flatbuffers.Builder, data flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(data), 0)
}
func PayloadAddId(builder *flatbuffers.Builder, id flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(id), 0)
}
func PayloadEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
