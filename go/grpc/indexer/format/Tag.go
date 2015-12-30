// automatically generated, do not modify

package format

import (
	flatbuffers "github.com/google/flatbuffers/go"
)
type Tag struct {
	_tab flatbuffers.Table
}

func (rcv *Tag) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Tag) Name() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Tag) Offsets(j int) int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetInt64(a + flatbuffers.UOffsetT(j * 8))
	}
	return 0
}

func (rcv *Tag) OffsetsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func TagStart(builder *flatbuffers.Builder) { builder.StartObject(2) }
func TagAddName(builder *flatbuffers.Builder, name flatbuffers.UOffsetT) { builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(name), 0) }
func TagAddOffsets(builder *flatbuffers.Builder, offsets flatbuffers.UOffsetT) { builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(offsets), 0) }
func TagStartOffsetsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT { return builder.StartVector(8, numElems, 8)
}
func TagEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
