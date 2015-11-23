// automatically generated, do not modify

package datafbs

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Header struct {
	_tab flatbuffers.Table
}

func (rcv *Header) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Header) TimeId() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Header) Offset() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Header) NodeId() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Header) Tags(j int) []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.ByteVector(a + flatbuffers.UOffsetT(j*4))
	}
	return nil
}

func (rcv *Header) TagsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func HeaderStart(builder *flatbuffers.Builder) { builder.StartObject(4) }
func HeaderAddTimeId(builder *flatbuffers.Builder, timeId uint64) {
	builder.PrependUint64Slot(0, timeId, 0)
}
func HeaderAddOffset(builder *flatbuffers.Builder, offset uint64) {
	builder.PrependUint64Slot(1, offset, 0)
}
func HeaderAddNodeId(builder *flatbuffers.Builder, nodeId uint64) {
	builder.PrependUint64Slot(2, nodeId, 0)
}
func HeaderAddTags(builder *flatbuffers.Builder, tags flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(tags), 0)
}
func HeaderStartTagsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func HeaderEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
