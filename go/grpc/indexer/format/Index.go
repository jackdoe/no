// automatically generated, do not modify

package format

import (
	flatbuffers "github.com/google/flatbuffers/go"
)
type Index struct {
	_tab flatbuffers.Table
}

func GetRootAsIndex(buf []byte, offset flatbuffers.UOffsetT) *Index {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Index{}
	x.Init(buf, n + offset)
	return x
}

func (rcv *Index) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Index) Tags(obj *Tag, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
	if obj == nil {
		obj = new(Tag)
	}
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Index) TagsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func IndexStart(builder *flatbuffers.Builder) { builder.StartObject(1) }
func IndexAddTags(builder *flatbuffers.Builder, tags flatbuffers.UOffsetT) { builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(tags), 0) }
func IndexStartTagsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT { return builder.StartVector(4, numElems, 4)
}
func IndexEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
