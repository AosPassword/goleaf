package model

import (
	"leaf/core/common"
	"sync"
	"time"
)

type Segment struct {
	AutoInc		common.AtomInc
	Max 		int64
	Step		int64
	buffer  	*SegmentBuffer
	RWMutex     *sync.RWMutex
}

func NewSegment(buffer *SegmentBuffer) *Segment {
	return &Segment{
		common.NewStableAtomInc(0,1,40,40,40),
		0,
		0,
		buffer,
		&sync.RWMutex{},
	}
}


func (s *Segment)GetBuffer() *SegmentBuffer {
	return s.buffer
}

func (s *Segment)GetIdle() int64 {
	return s.Max - s.Get()
}


func (s *Segment)String() string  {
	str := "Segment{" + "value:" + s.AutoInc.String() + "}"
	return str
}

func (s *Segment)Get() int64 {
	return s.AutoInc.Get()
}

func (s *Segment)IncAndGet(step int64) int64 {
	var value int64
	var ok  = false
	var num = 0
	for !ok {
		num ++
		if num > 10000{
			time.Sleep(1 * time.Millisecond)
		}
		value ,ok = s.AutoInc.Inc(step)
	}
	return value
}
