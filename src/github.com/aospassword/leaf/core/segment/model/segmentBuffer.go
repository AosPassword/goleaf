package model

import (
	"sync"
)

type SegmentBuffer struct {
	Key 			string
	Current    	    *Segment
	Next 			*Segment
	NextReady  		bool
	InitOK			bool
	Running			int32
	RWMutex			sync.RWMutex

	Step			int64
	MinStep			int64
	UpdateTimestamp int64
}


func NewSegmentBuffer(key string) *SegmentBuffer {

	segBuf := &SegmentBuffer{
		Key: key,
		NextReady: false,
		InitOK: false,
		Running: 0,
		RWMutex: sync.RWMutex{},
	}
	segBuf.Current = NewSegment(segBuf)

	segBuf.Next = NewSegment(segBuf)
	return segBuf
}
