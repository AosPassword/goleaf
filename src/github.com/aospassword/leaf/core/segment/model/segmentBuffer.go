package model

import "sync"

type SegmentBuffer struct {
	key 			string
	segments    	[2]*Segment
	currentPos		int
	nextReady  		bool
	initOK			bool
	Running			int32
	RWMutex			sync.RWMutex


	Step			int64
	MinStep			int64
	UpdateTimestamp int64
}


func (b *SegmentBuffer)String() string {
	return b.key
}

// 线程不安全，需要读锁包裹
func (b *SegmentBuffer) GetCurrent() *Segment {
	return b.segments[b.currentPos]
}

func NewSegmentBuffer(key string) *SegmentBuffer {
	segBuf := &SegmentBuffer{
		key: key,
		currentPos: 0,
		nextReady: false,
		initOK: false,
		Running: 0,
		RWMutex: sync.RWMutex{},
	}
	segBuf.segments[0] = NewSegment(segBuf)
	segBuf.segments[1] = NewSegment(segBuf)
	return segBuf
}

func (b *SegmentBuffer)String() string {
	return b.key
}

// 线程不安全，需要读锁包裹
func (b *SegmentBuffer) IsInitOk() (bo bool) {
	bo = b.initOK
	return
}

// 线程不安全，需要读锁包裹
func (b *SegmentBuffer) IsNextReady() (bo bool) {
	bo = b.nextReady
	return
}


// 线程不安全，需要写锁
func (b *SegmentBuffer) SetInitOk(b2 bool) {
	b.initOK = b2
}

// 线程不安全，需要写锁
func (b *SegmentBuffer) SetNextReady(b2 bool) {
	b.nextReady = b2
}
