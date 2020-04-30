package model

import "github.com/sirupsen/logrus"

func (b *SegmentBuffer)Switch() {
	logrus.Infof("switch from segment {max:%d,step:%d} to segment {max:%d,step:%d}",b.Current.Max,b.Current.Step,b.Next.Max,b.Next.Step)

	b.Current.AutoInc.Close()
	b.Current = b.Next
	b.InitOK = b.NextReady
	b.Next = NewSegment(b)
	b.NextReady = false
	logrus.Infof("switch end！")

	return
}


func (b *SegmentBuffer)String() string {
	return b.Key
}