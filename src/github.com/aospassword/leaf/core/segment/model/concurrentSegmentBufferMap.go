package model

import "fmt"

const (
	closeRequestTag int32 = iota
	getRequestTag
	deleteRequestTag
	putRequestTag
)

type ConcurrentSegmentBufferMap struct {
	requests			chan Request
	segmentMaps 		map[string]*SegmentBuffer
	results				chan struct{} 				// token 池
	keySet				chan []string
	keySetChan			chan RangeRequest
}

type Result struct {
	value 		*SegmentBuffer
	ok 			bool
}

type RangeRequest struct {
	payload		chan []string
}


type Request struct {
	key 		string
	tag			int32
	payload 	chan  *SegmentBuffer
}

func NewConcurrentSegmentBufferMapWithBuffer(requestBuffer,resultBuffer int,put func(key string) *SegmentBuffer) *ConcurrentSegmentBufferMap {
	csbMap := &ConcurrentSegmentBufferMap{
		make(chan Request,requestBuffer),
		make(map[string]*SegmentBuffer,10),
		make(chan struct{},resultBuffer),
		make(chan []string),
		make(chan RangeRequest),
	}
	fmt.Println("creating goroutine")
	go csbMap.process(put,resultBuffer)
	return csbMap
}

func NewDefaultConcurrentSegmentBufferMap(put func(key string) *SegmentBuffer) *ConcurrentSegmentBufferMap {
	return NewConcurrentSegmentBufferMapWithBuffer(50,50,put)
}

// 老规矩，单线程处理
func (csMap *ConcurrentSegmentBufferMap)process(put func(key string) *SegmentBuffer,buffer int)  {
	for i := 0;i < buffer; i++ {
		csMap.results <- struct{}{}
	}
	for  {
		select {
		case req := <- csMap.requests :
			csMap.handlerReq(req,put)
		case rc := <- csMap.keySetChan :
			keys := make([]string,10)
			for	key,_ := range csMap.segmentMaps {
				keys = append(keys,key)
			}
			rc.payload <- keys
		}
	}

}

func (csMap *ConcurrentSegmentBufferMap)handlerReq(req Request,put func(key string) *SegmentBuffer)  {
	switch req.tag {
	case getRequestTag:
		value,_ := csMap.segmentMaps[req.key]
		req.payload <- value
	case deleteRequestTag:
		value,ok := csMap.segmentMaps[req.key]
		if	ok {
			delete(csMap.segmentMaps,req.key)
		}
		req.payload <- value
	case putRequestTag:
		value,ok := csMap.segmentMaps[req.key]
		if !ok {
			value = put(req.key)
			csMap.segmentMaps[req.key] = value
		}
		req.payload <- value
	case closeRequestTag:
		req.payload <- nil
		close(csMap.requests)
		close(csMap.results)
		return
	}
}

func (csMap *ConcurrentSegmentBufferMap)Get(key string) (*SegmentBuffer,bool) {
	return csMap.handlerCore(key,getRequestTag)
}

func (csMap *ConcurrentSegmentBufferMap)Delete(key string) (*SegmentBuffer,bool) {

	return csMap.handlerCore(key,deleteRequestTag)
}

func (csMap *ConcurrentSegmentBufferMap)Put(key string) (*SegmentBuffer,bool) {
	return csMap.handlerCore(key,putRequestTag)
}

func (csMap *ConcurrentSegmentBufferMap)Close() bool {

	_,ok := csMap.handlerCore("",closeRequestTag)
	return ok
}

func (csMap *ConcurrentSegmentBufferMap)GetRange() []string {
	token := <-csMap.results

	rangReq := RangeRequest{
		payload: make(chan []string),
	}
	csMap.keySetChan <- rangReq
	result := <- rangReq.payload

	csMap.results <- token

	return result
}

func (csMap *ConcurrentSegmentBufferMap)handlerCore(key string,tag int32) (*SegmentBuffer,bool) {

	token := <-csMap.results


	response := make(chan *SegmentBuffer)

	csMap.requests <- Request{
		key: key,
		tag: tag,
		payload: response,
	}


	res,ok := <- response

	csMap.results <- token


	return res,ok
}

