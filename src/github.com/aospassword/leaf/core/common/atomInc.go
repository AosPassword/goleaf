package common

import (
	"sync"
)
type AtomInc interface {
	Close()
	Inc(step int64) (int64,bool)
	String() string
	Set (start int64)
	Get() int64
}

type abstractChanAtomInc struct {
	start 		int64
	queue 		chan int64
	stop 		chan bool
	set  		chan int64
}


type stableAtomInc struct {
	rwMutex 	sync.RWMutex
	step 			int64
	queueBuffer  	int
	channelsBuffer 	int
	requestBuffer 	int
	channels		chan chan int64
	getRequest		chan chan int64
	ai 				*abstractChanAtomInc
}


//type customerAtomInc struct {
//	step chan int64
//	ai *abstractAtomInc
//}


func NewStableAtomInc(start,step int64,queueBuffer,channelsBuffer,requesteBuffer int)(sai *stableAtomInc)  {
	sai = &stableAtomInc{
		sync.RWMutex{},
		step,
		queueBuffer,
		channelsBuffer,
		requesteBuffer,
		make(chan chan int64,channelsBuffer),
		make(chan chan int64,requesteBuffer),
		&abstractChanAtomInc{
			start,
			make(chan int64,queueBuffer),
			make(chan bool),
			make(chan int64),
		},
	}
	go sai.process()
	return
}
// 单 goroutine 递增
func (sai *stableAtomInc) process() {
	defer func() {recover()}()
	for i := 0;i < sai.channelsBuffer;i++ {
		sai.channels <- make(chan int64)
	}

	i := sai.ai.start
	// for select default 中不应该写阻塞的方法

	for  {
		select {
		case <-sai.ai.stop:
			close(sai.ai.queue)
			close(sai.ai.stop)
			return
		case start := <- sai.ai.set:
			oldQueue := sai.ai.queue
			close(oldQueue)
			i = start
			sai.ai.queue = make(chan int64,sai.queueBuffer)
			sai.rwMutex.Unlock()
		case response := <-sai.getRequest:
			length := int64(len(sai.ai.queue))
			response <- i - length
		default:
			i += sai.step
			if len(sai.ai.queue) < sai.queueBuffer  {
				sai.ai.queue <- i
			}else {
				i -= sai.step
			}
		}
	}
}

func (sai *stableAtomInc)Close()  {
	sai.ai.stop <- true
}

func (sai *stableAtomInc)Inc(step int64) (int64,bool) {
	sai.rwMutex.RLock()
	defer sai.rwMutex.RUnlock()

	queue := sai.ai.queue
	value,ok := <- queue
	return value,ok
}

func (sai *stableAtomInc)Set(start int64)  {
	sai.rwMutex.Lock()

	sai.ai.set <- start
}
func (sai *stableAtomInc)Get() int64 {
	sai.rwMutex.RLock()
	defer sai.rwMutex.RUnlock()

	channel :=<- sai.channels
	sai.getRequest <- channel

	result := <- channel
	sai.channels <- channel

	return result
}

func (sai *stableAtomInc) String() string {
	string2 :=  "stableAutoInc { start :" + string(sai.ai.start) +
		",step :" + string(sai.step) + "}"
	return string2
}

//func NewCustomerAtomInc(start int64)(cai *customerAtomInc)  {
//	cai = &customerAtomInc{
//		make(chan int64),
//		&abstractAtomInc{
//			start: start,
//			stop: make(chan bool),
//			queue: make(chan int64),
//			set: make(chan int64),
//		},
//	}
//	go cai.process()
//	return
//}
//// 单 goroutine 递增
//func (cai *customerAtomInc) process() {
//	defer func() {recover()}()
//	i := cai.ai.start
//	for  {
//		select {
//		case <-cai.ai.stop:
//			close(cai.ai.queue)
//			close(cai.ai.stop)
//			close(cai.step)
//			close(cai.ai.set)
//			fmt.Println("close!!!!")
//			return
//		case start := <- cai.ai.set:
//			fmt.Printf("set start:%d \n",start)
//			i = start
//		default:
//			// 只有当前面的条件都不满足的时候，才会调用 default
//			// 当无人写入，或者无人读取时，会陷入阻塞
//			step := <- cai.step
//			i += step
//			cai.ai.queue <- i
//		}
//	}
//}
//
//func (cai *customerAtomInc)Close()  {
//	cai.ai.stop <- true
//}
//
//func (cai *customerAtomInc)Inc(step int64) (int64,bool) {
//	cai.step <- step
//	value,ok := <-cai.ai.queue
//	return value,ok
//}
//
//func (cai *customerAtomInc)Set(start int64)  {
//
//}
//
//func (cai *customerAtomInc) String() string {
//	string2 :=  "stableAutoInc { start :" + string(cai.ai.start) +
//		"}"
//	return string2
//}