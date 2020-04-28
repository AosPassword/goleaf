package segment

import (
	"errors"
	"github.com/aospassword/leaf/core/common"
	"github.com/aospassword/leaf/core/segment/dao"
	"github.com/aospassword/leaf/core/segment/dao/impls"
	"github.com/aospassword/leaf/core/segment/model"
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxStep = 1000000
	// 单位 ms
	segmentDuration = int64(15 * time.Minute / 1e6)
	exceptionIDCacheInitFalse int64 = -1
	exceptionIDKeyNotExists = -2
	exceptionIDTwoSegmentsAreNull = -3
)

type SegmentIDGenImpl struct {
	initOK 	bool
	cache	sync.Map
	dao		dao.IDAllocDao
	close   chan bool
	rwMutex sync.RWMutex
}

// 搜索数据库，对比数据库 alloc 的 all Tags，如果 cache 的 key 和 tags 不相等
// 则将数据库的 tags 灌入到 cache 中
func (idGen *SegmentIDGenImpl)Initialization() (err error) {
	logrus.Info("SegmentIDGenImpl Init...")
	idGen.updateCacheFromDB()

	idGen.rwMutex.Lock()
	idGen.initOK = true
	idGen.rwMutex.Unlock()

	go idGen.updateCacheFromDbAtEveryMinute()
	return
}


func (idGen *SegmentIDGenImpl)updateCacheFromDB() {
	logrus.Info("update cache from db")
	// 数据库中的所有 tags
	tags,err := impls.DefaultIDAllocDaoBean.GetAllTags()
	if	err != nil {
		logrus.Errorf("GetAllTags() error")
		return
	}
	// tags 的深拷贝，用于向 cache 中填充 新增 的数据
	insertTagsMap := make(map[string]bool)
	// cache 的深拷贝，用于删除cache中 失效 的tag
	removeTagsMap := make(map[string]bool)

	// 放到 map 中去，方便判断 tag 是否存在
	for _,s := range tags{
		insertTagsMap[s] = true
	}


	cache := idGen.cache


	//将 cache 中已经存在的 tag 移出 insertTagsMap，为新建 Segment Buffer 做准备
	for	_,k:= range cache {
		_,ok := insertTagsMap[k]
		if ok {
			delete(insertTagsMap,k)
		}
		removeTagsMap[k] = true
	}

	// 向 cache 中填充 buffer
	// buffer 于此处初始化
	for k,_ := range insertTagsMap {

		buffer,ok := idGen.cache.Put(k)
		if	ok {
			logrus.Info("add tag:" + k + "from db to IdCache,SegmentBuffer" + buffer.String())
		}else {
			logrus.Warn("tag:" + k + "already was in IdCache,SegmentBuffer" + buffer.String())
		}

	}

	// 移除过期的 tags
	for _,tag := range tags {
		_,ok := removeTagsMap[tag]
		if	ok {
			delete(removeTagsMap,tag)
		}
	}

	for k,_ := range removeTagsMap{
		idGen.cache.Delete(k)
		logrus.Info("Remove tag:"+ k +" from IdCache")
	}

}

func (idGen *SegmentIDGenImpl)updateCacheFromDbAtEveryMinute()  {
	ticker := time.NewTicker(time.Minute * time.Duration(1))
	for  {
		<- ticker.C
		go idGen.updateCacheFromDB()
	}
}

// 核心方法它lei了！
func (idGen *SegmentIDGenImpl)Get(key string) *common.Result {
	if	!idGen.getInitOK() {
		return &common.Result {
			Id : exceptionIDCacheInitFalse,
			Status: common.Err,
		}
	}
	buffer,ok := idGen.cache.Get(key)
	if	ok {
		// 一个经典的 初始化单例
		buffer.RWMutex.RLock()
		if !buffer.IsInitOk() {
			value,ok := idGen.cache.Get(key)
			if	ok {
				return getIdFromSegmentBuffer(value)
			}
		}
		buffer.RWMutex.RUnlock()

		buffer.RWMutex.Lock()
		defer buffer.RWMutex.Unlock()
		if	!buffer.IsInitOk() {
			err := updateSegmentFromDb(key, buffer.GetCurrent())
			logrus.Info("Init buffer. Update leafkey %s %s from db", key, buffer.GetCurrent())
			buffer.SetInitOk(true)

			if	err!= nil {
				logrus.Warnf("Init buffer %s exception : %s",buffer.GetCurrent(),err)
			}
			value,ok := idGen.cache.Get(key)
			if	ok {
				return getIdFromSegmentBuffer(value)
			}
		}
	}
	return &common.Result{
		exceptionIDKeyNotExists,
		common.Err,
	}
}

func (idGen *SegmentIDGenImpl)getInitOK() (bo bool) {
	idGen.rwMutex.RLock()
	bo = idGen.initOK
	idGen.rwMutex.RUnlock()
	return bo
}

// 更新 Segment 中的内容，其本身是 线程不安全 的。
// 因为其在 buffer 的 RWMutex 的 Lock 中执行，所以不用担心并发
func updateSegmentFromDb(key string, segment *model.Segment) (err error) {
	old := time.Now()
	buffer := segment.GetBuffer()
	var alloc *model.LeafAlloc

	// buffer 未初始化，则联系 DB 初始化 buffer
	if	!buffer.IsInitOk() {
		alloc,err = impls.DefaultIDAllocDaoBean.UpdateMaxIdAndGetLeafAlloc(key)
		if	err != nil{
			return err
		}
		buffer.Step = alloc.Step
		buffer.MinStep = alloc.Step

		//如果 buffer 的updateTimestamp == 0，则说明其未被调用过，则连接 DB 获取段号
	}else if buffer.UpdateTimestamp == 0 {
		alloc,err = impls.DefaultIDAllocDaoBean.UpdateMaxIdAndGetLeafAlloc(key)
		if	err != nil{
			return err
		}
		buffer.UpdateTimestamp = time.Now().UnixNano() / 1e6
		buffer.Step = alloc.Step
		buffer.MinStep = alloc.Step

		// 如果不是以上两种情况呢，说明段号被初始化过了，也已经被调用过了，则负责更新段号
		// 首先需要根据 时间 来动态控制 step 的大小
		// 之后负责更新段号
	}else {
		duration := time.Now().UnixNano() / 1e6 - buffer.UpdateTimestamp
		nextStep := buffer.Step
		if	duration < segmentDuration {
			// 更新速度过快，则尝试增大 Step
			if nextStep * 2 > maxStep {
				// 步子已经够大了，所以 do nothing
			}else {
				// segment 的步长翻倍
				nextStep = nextStep * 2
			}
		}else if duration < segmentDuration * 2{
			// 这正是我们所期望的，segmentBuffer 的更新速度，所以保持的状态
			// do nothing
		}else {
			// segmentBuffer 更新过慢，尝试缩小 Step
			if nextStep / 2 >= buffer.MinStep {
				nextStep = nextStep / 2
			}
		}

		logrus.Infof("key %s,step %d,duration %d,nextStep %d",key,buffer.Step,duration,nextStep)
		// 之后负责更新段号
		temp := &model.LeafAlloc{
			Key : key,
			Step : nextStep,
		}
		// 返回的alloc中不含有时间戳
		alloc, err = impls.DefaultIDAllocDaoBean.UpdateMaxIdByCustomStepAndGetLeafAlloc(temp)
		if err != nil {
			logrus.Errorf("DefaultIDAllocDaoBean.UpdateMaxIdByCustomStepAndGetLeafAlloc(temp) error,temp is : %s",temp)
			return err
		}
		buffer.UpdateTimestamp = time.Now().UnixNano() / 1e6
		buffer.Step = nextStep
		buffer.MinStep = alloc.Step //leafAlloc的step为DB中的step
	}
	value := alloc.MaxID - alloc.Step

	// 这里需要将 segment内含的计数器置为value，但是如果不用 unsafe 的话，很难同时保证 线程安全 和 高并发，所以我决定直接 new 一个新的 AtomInc
	segment.AutoInc.Close()
	segment.AutoInc = common.NewStableAtomInc(value,1,30)
	segment.Max = alloc.MaxID
	segment.Step = buffer.Step

	stopWatch := time.Since(old)
	logrus.Info("updateSegmentFromDb running during %s",stopWatch)
	return nil
}

// 这里负责需要干三件事情，
// 一是从申请的段号中，申请一个唯一的ID，
// 二是需要检测段号的使用情况，如果段号使用超过限度，则备用 segment 去预先申请段号
// 三是判断时机切换两个 Segment
func getIdFromSegmentBuffer(buffer *model.SegmentBuffer) (result *common.Result){
	// 先整一个死循环！
	for  {
		result,err := tryGetIdFromSegmentBuffer(buffer)
		if err == nil {
			return result
		}
		// 如果取不到结果，则让线程自旋，自旋等待时间过长则睡眠 10ms
		waitAndSleep(buffer)

	}

}


func tryGetIdFromSegmentBuffer(buffer *model.SegmentBuffer) (result *common.Result,err error)  {
	buffer.RWMutex.RLock()
	defer buffer.RWMutex.RUnlock()

	segment := buffer.GetCurrent()
	border :=  float64(segment.Step) * 0.9
	// 当 buffer 的 NextSegment 没有初始化完成
	// 且 正在使用的 segment 已经使用的段号小于 step 的 90%
	// 且 buffer 负责准备下一个的 Segment 的线程未在工作
	// 线程池更新 nextSegment
	if	!buffer.IsNextReady() && (segment.GetIdle() < int64(border) && atomic.CompareAndSwapInt32(&buffer.Running,0,1)){
		// 最大的问题来了，如何同时保证 buffer.Running 的 原子性 和 可见性
		go func() {}()
	}
	// 获得自增ID
	value := segment.IncAndGet(1)
	if	value < segment.Max {
		return &common.Result{
			value,
			common.Success,
		},nil
	}
	return nil,errors.New("get Inc bigger with Segment.Max:"+ string(segment.IncAndGet(-1) ))
}
// 先让线程自旋，自旋等待时间过长则睡眠 10ms
func waitAndSleep(buffer *model.SegmentBuffer) {
	roll := 0
	for atomic.CompareAndSwapInt32(&buffer.Running,1,1) {
		roll++
		if roll > 10000 {
			time.Sleep(10 * time.Millisecond)
			break
		}
	}
}

func PutSegmentBuffer() func(k string) *model.SegmentBuffer {
	return NewSegmentBuffer
}

func NewSegmentBuffer(k string) *model.SegmentBuffer {
	buffer := model.NewSegmentBuffer(k)

	return buffer
}