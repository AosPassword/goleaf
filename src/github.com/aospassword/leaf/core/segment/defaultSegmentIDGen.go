package segment

import (
	"errors"
	"github.com/aospassword/leaf/core/common"
	"github.com/aospassword/leaf/core/segment/dao/impls"
	"github.com/aospassword/leaf/core/segment/model"
	"github.com/sirupsen/logrus"
	"strconv"
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
	dao		*impls.DefaultIDAllocDao
	rwMutex sync.RWMutex
}

func NewSegmentIdGenImpl() (*SegmentIDGenImpl,error) {
	idGen := &SegmentIDGenImpl{
		initOK: false,
		cache: sync.Map{},
		dao: impls.DefaultIDAllocDaoBean,
		rwMutex: sync.RWMutex{},
	}
	if err := idGen.Initialization();err != nil {
		return nil, err
	}
	return idGen,nil
}

// 搜索数据库，对比数据库 alloc 的 all Tags，如果 cache 的 key 和 tags 不相等
// 则将数据库的 tags 灌入到 cache 中
func (idGen *SegmentIDGenImpl)Initialization() (err error) {
	logrus.Info("SegmentIDGenImpl Init...")
	idGen.updateCacheFromDB()

	idGen.rwMutex.Lock()
	defer idGen.rwMutex.Unlock()
	idGen.initOK = true

	go idGen.updateCacheFromDbAtEveryMinute()
	return
}


func (idGen *SegmentIDGenImpl)updateCacheFromDB()  {
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


	//将 cache 中已经存在的 tag 移出 insertTagsMap，为新建 Segment Buffer 做准备
	idGen.cache.Range(func(key, value interface{}) bool {
		k := key.(string)
		if	_,ok := insertTagsMap[k];ok {
			delete(insertTagsMap,k)
		}
		removeTagsMap[k] = true
		return true
	})

	// 向 cache 中填充 buffer
	// buffer 于此处初始化
	for k,_ := range insertTagsMap {

		buffer,ok := idGen.cache.LoadOrStore(k,NewSegmentBuffer(k))
		segmentBuffer :=  buffer.(*model.SegmentBuffer)
		if	ok {
			logrus.Info("add tag:" + k + " from db to IdCache,SegmentBuffer" + segmentBuffer.String())
		}else {
			logrus.Warn("tag:" + k + " already was in IdCache,SegmentBuffer" + segmentBuffer.String())
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
	logrus.Debug("start getting...")

	if	!idGen.initOK {
		return &common.Result {
			Id : exceptionIDCacheInitFalse,
			Status: common.Err,
		}
	}

	value,ok := idGen.cache.Load(key)

	if	ok {
		// 一个经典的 初始化单例
		buffer := value.(*model.SegmentBuffer)
		buffer.RWMutex.RLock()


		logrus.Debug("Get Buffer Read Lock!")
		// 判定其是否初始化完毕
		if buffer.InitOK {
			buffer.RWMutex.RUnlock()
			logrus.Debug("Load "+ key + " SegmentBuffer!,RUnlock!")
			return getIdFromSegmentBuffer(value.(*model.SegmentBuffer))

		}
		buffer.RWMutex.RUnlock()

		buffer.RWMutex.Lock()
		// 二次判定
		if	!buffer.InitOK {
			logrus.Debug("buffer is not init ok!")
			err := updateSegmentFromDb(key,buffer.Current)


			logrus.Info("Init buffer. Update leafkey "+key+" from db")

			buffer.InitOK = true

			if	err!= nil {
				logrus.Warnf("Init buffer %s exception : %s",buffer.Current,err)
			}

		}
		buffer.RWMutex.Unlock()
		logrus.Debug(" init and load!")
		if	ok {
			return getIdFromSegmentBuffer(value.(*model.SegmentBuffer))
		}

	}
	return &common.Result{
		exceptionIDKeyNotExists,
		common.Err,
	}
}


// 更新 Segment 中的内容，其本身是 线程不安全 的。
// 因为其在 buffer 的 RWMutex 的 Lock 中执行，所以不用担心并发
// 致于nextBuffer，因为只有一个线程可以拿到 running 状态，所以也是单线程
func updateSegmentFromDb(key string, segment *model.Segment) (err error) {
	logrus.Debug("updateSegmentFromDb:key = "+key)

	old := time.Now()
	buffer := segment.GetBuffer()
	var alloc *model.LeafAlloc

	// buffer 未初始化，则联系 DB 初始化 buffer
	if	!buffer.InitOK {

		alloc,err = impls.DefaultIDAllocDaoBean.UpdateMaxIdAndGetLeafAlloc(key)
		if	err != nil{
			logrus.Errorf("impls.DefaultIDAllocDaoBean.UpdateMaxIdAndGetLeafAlloc(key):"+ key+"\terror")
			return err
		}
		buffer.Step = alloc.Step
		buffer.MinStep = alloc.Step

		logrus.Debug("init buffer end!")


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
	logrus.Debug("setting segment...")

	value := alloc.MaxID - buffer.Step

	segment.AutoInc.Set(value)
	segment.Max = alloc.MaxID
	segment.Step = buffer.Step

	logrus.Debug("setting end!")

	stopWatch := time.Since(old)
	logrus.Info("updateSegmentFromDb running during " + stopWatch.String())
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

		logrus.Debug("can't get ID ,so I Sleep!")
		waitAndSleep(buffer)
		// 加锁
		logrus.Debug("try getting lock to switch!")

		buffer.RWMutex.Lock()

		logrus.Debug("try got Lock! and get atomInc value to compare with Max")

		segment := buffer.Current
		value := segment.Get()

		logrus.Debug("get Lock,then got value: "+strconv.Itoa(int(value)))

		if	value < segment.Max{
			return &common.Result{
				value,
				common.Success,
			}
		}
		logrus.Debug("atomInc value is too big,try to switch pointer")

		if buffer.NextReady {
			buffer.Switch()
			buffer.NextReady = false
		}else {
			return &common.Result{
				exceptionIDTwoSegmentsAreNull,
				common.Err,
			}
		}

		logrus.Debug("try switch func UnLocking...")
		buffer.RWMutex.Unlock()
		logrus.Debug("try switch func UnLock!")
	}

}


func tryGetIdFromSegmentBuffer(buffer *model.SegmentBuffer) (*common.Result,error)  {
	logrus.Debug("tryGetIdFromSegmentBuffer getting Lock!")

	buffer.RWMutex.RLock()
	defer buffer.RWMutex.RUnlock()

	logrus.Debug("tryGetIdFromSegmentBuffer got Lock!")

	segment := buffer.Current
	border :=  segment.Step >> 3
	hasUsed := segment.Step - segment.GetIdle()
	// 当 buffer 的 NextSegment 没有初始化完成
	// 且 正在使用的 segment 已经使用的段号小于 step 的 90%
	// 且 buffer 负责准备下一个的 Segment 的线程未在工作
	// 线程池更新 nextSegment

	if	!buffer.NextReady &&
		hasUsed > border &&
		atomic.CompareAndSwapInt32(&buffer.Running,0,1){
		logrus.Infof("prepare Next Buffer,segment is : %d,hasUsed is %d,border is %d",segment.Get(),hasUsed,border)
		// 只有 switch 可以将 nextReady 置为false
		// 只有 updateOK 可将 &buffer.Running 置为 0

		// 这里需要注意，因为对于 &buffer.Running 的读，发生在 waitAndSleep中的时候，是不需要读锁的，这使得我们必须保证 写过程 是 原子 的
		// 但是因为 对于buffer.NextReady的读是包含在读锁中的，并不用考虑其数据竞争

		go func() {
			next := buffer.Next
			updateOK := false

			//
			err := updateSegmentFromDb(buffer.Key,next)
			updateOK = true

			logrus.Info("update segment " + buffer.Key + " from db,max = "+strconv.Itoa(int(buffer.Next.Max)))

			if err != nil {
				return
			}

			defer func() {
				if updateOK {
					buffer.RWMutex.Lock()
					buffer.NextReady = true
					atomic.StoreInt32(&buffer.Running,0)
					buffer.RWMutex.Unlock()
				}else {
					buffer.Running = 0
				}
			}()

		}()
	}
	// 获得自增ID
	value := segment.IncAndGet(1)
	logrus.Debugf("value is %d,max is %d \n",value,segment.Max)
	if	value < segment.Max {
		return &common.Result{
			Id:value,
			Status: common.Success,
		},nil
	}
	logrus.Infof("can't get the value,inc is too big")
	return nil,errors.New("get Inc bigger with Segment.Max")
}
// 先让线程自旋，自旋等待时间过长则睡眠 10ms
func waitAndSleep(buffer *model.SegmentBuffer) {
	roll := 0
	for atomic.LoadInt32(&buffer.Running) == 1 {
		roll++
		if roll > 10000 {
			time.Sleep(10 * time.Millisecond)
			break
		}
	}
}

//func PutSegmentBuffer() func(k string) *model.SegmentBuffer {
//	return NewSegmentBuffer
//}
//
func NewSegmentBuffer(k string) *model.SegmentBuffer {
	buffer := model.NewSegmentBuffer(k)

	return buffer
}

