package modelTest

import (
	"fmt"
	"github.com/aospassword/leaf/core/segment"
	"github.com/aospassword/leaf/core/segment/model"
	"runtime"
	"strconv"
	"sync"
	"testing"
)
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/segment/model/modelTest
//BenchmarkCSBMap
//BenchmarkCSBMap-8   	   85774	     13749 ns/op
//PASS
//race: limit on 8128 simultaneously alive goroutines is exceeded, dying
// put + get 两次操作加起来大概是 72,732 QPS，项目目标 QPS 为 5w，应该不会成为瓶颈
// 单次操作 QPS 145,465
// -race limit on 8128 simultaneously alive goroutines is exceeded, dying
// why???? 我明明没有创建 goroutine，唯一创建 goroutine 是由csMap的初始化
// 我猜，应该是创建 segmentBuffer 的时候，因为要创建 sync.RWMutex 导致了这种现象，我去看看
// 不对 mutex 没有涉及到创建 goroutine
var csbMap = model.NewDefaultConcurrentSegmentBufferMap(segment.PutSegmentBuffer())

var mutex = sync.Mutex{}

func init()  {
	var key string
	for i := 0;i < 10;i ++ {
		key = strconv.Itoa(i)
		csbMap.Put(key)
	}
}

func BenchmarkCSBMap(b *testing.B) {
	var ok bool
	for i := 0;i < b.N;i ++ {
		key := strconv.Itoa(i)
		_,ok = csbMap.Put(key)
		if	ok {
			csbMap.Get(key)
		}
	}
	fmt.Println(runtime.NumGoroutine())
}


// get + put 测试 put ：get = 1 ：1000
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/segment/model/modelTest
//BenchmarkNewCSBMap
//BenchmarkNewCSBMap-8   	  799957	     11355 ns/op  11us
// 大概是 9w QPS 有一说一，在不race的情况下有点差
//PASS

// 单纯get的速度测试
// creating goroutine
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/segment/model/modelTest
//BenchmarkNewCSBMap
//BenchmarkNewCSBMap-8   	  923133	      2757 ns/op
//PASS

// -race
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/segment/model/modelTest
//BenchmarkNewCSBMap
//BenchmarkNewCSBMap-8   	    1874	    783496 ns/op
//PASS
// 性能并不是很理想
func BenchmarkNewCSBMap(b *testing.B) {
	key := [8]string{
		"0","1","2","3","4","5","6","7",
	}
	for i := 0;i < b.N;i ++ {
		csbMap.Get(key[i & 0x07])
	}
}
//creating goroutine
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/segment/model/modelTest
//BenchmarkStrov
//BenchmarkStrov-8   	 5825270	       236 ns/op
//PASS
func BenchmarkStrov(b *testing.B)  {
	for i := 0;i < b.N;i ++ {
		strconv.Itoa(i)
	}
}

func TestNewCSBMap(t *testing.T)  {
	var ok bool
	for i := 0;i < 3;i ++ {
		key := strconv.Itoa(i)
		_,ok = csbMap.Put(key)

		if	ok {
			for j := 0;j < 99999;j ++ {
				csbMap.Get(key)
			}
		}
	}
}
// 基础功能测试，简单的一个 for 循环
// creating goroutine
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/segment/model/modelTest
//BenchmarkGetRange
//BenchmarkGetRange-8   	  480049	     11546 ns/op
//PASS


// 基础功能 -race 测试

// creating goroutine
// goos: windows
// goarch: amd64
// pkg: github.com/aospassword/leaf/core/segment/model/modelTest
// BenchmarkGetRange
// BenchmarkGetRange-8   	    1948	    709194 ns/op
// PASS

// put get getRange 混合 -race测试
// put : range : get = 1000 : 500 : 1
// creating goroutine
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/segment/model/modelTest
//BenchmarkGetRange
//BenchmarkGetRange-8   	    1405	   2654807 ns/op
//PASS
func BenchmarkGetRange(b *testing.B)  {
	key := strconv.Itoa(3)
	for i := 0;i < b.N;i ++ {
		if	i & 1000 == 1000 {
			csbMap.Put(string(i))
		}

		if i & 500 == 500 {
			csbMap.GetRange()
		}
		csbMap.Get(key)
	}
}

//=== RUN   TestGetRange
//--- PASS: TestGetRange (36.10s)
//PASS
func TestGetRange(t *testing.T)  {
	key := strconv.Itoa(3)
	for i := 0;i < 20000;i ++ {
		if	i & 10000 == 10000 {
			csbMap.Put(string(i))
		}

		if i & 500 == 500 {
			csbMap.GetRange()
		}
		csbMap.Get(key)
	}
}