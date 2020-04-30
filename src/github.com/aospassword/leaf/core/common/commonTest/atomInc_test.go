package commonTest
//  本机测试，java atomInteger 类 1,000,000 次 12ms - 14ms 平均 13ns/op
import (
	"github.com/aospassword/leaf/core/common"
	"testing"
)
// 添加 set 功能之前
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/common/commonTest
//BenchmarkCustomerAutoInc
//BenchmarkCustomerAutoInc-8   	 1687772	       600 ns/op
//PASS
// 对于一个目标 5w QPS 的项目来说，这个类不会成为瓶颈,但是还是被 Java 的 AtomLong 完爆了 QwQ
// 不过还在意料之中，毕竟没用 unsafe
// -race 结果
// goos: windows
// goarch: amd64
// pkg: github.com/aospassword/leaf/core/common/commonTest
// BenchmarkCustomerAutoInc
// BenchmarkCustomerAutoInc-8   	  460570	      2356 ns/op
// PASS

// 添加 set 功能之后
// goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/common/commonTest
//BenchmarkCustomerAutoInc
//BenchmarkCustomerAutoInc-8   	 1419718	       838 ns/op
//PASS
// 我只能说，select 添加 case 比想象的更耗时

// 测试set功能，发现会出现死锁，目前没有解决，暂时废弃
//func BenchmarkCustomerAutoInc(b *testing.B) {
//	autoInc := common.NewCustomerAtomInc(0)
//
//	for i := 0;i < b.N;i ++ {
//		autoInc.Inc(1)
//	}
//	value,ok := autoInc.Inc(0)
//
//	if !ok || int(value) != b.N {
//		b.Errorf("want %d,got %d",b.N,value)
//	}
//}

//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/common/commonTest
//BenchmarkStableAutoInc
//BenchmarkStableAutoInc-8   	 4615416	       138 ns/op
//PASS
// 对于一个目标 5w QPS 的项目来说，这个类不会成为瓶颈,但是还是被 Java 的 AtomLong 完爆了 QwQ
// 不过还在意料之中，毕竟没用 unsafe
// -race
// goos: windows
// goarch: amd64
// pkg: github.com/aospassword/leaf/core/common/commonTest
// BenchmarkStableAutoInc
// BenchmarkStableAutoInc-8   	 1210204	       995 ns/op
// PASS
// 即使是在极高并发的情况下也拥有不错的性能

// 添加 set 方法之后,纯 inc 操作
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/common/commonTest
//BenchmarkStableAutoInc
//BenchmarkStableAutoInc-8   	 4468016	       265 ns/op
//PASS

// 添加 set 方法后，inc 操作 和 set 操作 100 : 1
// goos: windows
// goarch: amd64
// pkg: github.com/aospassword/leaf/core/common/commonTest
// BenchmarkStableAutoInc
// BenchmarkStableAutoInc-8   	 2294463	       587 ns/op
// PASS
// -race
// 此处存在数据竞争，会导致 set 了数据之后，还会从 queue 中读取到脏数据

// 添加了 RWLock 之后，保证数据安全
// goos: windows
// goarch: amd64
// pkg: github.com/aospassword/leaf/core/common/commonTest
// BenchmarkStableAutoInc
// BenchmarkStableAutoInc-8   	 2419354	       470 ns/op
// PASS
// -race
// goos: windows
// goarch: amd64
// pkg: github.com/aospassword/leaf/core/common/commonTest
// BenchmarkStableAutoInc
// BenchmarkStableAutoInc-8   	  461400	      2301 ns/op
// 很明显满足 5w QPS 的目标
// PASS

var autoInc = common.NewStableAtomInc(0,1,40,40,40)
// +get(),-race
//goos: windows
//goarch: amd64
//pkg: github.com/aospassword/leaf/core/common/commonTest
//BenchmarkStableAutoInc
//BenchmarkStableAutoInc-8           60606             17878 ns/op
//PASS
func BenchmarkStableAutoInc(b *testing.B) {

	value, ok := autoInc.Inc(1)
	ok = false
	for i := 0;i < b.N;i ++ {
		if	value > 99 {
			autoInc.Set(0)
		}
		for !ok {
			value, ok = autoInc.Inc(1)
			if	value != autoInc.Get(){
				b.Errorf("error")
			}
		}
		ok = false
	}

	if value > 100 {
		b.Errorf("want <= 100,got %d",value)
	}
}

//func TestCreateChan(t *testing.T)  {
//	chans := make([]chan int,10000)
//	for i := 0; i < 10000;i++ {
//		chans := append(chans, make(chan int))
//	}
//	fmt.Println(chans[3])
//}

//=== RUN   TestParallelCustomerAutoInc
//--- PASS: TestParallelCustomerAutoInc (3.79s)
//PASS
//func TestParallelCustomerAutoInc(t *testing.T) {
//	atomInc := common.NewCustomerAtomInc(0)
//	defer atomInc.Close()
//
//	result := make(chan int64)
//
//	for i := 0;i < 500;i ++ {
//		go Inc(atomInc,10000,result)
//	}
//
//	var max int64 = 0
//	var end int64 = 0
//	for i := 0; i < 500;i ++ {
//		end = <-result
//		if max < end {
//			max = end
//		}
//	}
//
//	if max != 500 * 10000 {
//		t.Errorf("want 5,000,000,got:%d",max)
//	}
//}


//=== RUN   TestParallelCustomerAutoInc
//--- PASS: TestParallelCustomerAutoInc (2.65s)
//PASS
func TestParallelStableAtomInc(t *testing.T) {
	atomInc := common.NewStableAtomInc(0,1,50,50,50)
	defer atomInc.Close()

	result := make(chan int64)

	for i := 0;i < 500;i ++ {
		go Inc(atomInc,10000,result)
	}

	var max int64 = 0
	var end int64 = 0
	for i := 0; i < 500;i ++ {
		end = <-result
		if max < end {
			max = end
		}
	}

	if max != 500 * 10001 {
		t.Errorf("want 5,000,000,got:%d",max)
	}
}

func Inc(inc common.AtomInc,num int,out chan<- int64)  {
	for i := 0;i < num;i ++ {
		inc.Inc(1)
	}
	value,_ := inc.Inc(0)

	out <- value
}
