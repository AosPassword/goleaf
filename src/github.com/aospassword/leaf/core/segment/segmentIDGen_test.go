package segment

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"

	"leaf/core/common"
)
// -race
//--- PASS: TestSegmentIDGenImpl_Get (15.33s)
//PASS

func TestSegmentIDGenImpl_Get(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)

	idGen,err := NewSegmentIdGenImpl()
	var old int64  = 0
	if err != nil {
		t.Errorf(err.Error())
	}else {
		for i := 0; i < 500000; i++ {
			value := idGen.Get("picture")
			if value.Status == common.Err{
				fmt.Println(value.Id)
			}else {
				if value.Id < old {
					t.Errorf("inc error")
				}else {
					old = value.Id
				}
			}
		}
	}
	fmt.Println(old)
}

func BenchmarkNewSegmentBuffer(b *testing.B) {
	logrus.SetLevel(logrus.ErrorLevel)

	idGen,err := NewSegmentIdGenImpl()
	var old int64  = 0
	if err != nil {
		b.Errorf(err.Error())
	}else {
		for i := 0; i < b.N; i++ {
			value := idGen.Get("picture")
			if value.Status == common.Err{

			}else {
				if value.Id < old {
					b.Errorf("inc error")
				}else {
					old = value.Id
				}
			}
		}
	}
}