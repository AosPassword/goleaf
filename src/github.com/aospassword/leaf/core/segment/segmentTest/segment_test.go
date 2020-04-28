package segmentTest

import (
	"fmt"
	"testing"
)
import "github.com/aospassword/leaf/core/segment/dao/impls"

func TestDao(t *testing.T)  {
	allocs,_ := impls.DefaultIDAllocDaoBean.GetAllLeafAllocs()
	if len(allocs) != 1 {
		t.Errorf("want 1 got %d",len(allocs))
	}

	tags,_ := impls.DefaultIDAllocDaoBean.GetAllTags()
	if len(tags) != 1 {
		t.Errorf("want 1 got %d",len(tags))
	}

	res,_ := impls.DefaultIDAllocDaoBean.UpdateMaxIdAndGetLeafAlloc("picture")

	if	res.MaxID != 101 {
		t.Errorf("got alloc: %s",res)
	}
	fmt.Printf("got alloc: %s",res)
}

