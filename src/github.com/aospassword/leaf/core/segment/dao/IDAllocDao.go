package dao

import "github.com/aospassword/leaf/core/segment/model"

type IDAllocDao interface {
	GetAllLeafAllocs() ([]model.LeafAlloc,error)
	UpdateMaxIdAndGetLeafAlloc(tag string) (model.LeafAlloc,error)
	UpdateMaxIdByCustomStepAndGetLeafAlloc(alloc model.LeafAlloc) (model.LeafAlloc,error)
	GetALlTags() ([]string, error)
}
