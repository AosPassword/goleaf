package dao

import "leaf/core/segment/model"

type IDAllocDao interface {
	GetAllLeafAllocs() ([]model.LeafAlloc,error)
	UpdateMaxIdAndGetLeafAlloc(tag string) (*model.LeafAlloc,error)
	UpdateMaxIdByCustomStepAndGetLeafAlloc(*model.LeafAlloc) (*model.LeafAlloc,error)
	GetAllTags() ([]string, error)
}
