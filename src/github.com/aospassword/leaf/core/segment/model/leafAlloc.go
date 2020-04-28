package model

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
)

type LeafAlloc struct {
	Key			string		`json:"key"`
	MaxID		int64		`json:"max_id"`
	Step		int64		`json:"step"`
	UpdateTime	string		`json:"update_time"`
}

func (a LeafAlloc) String() string {
	bytes,err := json.Marshal(a)
	if err != nil {
		logrus.Errorf(a.Key + "\t"+ string(a.MaxID) +"\t"+ string(a.Step) +"\t"+ a.UpdateTime + "\tto JSON : Error")
		return ""
	}
	return string(bytes)
}
