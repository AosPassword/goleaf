package core

import (
	"leaf/core/common"
)

// ID生成器的接口
type IDGen interface {
	// 获取对应key的id
	Get(key string) *common.Result

	// ID 生成器初始化
	Initialization() error
}
