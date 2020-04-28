package core

// ID生成器的接口

type IDGen interface {
	// 获取对应 key 的id
	GetID(key string)
	// ID 生成器初始化
	Initialization() error
}
