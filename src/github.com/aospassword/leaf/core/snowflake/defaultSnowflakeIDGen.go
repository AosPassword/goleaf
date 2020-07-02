package snowflake

import "math/rand"

type IDGenImpl struct {
	twepoch				int64
	workerIdBits 		int64
	maxWorkId			int64
	sequenceBits		int64
	workIdShift			int64
	timestampLeftShift	int64
	sequenceMask		int64

	workerId			int64
	sequence			int64
	lastTimestamp		int64

	rand				rand.Rand
}

func NewSnowflakeIdGen(zkAddress string,port int,twepoch int64)  {

}
