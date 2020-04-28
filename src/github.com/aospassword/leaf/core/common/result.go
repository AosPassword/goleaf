package common

import "encoding/json"

type Result struct {
	Id		int64		`json:"id"`
	Status  Status		`json:"status"`
}


func (r *Result) String() string  {
	jsons, err := json.Marshal(r)
	if	err != nil{
		return ""
	}
	return string(jsons)
}




