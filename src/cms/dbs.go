package cms

import (
	"encoding/json"
	"fmt"
	"utils"
)

// helper function to load DBS data stream
func loadDBSData(furl string, data []byte) []Record {
	var out []Record
	err := json.Unmarshal(data, &out)
	if err != nil {
		if utils.VERBOSE > 0 {
			msg := fmt.Sprintf("DBS unable to unmarshal the data, furl=%s, data=%s, error=%v", furl, string(data), err)
			fmt.Println(msg)
		}
		return out
	}
	return out
}

// DBS helper function to get dataset info from blocksummaries DBS API
func datasetInfo(dataset string, ch chan float64) {
	api := "blocksummaries"
	furl := fmt.Sprintf("%s/%s/?dataset=%s", dbsUrl(), api, dataset)
	response := utils.FetchResponse(furl, "")
	size := 0.0
	if response.Error == nil {
		records := loadDBSData(furl, response.Data)
		if utils.VERBOSE > 1 {
			fmt.Println("furl", furl, records)
		}
		for _, rec := range records {
			size += rec["file_size"].(float64)
		}
	}
	ch <- size
}
