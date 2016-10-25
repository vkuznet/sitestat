package cms

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/sitestat/utils"
	"net/url"
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
func blockInfo(block string, ch chan Record) {
	api := "blocksummaries"
	furl := fmt.Sprintf("%s/%s/?block_name=%s", dbsUrl(), api, url.QueryEscape(block))
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
	rec := make(Record)
	rec["name"] = block
	rec["size"] = size
	rec["tier"] = utils.DataTier(block)
	ch <- rec
}

// DBS helper function to get dataset info from blocksummaries DBS API
func datasetSize(dataset string) float64 {
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
			size = rec["file_size"].(float64)
			break
		}
	}
	return size
}

// DBS helper function to get dataset info from blocksummaries DBS API
func datasetInfo(dataset string, ch chan Record) {
	size := 0.0
	rec := make(Record)
	rec["name"] = dataset
	if PBRDB != "" { // take dataset size from PBR DB, instead of DBS
		size = PBRMAP[dataset]
		if size == 0 {
			size = datasetSize(dataset)
		}

	} else {
		size = datasetSize(dataset)
	}
	rec["size"] = size
	rec["tier"] = utils.DataTier(dataset)
	ch <- rec
}

// DBS helper function to get dataset info from blocksummaries DBS API
func datasetInfoOrig(dataset string, ch chan Record) {
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
			size = rec["file_size"].(float64)
			break
		}
	}
	rec := make(Record)
	rec["name"] = dataset
	if PBRDB != "" { // take dataset size from PBR DB, instead of DBS
		rec["size"] = PBRMAP[dataset]
	} else {
		rec["size"] = size
	}
	rec["tier"] = utils.DataTier(dataset)
	ch <- rec
}

// helper function to get CMS data tier names
func dataTiers() []string {
	var out []string
	api := "datatiers"
	furl := fmt.Sprintf("%s/%s/", dbsUrl(), api)
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadDBSData(furl, response.Data)
		if utils.VERBOSE > 1 {
			fmt.Println("furl", furl, records)
		}
		for _, rec := range records {
			tier := rec["data_tier_name"].(string)
			out = append(out, tier)
		}
	}
	return utils.List2Set(out)

}

// helper function which convert string list into comma separate string values
func list2comma(chunk []string) string {
	var out string
	for _, v := range chunk {
		out += fmt.Sprintf("\"%s\",", v)
	}
	return out[:len(out)-1]
}

// helper function to obtain datasets creation times for given list of datasets
func datasetsCreationTimes(datasets []string) map[string]float64 {
	rdict := make(map[string]float64)
	api := "datasetlist"
	furl := fmt.Sprintf("%s/%s", dbsUrl(), api)
	for _, chunk := range utils.MakeChunks(datasets, 1000) {
		args := fmt.Sprintf("{\"detail\":true,\"dataset\":[%s]}", list2comma(chunk))
		response := utils.FetchResponse(furl, args)
		if response.Error == nil {
			records := loadDBSData(furl, response.Data)
			if utils.VERBOSE > 1 {
				fmt.Println("furl", furl, records)
			}
			for _, rec := range records {
				name := rec["dataset"].(string)
				ctime := rec["creation_date"].(float64)
				rdict[name] = ctime
			}
		}
	}
	return rdict
}
