/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: part of cms package responsible for DBS calls
 * Created    : Wed Feb 10 19:31:44 EST 2016
 */
package cms

import (
	"encoding/json"
	"fmt"
	"net/url"
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
func datasetInfo(dataset string, ch chan Record) {
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
	rec := make(Record)
	rec["name"] = dataset
	rec["size"] = size
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
