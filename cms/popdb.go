package cms

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/sitestat/utils"
	"strings"
)

// helper function to load PopDB data stream
// if data tier is given we only filter datasets with given tier
func loadVictorDBData(furl string, data []byte, site, tier string) []Record {
	var out []Record
	var rec Record
	err := json.Unmarshal(data, &rec)
	if err != nil {
		if utils.VERBOSE > 0 {
			msg := fmt.Sprintf("PopDB unable to unmarshal the data, furl=%s, data=%s, error=%v", furl, string(data), err)
			fmt.Println(msg)
		}
		return out
	}
	val := rec[site]
	if val == nil {
		return out
	}
	values := val.(map[string]interface{})
	for blk, idict := range values {
		rec := idict.(map[string]interface{})
		rec["name"] = blk
		dataset := strings.Split(blk, "#")[0]
		if keepDataTier(dataset, tier) {
			out = append(out, rec)
		}
	}
	return out
}

// helper function to load PopDB data stream
// if data tier is given we only filter datasets with given tier
func loadPopDBData(furl string, data []byte, tier string) []Record {
	var out []Record
	var rec Record
	err := json.Unmarshal(data, &rec)
	if err != nil {
		if utils.VERBOSE > 0 {
			msg := fmt.Sprintf("PopDB unable to unmarshal the data, furl=%s, data=%s, error=%v", furl, string(data), err)
			fmt.Println(msg)
		}
		return out
	}
	values := rec["DATA"].([]interface{})
	for _, item := range values {
		rec := item.(map[string]interface{})
		dataset := rec["COLLNAME"].(string)
		rec["name"] = dataset
		if keepDataTier(dataset, tier) {
			out = append(out, rec)
		}
	}
	return out
}

// convert YYYYDDMM into popdb notation
func popDBtstamp(ts string) string {
	return fmt.Sprintf("%s-%s-%s", ts[0:4], ts[4:6], ts[6:8])
}

// helper function to collect dataset usage from popularity DB
func datasetStats(siteName string, tstamps []string, tier string) []Record {
	var out []Record
	api := "DSStatInTimeWindow"
	tstart := popDBtstamp(tstamps[0])
	tstop := popDBtstamp(tstamps[len(tstamps)-1])
	siteName = strings.Replace(siteName, "_Disk", "", 1)
	furl := fmt.Sprintf("%s/%s/?sitename=%s&tstart=%s&tstop=%s", popdbUrl(), api, siteName, tstart, tstop)
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl)
	}
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadPopDBData(furl, response.Data, tier)
		return records
	}
	return out
}

// helper function to collect info about block usage from victor DB
func blockStats(siteName string, tstamps []string, tier string) []Record {
	var out []Record
	api := "accessedBlocksStat"
	tstart := popDBtstamp(tstamps[0])
	tstop := popDBtstamp(tstamps[len(tstamps)-1])
	siteName = strings.Replace(siteName, "_Disk", "", 1)
	furl := fmt.Sprintf("%s/%s/?sitename=%s&tstart=%s&tstop=%s", victordbUrl(), api, siteName, tstart, tstop)
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl)
	}
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadVictorDBData(furl, response.Data, siteName, tier)
		return records
	}
	return out
}
