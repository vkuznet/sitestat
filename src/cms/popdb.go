/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: part of cms package responsible for popDB calls
 * Created    : Wed Feb 10 19:31:44 EST 2016
 */
package cms

import (
	"encoding/json"
	"fmt"
	"utils"
)

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
		row := make(Record)
		rec := item.(map[string]interface{})
		dataset := rec["COLLNAME"].(string)
		dataTier := utils.DataTier(dataset)
		keep := true
		if tier != "" {
			if dataTier != tier {
				keep = false
			}
		}
		if keep {
			for k, v := range item.(map[string]interface{}) {
				row[k] = v
			}
			out = append(out, row)
		}
	}
	return out
}

// convert YYYYDDMM into popdb notation
func popDBtstamp(ts string) string {
	return fmt.Sprintf("%s-%s-%s", ts[0:4], ts[4:6], ts[6:8])
}

func datasetStats(siteName string, tstamps []string, tier string) []Record {
	var out []Record
	api := "DSStatInTimeWindow"
	tstart := popDBtstamp(tstamps[0])
	tstop := popDBtstamp(tstamps[len(tstamps)-1])
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
