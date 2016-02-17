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
	"strings"
	"utils"
)

// helper function to load data stream and return DAS records
func loadPhedexData(furl string, data []byte) []Record {
	var out []Record
	var rec Record
	err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("unable to unmarshal the data into record, furl=%s, data=%s, error=%v", furl, string(data), err)
		panic(msg)
	}
	out = append(out, rec)
	return out
}

// helper function to find all dataset at a given tier-site
func datasetInfoAtSite(dataset, siteName, tstamp string, ch chan Record) {
	if !datasetNameOk(dataset) {
		ch <- Record{"dataset": dataset, "size": 0.0, "tier": "unknown"}
		return
	}
	api := "blockreplicas"
	furl := fmt.Sprintf("%s/%s?dataset=%s&node=%s&create_since=%d", phedexUrl(), api, dataset, siteName, utils.UnixTime(tstamp))
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl)
	}
	if strings.HasPrefix(siteName, "T1_") && !strings.HasSuffix(siteName, "_Disk") {
		siteName += "_Disk"
	}
	response := utils.FetchResponse(furl, "")
	size := 0.
	if response.Error == nil {
		records := loadPhedexData(furl, response.Data)
		for _, rec := range records {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				bytes := brec["bytes"].(float64)
				size += bytes
			}
		}
	}
	ch <- Record{"dataset": dataset, "size": size, "tier": utils.DataTier(dataset)}
}

// helper function to find all dataset at a given tier-site
func datasetsDictAtSite(siteName, tstamp string) Record {
	rdict := make(Record)
	api := "blockreplicas"
	furl := fmt.Sprintf("%s/%s?node=%s&create_since=%d", phedexUrl(), api, siteName, utils.UnixTime(tstamp))
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl)
	}
	if strings.HasPrefix(siteName, "T1_") && !strings.HasSuffix(siteName, "_Disk") {
		siteName += "_Disk"
	}
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadPhedexData(furl, response.Data)
		for _, rec := range records {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				dataset := strings.Split(brec["name"].(string), "#")[0]
				bytes := brec["bytes"].(float64)
				val, ok := rdict[dataset]
				if ok {
					rdict[dataset] = bytes + val.(float64)
				} else {
					rdict[dataset] = bytes
				}
			}
		}
	}
	return rdict
}

// helper function to get site content. Return either list of blocks or datasets on site.
func siteContent(siteName, tstamp, recType string) []string {
	api := "blockreplicasummary"
	if strings.HasPrefix(siteName, "T1_") && !strings.HasSuffix(siteName, "_Disk") {
		siteName += "_Disk"
	}
	furl := fmt.Sprintf("%s/%s?node=%s&create_since=%d", phedexUrl(), api, siteName, utils.UnixTime(tstamp))
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl)
	}
	response := utils.FetchResponse(furl, "")
	// use a map to collect dataset names as keys
	ddict := make(Record)
	if response.Error == nil {
		records := loadPhedexData(furl, response.Data)
		for _, rec := range records {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				blk := brec["name"].(string)
				if recType == "block" {
					ddict[blk] = struct{}{}
				} else { // look-up dataset name
					dataset := strings.Split(blk, "#")[0]
					if datasetNameOk(dataset) {
						ddict[dataset] = struct{}{}
					}
				}
			}
		}
		// return map keys, they're unique already
		return utils.MapKeys(ddict)
	}
	return []string{}
}
