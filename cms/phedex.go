package cms

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/sitestat/utils"
	"math"
	"strings"
	"sync"
)

// helper function to load data stream and return DAS records
func loadPhedexData(furl string, data []byte) []Record {
	var out []Record
	var rec Record
	err := json.Unmarshal(data, &rec)
	if err != nil {
		//         msg := fmt.Sprintf("unable to unmarshal the data into record, furl=%s, data=%s, error=%v", furl, string(data), err)
		//         panic(msg)
		return out
	}
	out = append(out, rec)
	return out
}

// helper function to find all dataset at a given tier-site
func datasetInfoAtSite(dataset, siteName, tstamp string, ch chan Record, wg *sync.WaitGroup) {
	defer wg.Done()
	if !datasetNameOk(dataset) {
		ch <- Record{"dataset": dataset, "size": 0.0, "tier": "unknown"}
		return
	}
	api := "blockreplicas"
	//     furl := fmt.Sprintf("%s/%s?dataset=%s&node=%s&create_since=%d", phedexUrl(), api, dataset, siteName, utils.UnixTime(tstamp))
	furl := fmt.Sprintf("%s/%s?dataset=%s&node=%s", phedexUrl(), api, dataset, siteName)
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl, "look-up tstamp", tstamp)
	}
	//     if strings.HasPrefix(siteName, "T1_") && !strings.HasSuffix(siteName, "_Disk") {
	//         siteName += "_Disk"
	//     }
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
func datasetInfoPhEDEx(dataset, siteName, tstamp string, metric int, ch chan Record, wg *sync.WaitGroup) {
	defer wg.Done()
	if !datasetNameOk(dataset) {
		ch <- Record{"dataset": dataset, "size": 0.0, "tier": "unknown", "norm": 0.0, "bin": 0}
		return
	}
	api := "blockreplicas"
	furl := fmt.Sprintf("%s/%s?dataset=%s&create_since=%d", phedexUrl(), api, dataset, utils.UnixTime(tstamp))
	//     furl := fmt.Sprintf("%s/%s?dataset=%s", phedexUrl(), api, dataset)
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl, "look-up tstamp", tstamp)
	}
	response := utils.FetchResponse(furl, "")
	size := 0.
	sdict := make(map[string]int)
	if response.Error == nil {
		records := loadPhedexData(furl, response.Data)
		for _, rec := range records {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				bytes := brec["bytes"].(float64)
				size += bytes
				replicas := brec["replica"].([]interface{})
				for _, r := range replicas {
					rep := r.(map[string]interface{})
					node := rep["node"].(string)
					files := int(rep["files"].(float64))
					v, ok := sdict[node]
					if ok {
						sdict[node] = v + files
					} else {
						sdict[node] = files
					}
				}
			}
		}
	}
	// norm factor
	maxFiles := 0
	for _, v := range sdict {
		if maxFiles < v {
			maxFiles = v
		}
	}
	norm := 0.0
	for _, v := range sdict {
		norm += float64(v) / float64(maxFiles)
	}
	bin := int(math.Ceil(float64(metric) / (float64(maxFiles) * norm)))
	if norm == 0 {
		bin = 0
	}
	//     if _, ok := sdict[siteName]; !ok {
	//         size = 0 // dataset is not present on our site
	//     }
	ch <- Record{"dataset": dataset, "size": size, "tier": utils.DataTier(dataset), "norm": norm, "bin": bin, "maxFiles": maxFiles, "nsites": len(sdict), "metric": metric}
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
func siteContent(siteName, tstamp, recType, tier string) []string {
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
				dataset := strings.Split(blk, "#")[0]
				if !keepDataTier(dataset, tier) {
					continue
				}
				if recType == "block" {
					ddict[blk] = struct{}{}
				} else { // look-up dataset name
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
