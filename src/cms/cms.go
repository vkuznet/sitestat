/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: cms package which provides set of utilities to get statistics
 *				about CMS tier sites
 * Created    : Wed Feb 10 19:31:44 EST 2016
 */
package cms

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"
	"utils"
)

// exported function which process user request
func Process(siteName, tstamp, report string) {
	sites := siteNames(siteName)
	tstamps := utils.TimeStamps(tstamp)
	ch := make(chan Record)
	for _, siteName := range sites {
		go process(siteName, tstamps, ch)
	}
	// collect results
	var out []Record
	for {
		select {
		case r := <-ch:
			out = append(out, r)
		default:
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
		}
		if len(out) == len(sites) {
			break
		}
	}
	if report == "json" {
		res, _ := json.Marshal(out)
		fmt.Println(string(res))
	} else {
		fmt.Printf("\nFinal results\n")
		formatResults(out)
	}
}

// helper function to format aggregated results
func formatResults(records []Record) {
	for _, rec := range records {
		for site, vals := range rec {
			results := vals.(Record)
			report := fmt.Sprintf("%s:\n", site)
			keys := utils.MapKeys(results)
			var ikeys []int
			for _, key := range keys {
				ikey, err := strconv.Atoi(key)
				if err != nil {
					panic("Unable to conver bin keys")
				}
				ikeys = append(ikeys, ikey)
			}
			//             sort.Sort(utils.StringList(keys))
			sort.Ints(ikeys)
			for _, ikey := range ikeys {
				bin := fmt.Sprintf("%d", ikey)
				size := results[bin].(float64)
				report += fmt.Sprintf("Bin %s size %f (%s)\n", bin, size, utils.SizeFormat(size))
			}
			fmt.Println(report)
		}
	}
}

// update dictionary of dict[nacc] = [datasets]
func updateDict(dict Record, nacc int, val string) {
	key := "15"
	if nacc < 15 {
		key = fmt.Sprintf("%d", nacc)
	}
	rec, ok := dict[key]
	if ok {
		arr := rec.([]string)
		arr = append(arr, val)
		dict[key] = arr
	} else {
		dict[key] = []string{val}
	}
}

// helper function to collect popularity results and merge them into bins of NACC
// with the help of updateDict function
func popdb2datasetBins(records []Record, siteDatasets []string) Record {
	var zeroAccessDatasets []string
	metric := "NACC"
	rdict := make(Record)
	for _, rec := range records {
		val := int(rec[metric].(float64))
		dataset := rec["COLLNAME"].(string)
		updateDict(rdict, val, dataset)
		if !utils.InList(dataset, siteDatasets) {
			zeroAccessDatasets = append(zeroAccessDatasets, dataset)
		}
	}
	rdict["0"] = zeroAccessDatasets
	return rdict
}

// helper function to convert popdb bin record dict[nacc] = [datasets] into
// dict[nacc] = size
// Here we use site purely to show the progress in verbose mode
func datasetBins2size(site string, record Record) Record {
	rdict := make(Record)
	for bin, val := range record {
		rdict[bin] = 0.0
		datasets := val.([]string)
		if utils.VERBOSE == 1 {
			fmt.Printf("%s, bin=%s, %d datasets", site, bin, len(datasets))
		}
		for cdx, chunk := range utils.MakeChunks(datasets, 100) {
			if utils.VERBOSE == 1 {
				fmt.Printf("process bin=%s, chunk=%d, %d datasets\n", bin, cdx, len(chunk))
			}
			if utils.VERBOSE == 2 {
				fmt.Println("process chunk", chunk)
			}
			ch := make(chan float64)
			for _, dataset := range chunk {
				go datasetInfo(dataset, ch)
			}
			var out []float64
			for { // collect results
				select {
				case r := <-ch:
					out = append(out, r)
				default:
					time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
				}
				if len(out) == len(chunk) {
					break
				}
			}
			old_sum := rdict[bin].(float64)
			rdict[bin] = old_sum + utils.Sum(out)
		}
	}
	return rdict
}

// local function which process single request for given site name and
// set of time stamps
func process(siteName string, tstamps []string, ch chan Record) {
	// get statistics from popDB for given site and time range
	popDBrecords := datasetStats(siteName, tstamps)
	// get all dataset names on given site (from PhEDEx)
	siteDatasets := datasetsAtSite(siteName)
	// sort datasets into bins by naccess metrics
	res := popdb2datasetBins(popDBrecords, siteDatasets)
	// find out size of dataset for all bins
	results := datasetBins2size(siteName, res)
	// create return record and send it back to given channel
	rec := make(Record)
	rec[siteName] = results
	ch <- rec
}
