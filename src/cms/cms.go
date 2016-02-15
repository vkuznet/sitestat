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
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"utils"
)

// exported function which process user request
func Process(metric, siteName, tstamp, tier, breakdown, report string) {
	utils.TestEnv()
	utils.TestMetric(metric)
	utils.TestBreakdown(breakdown)
	tiers := dataTiers()
	if tier != "" && !utils.InList(tier, tiers) {
		msg := fmt.Sprintf("Wrong data tier '%s'", tier)
		fmt.Println(msg)
		os.Exit(-1)
	}
	sites := siteNames(siteName)
	tstamps := utils.TimeStamps(tstamp)
	ch := make(chan Record)
	for _, siteName := range sites {
		go process(metric, siteName, tstamps, tier, breakdown, ch)
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
		msg := fmt.Sprintf("Final results: metric %s, site, %s, time interval %s", metric, siteName, tstamp)
		if tier != "" {
			msg += fmt.Sprintf(", tier %s", tier)
		}
		if breakdown != "" {
			msg += fmt.Sprintf(", breakdown %s", breakdown)
		}
		fmt.Println(msg)
		formatResults(metric, out, breakdown)
	}
}

// helper function to format aggregated results
func formatResults(metric string, records []Record, breakdown string) {
	for _, rec := range records {
		for site, vals := range rec {
			rec := vals.(Record)
			results := rec["results"].(Record)
			bresults := rec["breakdown"].(Record)
			//             results := vals.(Record)
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
			pad := ""
			for _, ikey := range ikeys {
				bin := fmt.Sprintf("%d", ikey)
				size := results[bin].(float64)
				bdown := bresults[bin].(Record)
				if ikey == 15 {
					pad = "+"
				}
				report += fmt.Sprintf("%s %s%s size %f (%s)\n", metric, bin, pad, size, utils.SizeFormat(size))
				report += formatBreakdown(bdown, breakdown)
			}
			fmt.Println(report)
		}
	}
}

func formatBreakdown(bdown Record, breakdown string) string {
	report := ""
	if breakdown == "" {
		return report
	}
	keys := utils.MapKeys(bdown)
	lsize := 0
	if breakdown == "tier" {
		sort.Sort(utils.StringList(keys))
		for _, k := range keys {
			if len(k) > lsize {
				lsize = len(k)
			}
		}
	}
	for _, k := range keys {
		v := bdown[k]
		size := v.(float64)
		pad := ""
		if breakdown == "tier" {
			if len(k) < lsize {
				pad = strings.Repeat(" ", (lsize - len(k)))
			}
		}
		report += fmt.Sprintf("   %s%s\t%f (%s)\n", k, pad, size, utils.SizeFormat(size))
	}
	return report
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
		dict[key] = utils.List2Set(arr)
	} else {
		dict[key] = []string{val}
	}
}

// helper function to collect popularity results and merge them into bins of given metric
// with the help of updateDict function
func popdb2datasetBins(metric string, records []Record, siteDatasets []string) Record {
	var zeroAccessDatasets []string
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
func bins2size(site string, record Record, breakdown string) (Record, Record) {
	rdict := make(Record)
	bdict := make(Record)
	for bin, val := range record {
		rdict[bin] = 0.0
		bdict[bin] = make(Record)
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
			ch := make(chan Record)
			for _, dataset := range chunk {
				go datasetInfo(dataset, ch)
			}
			var out []Record
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
			rdict[bin] = old_sum + sumSize(out)
			bdict[bin] = updateBreakdown(breakdown, bdict[bin].(Record), out)
		}
	}
	return rdict, bdict
}

// local function which process single request for given site name and
// set of time stamps
func process(metric, siteName string, tstamps []string, tier, breakdown string, ch chan Record) {
	// get statistics from popDB for given site and time range
	popDBrecords := datasetStats(siteName, tstamps, tier)
	// get all dataset names on given site (from PhEDEx)
	siteDatasets := datasetsAtSite(siteName)
	// sort datasets into bins by given metric
	res := popdb2datasetBins(metric, popDBrecords, siteDatasets)
	// find out size for all bins
	results, bres := bins2size(siteName, res, breakdown)
	// create return record and send it back to given channel
	rec := make(Record)
	rec[siteName] = Record{"results": results, "breakdown": bres}
	ch <- rec
}
