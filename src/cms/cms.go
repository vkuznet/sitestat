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
	"strings"
	"time"
	"utils"
)

// exported function which process user request
func Process(metric, siteName, tstamp, tier, breakdown, binValues, report string) {
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
	bins := utils.Bins(binValues)
	tstamps := utils.TimeStamps(tstamp)
	ch := make(chan Record)
	for _, siteName := range sites {
		go process(metric, siteName, tstamps, tier, breakdown, bins, ch)
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
		formatResults(metric, bins, out, breakdown)
	}
}

// helper function to format aggregated results
func formatResults(metric string, bins []int, records []Record, breakdown string) {
	for _, rec := range records {
		for site, vals := range rec {
			rec := vals.(Record)
			results := rec["results"].(BinRecord)
			bresults := rec["breakdown"].(BinRecord)
			//             results := vals.(Record)
			report := fmt.Sprintf("%s:\n", site)
			ikeys := utils.MapIntKeys(results)
			sort.Ints(ikeys)
			pad := ""
			for _, bin := range ikeys {
				size := results[bin].(float64)
				bdown := bresults[bin].(Record)
				if bin == bins[len(bins)-1] {
					pad = "+"
				}
				report += fmt.Sprintf("%s %d%s size %f (%s)\n", metric, bin, pad, size, utils.SizeFormat(size))
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
func updateDict(bins []int, dict BinRecord, metricValue int, val string) {
	// find bin where our metric value fall into
	binValue := bins[0]
	for _, v := range bins {
		if metricValue >= v {
			binValue = v
		}
	}
	rec, ok := dict[binValue]
	if ok {
		arr := rec.([]string)
		arr = append(arr, val)
		dict[binValue] = utils.List2Set(arr)
	} else {
		dict[binValue] = []string{val}
	}
}

// helper function to collect popularity results and merge them into bins of given metric
// with the help of updateDict function
func popdb2Bins(metric string, bins []int, records []Record, siteDatasets []string) BinRecord {
	var zeroMetricDatasets []string
	rdict := make(BinRecord)
	for _, rec := range records {
		val := int(rec[metric].(float64))
		dataset := rec["COLLNAME"].(string)
		updateDict(bins, rdict, val, dataset)
		if !utils.InList(dataset, siteDatasets) {
			zeroMetricDatasets = append(zeroMetricDatasets, dataset)
		}
	}
	rdict[0] = zeroMetricDatasets
	return rdict
}

// helper function to convert popdb bin record dict[nacc] = [datasets] into
// dict[nacc] = size
// Here we use site purely to show the progress in verbose mode
func bins2size(site string, record BinRecord, breakdown string) (BinRecord, BinRecord) {
	rdict := make(BinRecord)
	bdict := make(BinRecord)
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
func process(metric, siteName string, tstamps []string, tier, breakdown string, bins []int, ch chan Record) {
	// get statistics from popDB for given site and time range
	popDBrecords := datasetStats(siteName, tstamps, tier)
	// get all dataset names on given site (from PhEDEx)
	siteDatasets := datasetsAtSite(siteName)
	// sort dataset results from popDB into bins by given metric
	res := popdb2Bins(metric, bins, popDBrecords, siteDatasets)
	// find out size for all bins
	results, bres := bins2size(siteName, res, breakdown)
	// create return record and send it back to given channel
	rec := make(Record)
	rec[siteName] = Record{"results": results, "breakdown": bres}
	ch <- rec
}
