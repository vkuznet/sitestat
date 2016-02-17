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
	"time"
	"utils"
)

// global variables
var DBSINFO bool

// exported function which process user request
func Process(metric, siteName, tstamp, tier, breakdown, binValues, format string) {
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
	if utils.VERBOSE > 0 {
		fmt.Printf("Site: %s, sites %v, tstamp %s, interval %v\n", siteName, sites, tstamp, tstamps)
	}
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
	if format == "json" {
		var records []Record
		for _, rec := range out {
			nrec := make(Record)
			for site, sdict := range rec {
				nrow := make(map[string]interface{})
				for key, val := range sdict.(Record) { // key=results|breakdown, val is a dict
					row := make(map[string]interface{})
					for kkk, vvv := range val.(BinRecord) {
						row[fmt.Sprintf("%d", kkk)] = vvv
					}
					nrow[key] = row
				}
				nrec[site] = nrow
			}
			records = append(records, nrec)
		}
		res, err := json.Marshal(records)
		if err != nil {
			fmt.Println("Unable to marshal json out of found results")
			fmt.Println(err)
			os.Exit(-1)
		}
		fmt.Println(string(res))
	} else {
		msg := fmt.Sprintf("Final results: metric %s, site, %s, time interval %s %v", metric, siteName, tstamp, tstamps)
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

// helper function to collect popularity results and merge them into bins of given metric
// with the help of updateDict function
func popdb2Bins(metric string, bins []int, records []Record, siteDatasets *[]string) BinRecord {
	var popdbDatasets []string
	rdict := make(BinRecord)
	for _, bin := range bins {
		rdict[bin] = []string{} // init all bins
	}
	for _, rec := range records { // loop over popularity records
		mval := int(rec[metric].(float64))
		dataset := rec["COLLNAME"].(string)
		popdbDatasets = append(popdbDatasets, dataset)
		updateDict(bins, rdict, mval, dataset)
	}
	// loop over site datasets and collect zero bin for given metric
	var zeroMetricDatasets []string
	for _, dataset := range *siteDatasets {
		if !utils.InList(dataset, popdbDatasets) {
			zeroMetricDatasets = append(zeroMetricDatasets, dataset)
		}
	}
	rdict[0] = zeroMetricDatasets
	for _, bin := range bins { // make sure that we have unique list of datasets in every bin
		arr := rdict[bin].([]string)
		rdict[bin] = utils.List2Set(arr)
	}
	return rdict
}

type BinStruct struct {
	bin  int
	size float64
	brec Record
}

// helper function to update given bin
func updateBin(bin int, site string, datasets []string, tstamp, breakdown string, ch chan BinStruct) {
	newSize := 0.0
	bdict := make(Record)
	for cdx, chunk := range utils.MakeChunks(datasets, 100) {
		if utils.VERBOSE == 1 {
			fmt.Printf("process bin=%d, chunk=%d, %d datasets\n", bin, cdx, len(chunk))
		}
		if utils.VERBOSE == 2 {
			fmt.Println("process chunk", chunk)
		}
		dch := make(chan Record)
		for _, dataset := range chunk {
			if DBSINFO {
				go datasetInfo(dataset, dch) // DBS call
			} else {
				go datasetInfoAtSite(dataset, site, tstamp, dch) // PhEDEx call
			}
		}
		var out []Record
		for { // collect results
			select {
			case r := <-dch:
				out = append(out, r)
			default:
				time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
			}
			if len(out) == len(chunk) {
				break
			}
		}
		newSize += sumSize(out)
		bdict = updateBreakdown(breakdown, bdict, out)
	}
	ch <- BinStruct{bin, newSize, bdict}
}

// helper function to convert popdb bin record dict[nacc] = [datasets] into
// dict[nacc] = size
// Here we use site purely to show the progress in verbose mode
func bins2size(site string, brecord BinRecord, tstamp, breakdown string) (BinRecord, BinRecord) {
	rdict := make(BinRecord)
	bdict := make(BinRecord)
	ch := make(chan BinStruct)
	for bin, val := range brecord { // loop over record with bins
		rdict[bin] = 0.0
		bdict[bin] = make(Record)
		datasets := val.([]string)
		if utils.VERBOSE == 1 {
			fmt.Printf("%s, bin=%s, %d datasets", site, bin, len(datasets))
		}
		go updateBin(bin, site, datasets, tstamp, breakdown, ch)
	}
	counter := 0
	for { // collect results
		select {
		case r := <-ch:
			rdict[r.bin] = r.size
			bdict[r.bin] = r.brec
			counter += 1
		default:
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
		}
		if counter == len(brecord) {
			break
		}
	}
	return rdict, bdict
}

// local function which process single request for given site name and
// set of time stamps
func process(metric, siteName string, tstamps []string, tier, breakdown string, bins []int, ch chan Record) {
	startTime := time.Now()
	// get statistics from popDB for given site and time range
	popDBrecords := datasetStats(siteName, tstamps, tier)
	if utils.PROFILE {
		fmt.Println("popDBRecords", time.Now().Sub(startTime))
	}
	// get dataset dict on given site (from PhEDEx)
	siteDatasets := datasetsAtSite(siteName, tstamps[0])
	if utils.PROFILE {
		fmt.Println("datasetsAtSite", time.Now().Sub(startTime))
	}
	// sort dataset results from popDB into bins by given metric
	rdict := popdb2Bins(metric, bins, popDBrecords, &siteDatasets)
	if utils.PROFILE {
		fmt.Println("popdb2Bins", time.Now().Sub(startTime))
	}
	// find out size for all bins
	results, bres := bins2size(siteName, rdict, tstamps[0], breakdown)
	if utils.PROFILE {
		fmt.Println("bins2size", time.Now().Sub(startTime))
	}
	// create return record and send it back to given channel
	rec := make(Record)
	rec[siteName] = Record{"results": results, "breakdown": bres}
	ch <- rec
}
