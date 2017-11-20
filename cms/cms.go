// CMS module collects various statistics from CMS data-services
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
package cms

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	//     "sort"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/sitestat/utils"
)

// global variables
var DBSINFO, BLKINFO bool
var PBRDB, PHGROUP string
var PDB *sql.DB
var DBSDATASETS map[string]float64
var PBRMAP map[string][]DatasetAttrs

// exported function which process user request
func Process(metric, siteName, tstamp, tier, breakdown, binValues, format string) {
	startTime := time.Now()
	utils.TestEnv()
	utils.TestMetric(metric)
	utils.TestBreakdown(breakdown)
	tiers := dataTiers()
	if tier != "" && !utils.InList(tier, tiers) {
		msg := fmt.Sprintf("Wrong data tier '%s'", tier)
		fmt.Println(msg)
		os.Exit(-1)
	}
	if PBRDB != "" { // we got PBR name, open DB
		db, err := sql.Open("sqlite3", PBRDB)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		//         db.SetMaxIdleConns(100)
		err = db.Ping()
		if err != nil {
			panic(err)
		}
		PDB = db
		var pbr PBR
		PBRMAP = pbr.Map()
		if utils.VERBOSE > 0 {
			fmt.Println("Loaded PBRMAP", len(PBRMAP), "items")
		}
	}
	sites := siteNames(siteName)
	bins := utils.Bins(binValues)
	tstamps := utils.TimeStamps(tstamp)
	if utils.VERBOSE > 0 {
		fmt.Printf("Site: %s, sites %v, tstamp %s, interval %v\n", siteName, sites, tstamp, tstamps)
	}
	ch := make(chan Record, len(sites))
	var wg sync.WaitGroup
	for _, siteName := range sites {
		wg.Add(1)
		// I'm unable (yet) concurrently process site metrics since DBS
		// server timeout on concurrent requests, therefore I process them sequentially
		process(metric, siteName, tstamps, tier, breakdown, bins, ch, &wg)
		//         go process(metric, siteName, tstamps, tier, breakdown, bins, ch, &wg)
	}
	wg.Wait()
	// collect results
	var out []Record
	for i := 0; i < len(sites); i++ {
		r := <-ch
		out = append(out, r)
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
	} else if format == "csv" {
		formatCSV(bins, out)
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
	if utils.PROFILE {
		fmt.Printf("Processed %d urls\n", utils.UrlCounter)
		fmt.Printf("Elapsed time %s\n", time.Since(startTime))
	}
}

// helper function to collect old datasets based on zero bin dataset list and given threshold
func oldDatasets(datasets []string, thr float64, site string) []string {
	//     if DBSDATASETS == nil {
	//         DBSDATASETS = datasetsCreationTimes(datasets)
	//     }
	//     var out []string
	//     for _, name := range datasets {
	//         if DBSDATASETS[name] < thr {
	//             out = append(out, name)
	//         }
	//     }
	var out []string
	ddict := datasetsCreationTimes(datasets)
	//     if site == "T2_IT_Pisa" {
	//         sort.Sort(utils.StringList(datasets))
	//         fmt.Println("### oldDatasets input", len(datasets), datasets[0], "output", len(ddict))
	//     }
	for name, ctime := range ddict {
		if ctime < thr {
			out = append(out, name)
		}
	}
	return out
}

// Pseudo code
// for timewindow in (3months, 6months, 12months):
//  if dataset.naccess(tstart=now-timewindow, tstop=now)==0:
//    if dataset.timecreate < now-timewindow:
//       histogram[0_old] = dataset.weightedsize
//    else if dataset.timecreate >= now-timewindow:
//  	 histogram[0_new] = dataset.weightedsize

// helper function to collect popularity results and merge them into bins of given metric
// with the help of updateDict function.
// return rdict which is a dictionary of bins and corresponding dataset names
func popdb2Bins(metric string, bins []int, records []Record, siteName string, tstamps []string) BinRecord {
	var popdbNames []string
	rdict := make(BinRecord)
	for _, bin := range bins {
		rdict[bin] = []string{} // init all bins
	}
	recType := "dataset"            // type of record we'll process
	for idx, rec := range records { // loop over popularity records
		mval := int(rec[metric].(float64))
		name := rec["name"].(string)
		if idx == 0 && strings.Contains(name, "#") {
			recType = "block"
		}
		popdbNames = append(popdbNames, name)
		updateDict(bins, rdict, mval, name)
	}
	// loop over site content and collect zero bin for given metric
	siteNames := siteContent(siteName, tstamps[0], recType)
	var zeroMetricNames []string
	for _, name := range siteNames {
		if !utils.InList(name, popdbNames) {
			zeroMetricNames = append(zeroMetricNames, name)
		}
	}
	rdict[0] = zeroMetricNames

	// fetch old datasets, those who are in zero bin but their creation time
	// is older then interval we're intersting.
	thr := float64(utils.UnixTime(tstamps[0]))
	olds := oldDatasets(rdict[0].([]string), thr, siteName)
	rdict[-1] = olds
	newd := utils.Substruct(rdict[0].([]string), rdict[-1].([]string))
	if utils.VERBOSE > 0 {
		fmt.Println("Bin-zero division, bin0-old", len(olds), "bin0-new", len(newd))
	}
	rdict[0] = newd
	//     if siteName == "T2_IT_Pisa" {
	//         fmt.Println("### oldDatasets", len(olds))
	//         fmt.Println("### zero", len(zeroMetricNames))
	//         fmt.Println("### newd", len(newd))
	//     }

	// make sure that we have unique list of datasets in every bin
	allbins := []int{-1, 0}
	for _, bin := range bins {
		allbins = append(allbins, bin)
	}
	for _, bin := range allbins {
		arr := rdict[bin].([]string)
		rdict[bin] = utils.List2Set(arr)
		//         val := rdict[bin]
		//         fmt.Println(siteName, "bin ", bin, " contains ", len(val.([]string)))
	}
	return rdict
}

type BinStruct struct {
	bin  int
	size float64
	brec Record
}

// helper function to update given bin
func updateBin(bin int, site string, names []string, tstamp, breakdown string, ch chan BinStruct, sg *sync.WaitGroup) {
	defer sg.Done()
	newSize := 0.0
	var allRecords []Record
	for cdx, chunk := range utils.MakeChunks(names, utils.CHUNKSIZE) {
		if utils.VERBOSE == 1 {
			fmt.Printf("process bin=%d, chunk=%d, %d records\n", bin, cdx, len(chunk))
		}
		if utils.VERBOSE == 2 {
			fmt.Println("process chunk", chunk)
		}
		dch := make(chan Record, len(chunk))
		var wg sync.WaitGroup
		for _, name := range chunk {
			wg.Add(1)
			go datasetInfoAtSite(name, site, tstamp, dch, &wg) // PhEDEx call
			/*
				if BLKINFO {
					go blockInfo(name, dch) // DBS call
				} else {
					if PBRDB != "" {
						go datasetInfoPBR(name, site, dch) // DBS call
						//                 } else if DBSINFO {
						//                     go datasetInfo(name, dch) // DBS call
					} else {
						go datasetInfoAtSite(name, site, tstamp, dch) // PhEDEx call
					}
				}
			*/
		}
		wg.Wait()
		var out []Record
		for i := 0; i < len(chunk); i++ {
			out = append(out, <-dch)
		}
		newSize += sumSize(out)
		for _, v := range out {
			allRecords = append(allRecords, v)
		}
	}
	bdict := updateBreakdown(breakdown, allRecords)
	ch <- BinStruct{bin, newSize, bdict}
}

// helper function to convert popdb bin record dict[nacc] = [datasets] into
// dict[nacc] = size
// Here we use site purely to show the progress in verbose mode
func bins2size(site string, brecord BinRecord, tstamp, breakdown string) (BinRecord, BinRecord) {
	rdict := make(BinRecord)
	bdict := make(BinRecord)
	ch := make(chan BinStruct, len(brecord))
	var wg sync.WaitGroup
	for bin, val := range brecord { // loop over record with bins
		rdict[bin] = 0.0
		bdict[bin] = make(Record)
		names := val.([]string)
		if utils.VERBOSE == 1 {
			fmt.Printf("%s, bin=%d, %d records\n", site, bin, len(names))
		}
		wg.Add(1)
		go updateBin(bin, site, names, tstamp, breakdown, ch, &wg)
		//         if site == "T2_IT_Pisa" {
		//             fmt.Println("### bin sent", bin, len(names))
		//         }
	}
	wg.Wait()
	for i := 0; i < len(brecord); i++ {
		r := <-ch
		//         if site == "T2_IT_Pisa" {
		//             fmt.Println("### bin recv", r.bin, r.size)
		//         }
		rdict[r.bin] = r.size
		bdict[r.bin] = r.brec
	}
	return rdict, bdict
}

// local function which process single request for given site name and
// set of time stamps
func process(metric, siteName string, tstamps []string, tier, breakdown string, bins []int, ch chan Record, wg *sync.WaitGroup) {
	defer wg.Done()
	startTime := time.Now()
	if utils.PROFILE {
		fmt.Println("process", metric, siteName)
	}
	// get statistics from popDB for given site and time range
	var popdbRecords []Record
	if BLKINFO {
		popdbRecords = blockStats(siteName, tstamps, tier)
	} else {
		popdbRecords = datasetStats(siteName, tstamps, tier)
	}
	if utils.PROFILE {
		fmt.Println("popDBRecords", time.Since(startTime))
	}
	// sort dataset results from popDB into bins by given metric
	rdict := popdb2Bins(metric, bins, popdbRecords, siteName, tstamps)
	if utils.PROFILE {
		fmt.Println("popdb2Bins", time.Since(startTime))
	}
	// find out size for all bins
	results, bres := bins2size(siteName, rdict, tstamps[0], breakdown)
	//     if siteName == "T2_IT_Pisa" {
	//         fmt.Println("### popdbRecords", len(popdbRecords))
	//         fmt.Println("### rdict", tstamps)
	//         for k, v := range rdict {
	//             fmt.Println("#", k, v, len(v.([]string)))
	//         }
	//         fmt.Println("### results", results)
	//     }
	if utils.PROFILE {
		fmt.Println("bins2size", time.Since(startTime))
	}
	// create return record and send it back to given channel
	rec := make(Record)
	rec[siteName] = Record{"results": results, "breakdown": bres}
	ch <- rec
}
