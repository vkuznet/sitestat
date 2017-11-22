// CMS module collects various statistics from CMS data-services
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
package cms

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
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
func Process(metric, siteName, tstamp, tier, breakdown, binValues, format, tierPatterns string) {
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
		process(metric, siteName, tstamps, tier, breakdown, bins, tierPatterns, ch, &wg)
		//         go process(metric, siteName, tstamps, tier, breakdown, bins, tierPatterns, ch, &wg)
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

type PopDBRecord struct {
	metric int
	name   string
}

func pdbRecords(metric string, records []Record) []PopDBRecord {
	var out []PopDBRecord
	var mval int
	for _, rec := range records { // loop over popularity records
		if metric == "RNACC" {
			v, _ := strconv.ParseFloat(rec[metric].(string), 64)
			mval = int(math.Ceil(v))
		} else {
			mval = int(rec[metric].(float64))
		}
		name := rec["name"].(string)
		out = append(out, PopDBRecord{mval, name})
	}
	return out
}

// helper function to collect popularity results and merge them into bins of given metric
// with the help of updateDict function.
// return rdict which is a dictionary of bins and corresponding dataset names
func popdb2Bins(metric string, bins []int, records []Record, siteName string, tstamps []string) BinRecord {
	var popdbNames []string
	rdict := make(BinRecord)
	for _, bin := range bins {
		rdict[bin] = []string{} // init all bins
	}
	recType := "dataset" // type of record we'll process
	var mval int
	for idx, rec := range records { // loop over popularity records
		if metric == "RNACC" {
			v, _ := strconv.ParseFloat(rec[metric].(string), 64)
			mval = int(math.Ceil(v))
		} else {
			mval = int(rec[metric].(float64))
		}
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
	}
	wg.Wait()
	for i := 0; i < len(brecord); i++ {
		r := <-ch
		rdict[r.bin] = r.size
		bdict[r.bin] = r.brec
	}
	return rdict, bdict
}

// helper function to make chunks from provided list
func makeChunks(arr []PopDBRecord, size int) [][]PopDBRecord {
	if size == 0 {
		fmt.Println("WARNING: chunk size is not set, will use size 10")
		size = 10
	}
	var out [][]PopDBRecord
	alen := len(arr)
	abeg := 0
	aend := size
	for {
		if aend < alen {
			out = append(out, arr[abeg:aend])
			abeg = aend
			aend += size
		} else {
			break
		}
	}
	if abeg < alen {
		out = append(out, arr[abeg:alen])
	}
	return out
}

// helper function to convert popdb records int proper bins
func collectBins(siteName string, records []PopDBRecord, tstamp, breakdown string) (BinRecord, BinRecord) {

	// loop over site content and collect zero bin for given metric
	recType := "dataset" // TODO, get it from first records
	siteNames := siteContent(siteName, tstamp, recType)
	var zeroMetricNames []string
	var popdbNames []string
	for _, rec := range records {
		popdbNames = append(popdbNames, rec.name)
	}
	for _, name := range siteNames {
		if !utils.InList(name, popdbNames) {
			zeroMetricNames = append(zeroMetricNames, name)
		}
	}

	// fetch old datasets, those who are in zero bin but their creation time
	// is older then interval we're intersting.
	thr := float64(utils.UnixTime(tstamp))
	datasetsOld := oldDatasets(zeroMetricNames, thr, siteName)
	datasetsZero := utils.Substruct(zeroMetricNames, datasetsOld)

	// output dicts
	rdict := make(BinRecord)
	bdict := make(BinRecord)

	// get sizes of old datasets
	fmt.Println("old", len(datasetsOld))
	for _, chunk := range utils.MakeChunks(datasetsOld, utils.CHUNKSIZE) {
		dch := make(chan Record, len(chunk))
		var wg sync.WaitGroup
		for _, name := range chunk {
			wg.Add(1)
			go datasetInfoPhEDEx(name, tstamp, 0, dch, &wg) // PhEDEx call
		}
		wg.Wait()
		for i := 0; i < len(chunk); i++ {
			r := <-dch
			v, ok := rdict[-1]
			if ok {
				rdict[-1] = v.(float64) + r["size"].(float64)
			} else {
				rdict[-1] = r["size"].(float64)
			}
		}
	}
	// get sizes of zero bin datasets
	fmt.Println("zero", len(datasetsZero))
	for _, chunk := range utils.MakeChunks(datasetsZero, utils.CHUNKSIZE) {
		dch := make(chan Record, len(chunk))
		var wg sync.WaitGroup
		for _, name := range chunk {
			wg.Add(1)
			go datasetInfoPhEDEx(name, tstamp, 0, dch, &wg) // PhEDEx call
		}
		wg.Wait()
		for i := 0; i < len(chunk); i++ {
			r := <-dch
			v, ok := rdict[0]
			if ok {
				rdict[0] = v.(float64) + r["size"].(float64)
			} else {
				rdict[0] = r["size"].(float64)
			}
		}
	}
	// get sizes of the rest of datasets
	fmt.Println("all", len(records))
	for _, chunk := range makeChunks(records, utils.CHUNKSIZE) {
		dch := make(chan Record, len(chunk))
		var wg sync.WaitGroup
		for _, rec := range chunk {
			wg.Add(1)
			go datasetInfoPhEDEx(rec.name, tstamp, rec.metric, dch, &wg) // PhEDEx call
		}
		wg.Wait()
		for i := 0; i < len(chunk); i++ {
			r := <-dch
			v := r["bin"]
			if v == nil {
				continue
			}
			bin := r["bin"].(int)
			if bin > 14 {
				bin = 15
			}
			v, ok := rdict[bin]
			if ok {
				rdict[bin] = v.(float64) + r["size"].(float64)
			} else {
				rdict[bin] = r["size"].(float64)
			}
		}
	}
	return rdict, bdict
}

// select specified data-tier patterns
func selectPatterns(records []Record, tierPatterns string) []Record {
	if tierPatterns == "" {
		return records
	}
	var out []Record
	for _, rec := range records {
		val := rec["COLLNAME"].(string)
		for _, pat := range strings.Split(tierPatterns, ",") {
			matched, _ := regexp.MatchString(pat, val)
			if matched {
				out = append(out, rec)
				break
			}
		}
	}
	return list2Set(out)
}

// helper function to check item in a list
func inList(a Record, list []Record) bool {
	check := 0
	for _, b := range list {
		if b["COLLNAME"].(string) == a["COLLNAME"].(string) {
			check += 1
		}
	}
	if check != 0 {
		return true
	}
	return false
}

// helper function to convert input list into set
func list2Set(arr []Record) []Record {
	var out []Record
	for _, r := range arr {
		if !inList(r, out) {
			out = append(out, r)
		}
	}
	return out
}

// local function which process single request for given site name and
// set of time stamps
func process(metric, siteName string, tstamps []string, tier, breakdown string, bins []int, tierPatterns string, ch chan Record, wg *sync.WaitGroup) {
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
	// select data-tier pattenrs
	popdbRecords = selectPatterns(popdbRecords, tierPatterns)
	// sort dataset results from popDB into bins by given metric
	rdict := popdb2Bins(metric, bins, popdbRecords, siteName, tstamps)
	if utils.PROFILE {
		fmt.Println("popdb2Bins", time.Since(startTime), len(rdict))
	}
	// find out size for all bins
	results, bres := bins2size(siteName, rdict, tstamps[0], breakdown)
	// new way to collect records, via normalization
	//     recs := pdbRecords(metric, popdbRecords)
	//     results, bres := collectBins(siteName, recs, tstamps[0], breakdown)
	if utils.PROFILE {
		fmt.Println("bins2size", time.Since(startTime))
	}
	// create return record and send it back to given channel
	rec := make(Record)
	rec[siteName] = Record{"results": results, "breakdown": bres}
	ch <- rec
}
