// sitestat tool aggregates statistics from CMS popularity DB, DBS, SiteDB
// and presents results for any given tier site and time interval
package main

import (
	"cms"
	"flag"
	"utils"
)

func main() {
	var site string
	flag.StringVar(&site, "site", "", "CMS site name, use T1, T2, T3 to specify all Tier sites")
	var trange string
	flag.StringVar(&trange, "trange", "1d", "Specify time interval in YYYYMMDD format, e.g 20150101-20150201 or use short notations 1d, 1m, 1y for one day, month, year, respectively")
	var tier string
	flag.StringVar(&tier, "tier", "", "Look-up specific data-tier")
	var metric string
	flag.StringVar(&metric, "metric", "NACC", "Popularity DB metric (NACC, TOTCPU, NUSERS)")
	var dbsinfo bool
	flag.BoolVar(&dbsinfo, "dbsinfo", false, "Use DBS to collect dataset information, default use PhEDEx")
	var breakdown string
	flag.StringVar(&breakdown, "breakdown", "", "Breakdown report into more details (tier, dataset)")
	var bins string
	flag.StringVar(&bins, "bins", "", "Comma separated list of bin values, e.g. 0,1,2,3,4 for naccesses or 0,10,100 for tot cpu metrics")
	var format string
	flag.StringVar(&format, "format", "txt", "Output format type, txt or json")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbose level, support 0,1,2")
	var profile bool
	flag.BoolVar(&profile, "profile", false, "profile code")
	flag.Parse()
	utils.VERBOSE = verbose
	utils.PROFILE = profile
	cms.DBSINFO = dbsinfo
	cms.Process(metric, site, trange, tier, breakdown, bins, format)
}
