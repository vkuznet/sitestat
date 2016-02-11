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
	flag.StringVar(&trange, "trange", "1d", "Specify time interval in YYYYMMDD format, e.g 20150101-20150201 or use short notations 1d, 1m, 1y for one day, one month and one year, respectively")
	var report string
	flag.StringVar(&report, "report", "txt", "Report type, txt or json")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbose level, support 0,1,2")
	flag.Parse()
	utils.VERBOSE = verbose
	cms.Process(site, trange, report)
}
