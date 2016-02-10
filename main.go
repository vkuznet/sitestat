package main

import (
	"cms"
	"flag"
	"utils"
)

func main() {
	var siteName string
	flag.StringVar(&siteName, "siteName", "", "CMS site name, use T1, T2, T3 to specify all Tier sites")
	var timeInterval string
	flag.StringVar(&timeInterval, "timeInterval", "", "Specify time interval in YYYYMMDD format")
	var report string
	flag.StringVar(&report, "report", "txt", "Report type, txt or json")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbose level, support 0,1,2")
	flag.Parse()
	utils.VERBOSE = verbose
	cms.Process(siteName, timeInterval, report)
}
