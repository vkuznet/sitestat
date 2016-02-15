/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: utils module for sitestat package
 * Created    : Wed Feb 10 19:31:44 EST 2016
 */
package utils

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// global variable for this module which we're going to use across
// many modules
var VERBOSE int

// test environment
func TestEnv() {
	uproxy := os.Getenv("X509_USER_PROXY")
	ucert := os.Getenv("X509_USER_CERT")
	if uproxy == "" && ucert == "" {
		fmt.Println("Neither X509_USER_PROXY or X509_USER_CERT is set")
		os.Exit(-1)
	}
	uckey := os.Getenv("X509_USER_KEY")
	if uckey == "" {
		fmt.Println("X509_USER_KEY is not set")
		os.Exit(-1)
	}
}
func TestMetric(metric string) {
	metrics := []string{"NACC", "TOTCPU", "NUSERS"}
	if !InList(metric, metrics) {
		msg := fmt.Sprintf("Wrong metric '%s', please choose from %v", metric, metrics)
		fmt.Println(msg)
		os.Exit(-1)
	}
}
func TestBreakdown(bdown string) {
	bdowns := []string{"tier", "dataset", ""}
	if !InList(bdown, bdowns) {
		msg := fmt.Sprintf("Wrong breakdown value '%s', please choose from %v", bdown, bdowns)
		fmt.Println(msg)
		os.Exit(-1)
	}
}

// helper function to extract data tier from dataset name
func DataTier(dataset string) string {
	dparts := strings.Split(dataset, "/")
	return dparts[len(dparts)-1]
}

// helper function to check item in a list
func InList(a string, list []string) bool {
	check := 0
	for _, b := range list {
		if b == a {
			check += 1
		}
	}
	if check != 0 {
		return true
	}
	return false
}

// helper function to return keys from a map
func MapKeys(rec map[string]interface{}) []string {
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// helper function to return keys from a map
func MapIntKeys(rec map[int]interface{}) []int {
	keys := make([]int, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// helper function to convert input list into set
func List2Set(arr []string) []string {
	var out []string
	for _, key := range arr {
		if !InList(key, out) {
			out = append(out, key)
		}
	}
	return out
}

// helper function to convert size into human readable form
func SizeFormat(val float64) string {
	base := 1000. // CMS convert is to use power of 10
	xlist := []string{"", "KB", "MB", "GB", "TB", "PB"}
	for _, vvv := range xlist {
		if val < base {
			return fmt.Sprintf("%3.1f%s", val, vvv)
		}
		val = val / base
	}
	return fmt.Sprintf("%3.1f%s", val, xlist[len(xlist)])
}

// helper function to perform sum operation over provided array of floats
func Sum(data []float64) float64 {
	out := 0.0
	for _, val := range data {
		out += val
	}
	return out
}

// implement sort for []int type
type IntList []int

func (s IntList) Len() int           { return len(s) }
func (s IntList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s IntList) Less(i, j int) bool { return s[i] < s[j] }

// implement sort for []string type
type StringList []string

func (s StringList) Len() int           { return len(s) }
func (s StringList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StringList) Less(i, j int) bool { return s[i] < s[j] }

// helper function to extract value from tstamp string
func extractVal(ts string) int {
	val, _ := strconv.Atoi(ts[0 : len(ts)-1])
	return val
}

// convert given timestamp into time stamp list
func TimeStamps(ts string) []string {
	var out []string
	const layout = "20060102"
	var bdate, edate string
	now := time.Now().Unix()
	t := time.Now()
	today := t.Format(layout)
	edate = today
	if strings.HasSuffix(ts, "d") == true { // N-days
		val := extractVal(ts)
		sec := now - int64(val*24*60*60)
		bdate = time.Unix(sec, 0).Format(layout)
	} else if strings.HasSuffix(ts, "m") == true { // N-months
		val := extractVal(ts)
		sec := now - int64(val*30*24*60*60)
		if VERBOSE > 0 {
			fmt.Println("time interval", ts, val, now, sec)
		}
		bdate = time.Unix(sec, 0).Format(layout)
	} else if strings.HasSuffix(ts, "y") == true { // N-years
		val := extractVal(ts)
		sec := now - int64(val*365*30*24*60*60)
		bdate = time.Unix(sec, 0).Format(layout)
	} else {
		res := strings.Split(ts, "-")
		sort.Sort(StringList(res))
		bdate = res[0]
		edate = res[len(res)-1]
	}
	if VERBOSE > 0 {
		fmt.Println("timestamp", bdate, edate)
	}
	out = append(out, bdate)
	out = append(out, edate)
	return out
}

// helper function to make chunks from provided list
func MakeChunks(arr []string, size int) [][]string {
	var out [][]string
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
		out = append(out, arr[abeg:alen-1])
	}
	return out
}

// helper function to return bin values
func Bins(bins string) []int {
	if bins == "" {
		return []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	}
	var out []int
	for _, v := range strings.Split(bins, ",") {
		val, _ := strconv.Atoi(v)
		if val > 0 {
			out = append(out, val)
		}
	}
	sort.Ints(out)
	return out
}
