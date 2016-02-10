/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS utils module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 */
package utils

import (
	"fmt"
	"log"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// global variable for this module which we're going to use across
// many modules
var VERBOSE int

// helper function to return Stack
func Stack() string {
	trace := make([]byte, 2048)
	count := runtime.Stack(trace, false)
	return fmt.Sprintf("\nStack of %d bytes: %s\n", count, trace)
}

// error helper function which can be used in defer ErrPropagate()
func ErrPropagate(api string) {
	if err := recover(); err != nil {
		log.Println("DAS ERROR", api, "error", err, Stack())
		panic(fmt.Sprintf("%s:%s", api, err))
	}
}

// error helper function which can be used in goroutines as
// ch := make(chan interface{})
// go func() {
//    defer ErrPropagate2Channel(api, ch)
//    someFunction()
// }()
func ErrPropagate2Channel(api string, ch chan interface{}) {
	if err := recover(); err != nil {
		log.Println("DAS ERROR", api, "error", err, Stack())
		ch <- fmt.Sprintf("%s:%s", api, err)
	}
}

// Helper function to run any given function in defered go routine
func GoDeferFunc(api string, f func()) {
	ch := make(chan interface{})
	go func() {
		defer ErrPropagate2Channel(api, ch)
		f()
		ch <- "ok" // send to channel that we can read it later in case of success of f()
	}()
	err := <-ch
	if err != nil && err != "ok" {
		panic(err)
	}
}

// helper function to find item in a list
func FindInList(a string, arr []string) bool {
	for _, e := range arr {
		if e == a {
			return true
		}
	}
	return false
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

// helper function to compare list of strings
func EqualLists(list1, list2 []string) bool {
	count := 0
	for _, k := range list1 {
		if InList(k, list2) {
			count += 1
		} else {
			return false
		}
	}
	if len(list2) == count {
		return true
	}
	return false
}

// helper function to check that entries from list1 are all appear in list2
func CheckEntries(list1, list2 []string) bool {
	var out []string
	for _, k := range list1 {
		if InList(k, list2) {
			//             count += 1
			out = append(out, k)
		}
	}
	if len(out) == len(list1) {
		return true
	}
	return false
}

// helper function to convert given time into Unix timestamp
func UnixTime(ts string) int64 {
	// time is unix since epoch
	if len(ts) == 10 { // unix time
		tstamp, _ := strconv.ParseInt(ts, 10, 64)
		return tstamp
	}
	// YYYYMMDD, always use 2006 as year 01 for month and 02 for date since it is predefined int Go parser
	const layout = "20060102"
	t, err := time.Parse(layout, ts)
	if err != nil {
		panic(err)
	}
	return int64(t.Unix())
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

// helper function to convert Unix time into human readable form
func TimeFormat(ts float64) string {
	layout := "2006-01-02 15:04:05"
	return time.Unix(int64(ts), 0).UTC().Format(layout)
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

// helper function to test if given value is integer
func IsInt(val string) bool {
	pat := "(^[0-9-]$|^[0-9-][0-9]*$)"
	matched, _ := regexp.MatchString(pat, val)
	if matched {
		return true
	}
	return false
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
		fmt.Println(ts, val, now, sec)
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
