package cms

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/vkuznet/sitestat/utils"
)

func datasetNameOk(dataset string) bool {
	pieces := strings.Split(dataset, "/")
	if len(pieces) == 4 { // /a/b/c -> ["", a, b, c]
		return true
	}
	return false
}

// helper function to yield CSV format
func formatCSV(bins []int, records []Record) {
	for _, rec := range records {
		for site, vals := range rec {
			out := site
			rec := vals.(Record)
			results := rec["results"].(BinRecord)
			ikeys := utils.MapIntKeys(results)
			sort.Ints(ikeys)
			for _, bin := range ikeys {
				size := results[bin].(float64)
				out += fmt.Sprintf(",%f", size)
			}
			fmt.Println(out)
		}
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
				brec := bresults[bin]
				if brec != nil {
					if bin == bins[len(bins)-1] {
						pad = "+"
					}
				}
				report += fmt.Sprintf("%s %d%s size %f (%s)\n", metric, bin, pad, size, utils.SizeFormat(size))
				if brec != nil {
					bdown := bresults[bin].(Record)
					report += formatBreakdown(bdown, breakdown)
				}
			}
			fmt.Println(report)
		}
	}
}

// helper function to format aggregated breakdown results
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
		dict[binValue] = arr
	} else {
		dict[binValue] = []string{val}
	}
}

// helper function to sum-up size attribute from Record
func sumSize(data []Record) float64 {
	out := 0.0
	for _, idict := range data {
		v := idict["size"].(float64)
		out += v
	}
	return out
}

// helper function to sum-up size attribute from Record and group it by data tier
func sumSizeTier(data []Record) Record {
	odict := make(Record)
	for _, idict := range data {
		val := idict["size"].(float64)
		tier := idict["tier"].(string)
		oval, ok := odict[tier]
		if ok {
			odict[tier] = oval.(float64) + val
		} else {
			odict[tier] = val
		}
	}
	return odict
}

// helper function to update breakdown records
func updateBreakdown(breakdown string, data []Record) Record {
	rec := make(Record)
	if breakdown == "tier" {
		bdata := sumSizeTier(data)
		for k, v := range bdata {
			val, ok := rec[k]
			if ok {
				rec[k] = val.(float64) + v.(float64) // update size value
			} else {
				rec[k] = v.(float64)
			}
		}
		return rec
	} else if breakdown == "dataset" {
		for _, idict := range data {
			switch v := idict["dataset"].(type) {
			case string:
				dataset := strings.Split(v, "#")[0]
				size := idict["size"].(float64)
				rec[dataset] = size
			}
		}
		return rec
	} else if breakdown == "block" {
		for _, idict := range data {
			block := idict["block"].(string)
			size := idict["size"].(float64)
			rec[block] = size
		}
		return rec
	} else if breakdown == "" {
		tot := 0.0
		for _, idict := range data {
			size := idict["size"].(float64)
			tot += size
		}
		rec[breakdown] = tot
		return rec
	} else {
		msg := fmt.Sprintf("Unsupported breakdown '%s'", breakdown)
		panic(msg)
	}
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

// helper function to check data-tier
func keepDataTier(name, tier string) bool {
	keep := true
	dataTier := utils.DataTier(name)
	if tier != "" && dataTier != tier {
		keep = false
	}
	return keep
}
