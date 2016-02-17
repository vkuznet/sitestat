/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: utils for cms package
 * Created    : Wed Feb 10 19:31:44 EST 2016
 */
package cms

import (
	"fmt"
	"sort"
	"strings"
	"utils"
)

func datasetNameOk(dataset string) bool {
	pieces := strings.Split(dataset, "/")
	if len(pieces) == 4 { // /a/b/c -> ["", a, b, c]
		return true
	}
	return false
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
func updateBreakdown(breakdown string, rec Record, data []Record) Record {
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
			dataset := idict["dataset"].(string)
			size := idict["size"].(float64)
			rec[dataset] = size
		}
		return rec
	} else if breakdown == "" {
		return rec
	} else {
		msg := fmt.Sprintf("Unsupported breakdown '%s'", breakdown)
		panic(msg)
	}
}
