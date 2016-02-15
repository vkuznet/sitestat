/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: utils for cms package
 * Created    : Wed Feb 10 19:31:44 EST 2016
 */
package cms

import (
	"fmt"
)

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
