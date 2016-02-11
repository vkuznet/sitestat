/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: part of cms package responsible for popDB calls
 * Created    : Wed Feb 10 19:31:44 EST 2016
 */
package cms

import (
	"encoding/json"
	"fmt"
	"strings"
	"utils"
)

// helper function to load data stream and return DAS records
func loadPhedexData(furl string, data []byte) []Record {
	var out []Record
	var rec Record
	err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("unable to unmarshal the data into record, furl=%s, data=%s, error=%v", furl, string(data), err)
		panic(msg)
	}
	out = append(out, rec)
	return out
}

// helper function to find all dataset at a given tier-site
func datasetsAtSite(siteName string) []string {
	var out []string
	api := "blockreplicasummary"
	furl := fmt.Sprintf("%s/%s?node=%s", phedexUrl(), api, siteName)
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl)
	}
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadPhedexData(furl, response.Data)
		for _, rec := range records {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				dataset := strings.Split(brec["name"].(string), "#")[0]
				out = append(out, dataset)
			}
		}
		return utils.List2Set(out)
	}
	return out
}
