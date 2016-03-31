package cms

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/sitestat/utils"
	"strings"
)

// helper function to load SiteDB data stream
func loadSiteDBData(furl string, data []byte) []Record {
	var out []Record
	var rec Record
	err := json.Unmarshal(data, &rec)
	if err != nil {
		if utils.VERBOSE > 0 {
			msg := fmt.Sprintf("SiteDB unable to unmarshal the data, furl=%s, data=%s, error=%v", furl, string(data), err)
			fmt.Println(msg)
		}
		return out
	}
	desc := rec["desc"].(map[string]interface{})
	headers := desc["columns"].([]interface{})
	values := rec["result"].([]interface{})
	for _, item := range values {
		row := make(Record)
		val := item.([]interface{})
		for i, h := range headers {
			key := h.(string)
			row[key] = val[i]
			if key == "username" {
				row["name"] = row[key]
			}
		}
		out = append(out, row)
	}
	return out
}

// helper function to get CMS site names for given siteName
func siteNames(site string) []string {
	var out []string
	api := "site-names"
	furl := fmt.Sprintf("%s/%s", sitedbUrl(), api)
	if utils.VERBOSE > 1 {
		fmt.Println("furl", furl)
	}
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadSiteDBData(furl, response.Data)
		for _, r := range records {
			siteName := r["alias"].(string)
			siteType := r["type"].(string)
			if siteType == "phedex" && strings.HasPrefix(siteName, site) {
				if strings.HasPrefix(site, "T1_") {
					if strings.HasSuffix(siteName, "_Disk") {
						out = append(out, siteName)
					}
				} else {
					out = append(out, siteName)
				}
			}
		}
	}
	return utils.List2Set(out)
}
