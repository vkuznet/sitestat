/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: part of cms package responsible for static URLs
 * Created    : Wed Feb 10 19:31:44 EST 2016
 */
package cms

func dbsUrl() string {
	return "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
}
func phedexUrl() string {
	return "https://cmsweb.cern.ch/phedex/datasvc/json/prod"
}
func sitedbUrl() string {
	return "https://cmsweb.cern.ch/sitedb/data/prod"
}
func popdbUrl() string {
	return "https://cmsweb.cern.ch/popdb/popularity"
}

// main record we work with
type Record map[string]interface{}
type BinRecord map[int]interface{}
