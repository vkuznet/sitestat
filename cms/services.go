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
func victordbUrl() string {
	return "https://cmsweb.cern.ch/popdb/victorinterface"
}

// main record we work with
type Record map[string]interface{}
type BinRecord map[int]interface{}
