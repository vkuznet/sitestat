// PBR module, see https://github.com/aurimasrep/PhedexReplicaMonitoring
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
// SQLite: https://astaxie.gitbooks.io/build-web-application-with-golang/content/en/05.3.html
// database/sql http://go-database-sql.org/index.html
package cms

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/sitestat/utils"
)

func sizeFromPBR(dbname, dataset, phgroup string) float64 {
	db, err := sql.Open("sqlite3", dbname)
	checkErr(err)
	defer db.Close()

	// query
	rows, err := db.Query("SELECT dataset,size FROM avg where dataset = ? and phgroup = ?", dataset, phgroup)
	checkErr(err)
	defer rows.Close()

	var dbDataset string
	var size float64
	for rows.Next() {
		err = rows.Scan(&dbDataset, &size)
		checkErr(err)
		if dbDataset == dataset {
			break
		}
	}
	return size
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func datasetInfoPBR(dataset string, ch chan Record) {
	rec := make(Record)
	rec["name"] = dataset
	rec["size"] = sizeFromPBR(PBRDB, dataset, PHGROUP)
	rec["tier"] = utils.DataTier(dataset)
	ch <- rec
}
