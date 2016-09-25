// PBR module, see https://github.com/aurimasrep/PhedexReplicaMonitoring
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//
// References:
// database/sql http://go-database-sql.org/index.html
// SQLite: https://astaxie.gitbooks.io/build-web-application-with-golang/content/en/05.3.html
package cms

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/sitestat/utils"
)

type PBR struct {
	records map[string]float64
}

func (p *PBR) Map() map[string]float64 {
	if p.records != nil {
		return p.records
	}
	db, err := sql.Open("sqlite3", PBRDB)
	checkErr(err)
	defer db.Close()
	err = db.Ping()
	checkErr(err)
	rows, err := db.Query("SELECT dataset,size FROM avg where phgroup = ?", PHGROUP)
	checkErr(err)
	defer rows.Close()

	p.records = make(map[string]float64)
	for rows.Next() {
		var dataset string
		var size float64
		err = rows.Scan(&dataset, &size)
		checkErr(err)
		p.records[dataset] = size
	}
	return p.records
}

func sizeFromPBR(dataset, phgroup string) float64 {
	rows, err := PDB.Query("SELECT dataset,size FROM avg where dataset = ? and phgroup = ?", dataset, phgroup)
	checkErr(err)
	defer rows.Close()

	var size float64
	for rows.Next() {
		err = rows.Scan(&dataset, &size)
		checkErr(err)
		break
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
	rec["size"] = PBRMAP[dataset]
	//     rec["size"] = sizeFromPBR(dataset, PHGROUP)
	rec["tier"] = utils.DataTier(dataset)
	ch <- rec
}
