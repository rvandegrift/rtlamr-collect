// Data aggregation for rtlamr.
// Copyright (C) 2017 Douglas Hall, 2019 Ross Vandegrift
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

const (
	host = "http://%s:8086"

	threshold = 30 * time.Second
)

var multiplier float64
var dbName string
var idmMeasurementName string
var scmMeasurementName string

type Message struct {
	Time    time.Time   `json:"Time"`
	Type    string      `json:"Type"`
	Message interface{} `json:"Message"`
}

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	stdinBuf := bufio.NewScanner(os.Stdin)

	hostname, ok := os.LookupEnv("COLLECT_INFLUXDB_HOSTNAME")
	if !ok {
		log.Fatal("COLLECT_INFLUXDB_HOSTNAME undefined")
	}

	username, ok := os.LookupEnv("COLLECT_INFLUXDB_USER")
	if !ok {
		log.Fatal("COLLECT_INFLUXDB_USER undefined")
	}

	password, ok := os.LookupEnv("COLLECT_INFLUXDB_PASS")
	if !ok {
		log.Fatal("COLLECT_INFLUXDB_PASS undefined")
	}

	multiplierEnv, ok := os.LookupEnv("COLLECT_MULTIPLIER")
	if ok {
		var err error
		multiplier, err = strconv.ParseFloat(multiplierEnv, 64)
		if err != nil {
			log.Fatal("COLLECT_MULTIPLIER is defined and does not contain a number")
		}
	} else {
		multiplier = 10.0
	}

	dbNameEnv, ok := os.LookupEnv("COLLECT_INFLUXDB_DATABASE")
	if ok {
		dbName = dbNameEnv
	} else {
		dbName = "rtlamr"
	}
	log.Printf("using database name \"%s\"", dbName)

	idmMeasurementEnv, ok := os.LookupEnv("COLLECT_INFLUXDB_IDM_MEASUREMENT_NAME")
	if ok {
		idmMeasurementName = idmMeasurementEnv
	} else {
		idmMeasurementName = "power"
	}
	log.Printf("using measurement name \"%s\" for IDM", idmMeasurementName)

	scmMeasurementEnv, ok := os.LookupEnv("COLLECT_INFLUXDB_SCM_MEASUREMENT_NAME")
	if ok {
		scmMeasurementName = scmMeasurementEnv
	} else {
		scmMeasurementName = "power"
	}
	log.Printf("using measurement name \"%s\" for SCM", scmMeasurementName)

	log.Printf("connecting to %q@%q", username, fmt.Sprintf(host, hostname))
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     fmt.Sprintf(host, hostname),
		Username: username,
		Password: password,
	})
	if err != nil {
		log.Fatalln(err)
	}

	batchPointConfig := client.BatchPointsConfig{
		Database:  dbName,
		Precision: "s",
	}

	mm := make(MeterMap)
	mm.Preload(c)

	for stdinBuf.Scan() {
		var msg Message
		err := json.Unmarshal(stdinBuf.Bytes(), &msg)
		if err != nil {
			log.Println(err)
			continue
		}

		bp, err := client.NewBatchPoints(batchPointConfig)
		if err != nil {
			log.Println(err)
			continue
		}

		var points []*client.Point
		if msg.Type == "SCM" {
			log.Println("Received SCM message")
			points, err = handleSCM(msg)

		} else if msg.Type == "IDM" {
			log.Println("Received IDM message")
			points, err = handleIDM(msg, mm)

		} else {
			log.Println("Ignoring unknown msg type %s", msg.Type)
			continue
		}

		if err != nil {
			log.Println(err)
			continue
		}

		for pt := range points {
			bp.AddPoint(points[pt])
		}
		err = c.Write(bp)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}
