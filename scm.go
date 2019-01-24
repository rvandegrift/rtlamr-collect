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
	"encoding/json"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type SCMMessage struct {
	Time time.Time `json:"Time"`
	Type string    `json:"Type"`
	SCM  SCM       `json:"Message"`
}

type SCM struct {
	EndPointType byte   `json:"Type"`
	EndPointID   uint32 `json:"ID"`
	TamperPhy    byte   `json:"TamperPhy"`
	TamperEnc    byte   `json:"TamperEnc"`
	Consumption  uint32 `json:"Consumption"`
	ChecksumVal  uint16 `json:"ChecksumVal"`
}

func (scm SCM) Tags() map[string]string {
	return map[string]string{
		"endpoint_id":   strconv.Itoa(int(scm.EndPointID)),
		"endpoint_type": strconv.Itoa(int(scm.EndPointType)),
	}
}

func (scm SCM) Fields() map[string]interface{} {
	return map[string]interface{}{
		"consumption": float64(scm.Consumption),
	}
}

func handleSCM(msg Message) ([]*client.Point, error) {
	var scm SCMMessage
	tmp, _ := json.Marshal(&msg)
	json.Unmarshal(tmp, &scm)

	points := make([]*client.Point, 0, 1)
	pt, err := client.NewPoint(
		scmMeasurementName,
		scm.SCM.Tags(),
		scm.SCM.Fields(),
		scm.Time,
	)
	points = append(points, pt)

	return points, err
}
