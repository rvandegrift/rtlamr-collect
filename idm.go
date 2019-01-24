// Data aggregation for rtlamr.
// Copyright (C) 2017 Douglas Hall
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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bemasher/rtlamr/crc"
	"github.com/influxdata/influxdb/client/v2"
)

type IDMMessage struct {
	Time time.Time `json:"Time"`
	Type string    `json:"Type"`
	IDM  IDM       `json:"Message"`
}

type IDM struct {
	EndPointType  byte     `json:"ERTType"`
	EndPointID    uint32   `json:"ERTSerialNumber"`
	TransmitTime  uint16   `json:"TransmitTimeOffset"`
	IntervalCount byte     `json:"ConsumptionIntervalCount"`
	Intervals     []uint16 `json:"DifferentialConsumptionIntervals"`
	IDCRC         uint16   `json:"SerialNumberCRC"`
}

func (idm IDM) Tags(idx int) map[string]string {
	return map[string]string{
		"endpoint_id":   strconv.Itoa(int(idm.EndPointID)),
		"endpoint_type": strconv.Itoa(int(idm.EndPointType)),
	}
}

func (idm IDM) Fields(idx int) map[string]interface{} {
	return map[string]interface{}{
		"consumption": float64(idm.Intervals[idx]) * 10,
		"interval":    int64(uint(int(idm.IntervalCount)-idx) % 256),
	}
}

type Consumption struct {
	New   [256]bool
	Time  [256]time.Time
	Usage [256]float64
}

func (c Consumption) Fields(idx uint) map[string]interface{} {
	return map[string]interface{}{
		idmMeasurementName: float64(c.Usage[idx]),
	}
}

func (c *Consumption) Update(msg IDMMessage) {
	for idx := range c.New {
		c.New[idx] = false
	}

	timeOffset := time.Duration(msg.IDM.TransmitTime) * 62500 * time.Microsecond
	for idx, usage := range msg.IDM.Intervals {
		interval := uint(int(msg.IDM.IntervalCount)-idx) % 256
		t := msg.Time.Add(-time.Duration(idx)*5*time.Minute - timeOffset).Truncate(time.Second)

		diff := c.Time[interval].Sub(t)
		if c.Time[interval].IsZero() || diff > threshold || diff < -threshold {
			c.New[interval] = true
			c.Time[interval] = t
			c.Usage[interval] = float64(usage) * multiplier
		}
	}
}

type MeterMap map[uint32]Consumption

func (mm MeterMap) Preload(c client.Client) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE time > now() - 4h", idmMeasurementName)
	q := client.NewQuery(query, "distinct", "ns")
	if res, err := c.Query(q); err == nil && res.Error() == nil {
		for _, r := range res.Results {
			for _, s := range r.Series {
				for _, v := range s.Values {
					// time, usage, endpoint_id, endpoint_type, interval
					nsec, _ := v[0].(json.Number).Int64()
					usage, _ := v[1].(json.Number).Float64()
					interval, _ := v[2].(json.Number).Int64()
					id, _ := strconv.Atoi(v[3].(string))
					meter := uint32(id)

					if _, exists := mm[meter]; !exists {
						mm[meter] = Consumption{}
					}

					consumption := mm[meter]
					consumption.Time[interval] = time.Unix(0, nsec)
					consumption.Usage[interval] = usage
					mm[meter] = consumption
				}
			}
		}
	}

	log.Printf("Preloaded: %d", len(mm))
}
func handleIDM(msg Message, mm MeterMap) ([]*client.Point, error) {
	var idm IDMMessage
	tmp, _ := json.Marshal(&msg)
	json.Unmarshal(tmp, &idm)

	points := make([]*client.Point, 0)

	if !checkIDMCRC(idm.IDM.EndPointID, idm.IDM.IDCRC) {
		return points, errors.New("Message failed checksum")
	}

	if _, exists := mm[idm.IDM.EndPointID]; !exists {
		mm[idm.IDM.EndPointID] = Consumption{}
	}

	consumption := mm[idm.IDM.EndPointID]
	consumption.Update(idm)
	mm[idm.IDM.EndPointID] = consumption

	for idx := range idm.IDM.Intervals {
		interval := uint(int(idm.IDM.IntervalCount)-idx) % 256

		if !consumption.New[interval] {
			continue
		}

		pt, err := client.NewPoint(
			idmMeasurementName,
			idm.IDM.Tags(idx),
			consumption.Fields(interval),
			consumption.Time[interval],
		)

		if err != nil {
			log.Println(err)
		} else {
			points = append(points, pt)
		}
	}

	return points, nil
}

func checkIDMCRC(EndPointID uint32, IDCRC uint16) bool {
	buf := make([]byte, 6)
	idmCRC := crc.NewCRC("CCITT", 0xFFFF, 0x1021, 0x1D0F)

	binary.BigEndian.PutUint32(buf[:4], EndPointID)
	binary.BigEndian.PutUint16(buf[4:], IDCRC)
	if residue := idmCRC.Checksum(buf); residue != idmCRC.Residue {
		return false
	}

	return true
}
