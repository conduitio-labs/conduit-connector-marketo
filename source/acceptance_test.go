// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"

	marketo "github.com/rustiever/conduit-connector-marketo"
	"github.com/rustiever/conduit-connector-marketo/source"
	"github.com/rustiever/conduit-connector-marketo/source/position"
	"go.uber.org/goleak"
)

func TestAcceptance(t *testing.T) {
	src := &source.Source{}
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: marketo.Specification,
					NewSource:        func() sdk.Source { return src },
				},
				SourceConfig: getConfigs(),
				GoleakOptions: []goleak.Option{
					goleak.IgnoreCurrent(),
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
					// external dependency - minimarketo
					goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).roundTrip"),
					goleak.IgnoreTopFunction("net/http.setRequestCancel.func4"),
				},
				AfterTest: func(t *testing.T) {
					t.Cleanup(func() {
						err := cleanUp(client)
						if err != nil {
							t.Error(err)
						}
					})
				},
				ReadTimeout: time.Minute * 5,
			},
		},
	},
	)
}

// AcceptanceTestDriver implements sdk.AcceptanceTestDriver.
type AcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

// WriteToSource writes data for source to pull data from.
func (d AcceptanceTestDriver) WriteToSource(t *testing.T, records []opencdc.Record) []opencdc.Record {
	var err error
	is := is.New(t)
	client, err := getClient()
	is.NoErr(err)
	leads, err := client.addLeads(len(records))
	is.NoErr(err)
	emailIDs := make([]string, 0)
	for _, v := range leads {
		emailIDs = append(emailIDs, v["email"].(string))
	}
	records, err = writeRecords(client, emailIDs)
	if err != nil {
		t.Error(err)
	}

	return records
}

func writeRecords(client Client, emailIDs []string) ([]opencdc.Record, error) {
	data, err := client.filterLeads("email", emailIDs)
	if err != nil {
		return nil, err
	}
	var record []map[string]interface{}
	err = json.Unmarshal(*data, &record)
	if err != nil {
		return nil, err
	}
	records := make([]opencdc.Record, 0)
	for _, v := range record {
		id := int(v["id"].(float64))
		data, err = client.getLeadByID(id, Fields)
		if err != nil {
			return nil, err
		}
		var rec []map[string]interface{}
		err = json.Unmarshal(*data, &rec)
		if err != nil {
			return nil, err
		}
		rec[0]["id"] = fmt.Sprint(id)
		r, err := prepareRecord(rec[0])
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, err
}

func prepareRecord(data map[string]interface{}) (opencdc.Record, error) {
	createdAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", data["createdAt"]))
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("error parsing createdAt %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", data["updatedAt"]))
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("error parsing updatedAt %w", err)
	}
	position := position.Position{
		Key:       fmt.Sprint(data["id"]),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		Type:      position.TypeSnapshot,
	}
	pos, err := position.ToRecordPosition()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("error converting position to record position %w", err)
	}

	metadata := make(opencdc.Metadata)
	metadata["id"] = position.Key
	metadata.SetCreatedAt(createdAt)
	metadata["updatedAt"] = strconv.FormatInt(updatedAt.UnixNano(), 10)

	return sdk.Util.Source.NewRecordCreate(
		pos, metadata, opencdc.RawData(position.Key), opencdc.StructuredData(data),
	), nil
}
