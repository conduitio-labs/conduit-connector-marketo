// Copyright © 2024 Meroxa, Inc.
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

package iterator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	marketoclient "github.com/rustiever/conduit-connector-marketo/marketo-client"
	"github.com/rustiever/conduit-connector-marketo/source/position"
	"gopkg.in/tomb.v2"
)

// ActivityTypeID to capture required CDC events.
// For reference https://developers.marketo.com/blog/synchronizing-lead-data-changes-using-rest-api/
const (
	ActivityTypeIDNewLead         = 12
	ActivityTypeIDChangeDataValue = 13
)

// custom Record type to handle CDC
type Record struct {
	id      int
	data    map[string]interface{}
	deleted bool
}

type CDCIterator struct {
	client       *marketoclient.Client // marketo client
	fields       []string              // fields to fetch from marketo
	buffer       chan Record           // buffer to store latest leads
	ticker       *time.Ticker          // ticker to poll marketo
	tomb         *tomb.Tomb            // tomb to handle errors in goRoutines
	lastModified time.Time             // last time fetched from marketo
	lastEntryKey string                // last key fetched from marketo
}

func NewCDCIterator(ctx context.Context, client *marketoclient.Client, pollingPeriod time.Duration, fields []string, lastModifiedTime time.Time, lastKey string) (*CDCIterator, error) {
	iterator := &CDCIterator{
		client:       client,
		buffer:       make(chan Record, 1),
		ticker:       time.NewTicker(pollingPeriod),
		tomb:         &tomb.Tomb{},
		fields:       fields,
		lastEntryKey: lastKey,
		lastModified: lastModifiedTime.UTC(),
	}
	iterator.tomb.Go(func() error {
		return iterator.poll(ctx)
	})
	return iterator, nil
}

// poll is the main goRoutine that polls marketo for new leads
func (c *CDCIterator) poll(ctx context.Context) error {
	defer close(c.buffer)
	for {
		select {
		case <-c.tomb.Dying():
			return c.tomb.Err()
		case <-c.ticker.C:
			err := c.flushLatestLeads(ctx)
			if err != nil {
				return err
			}
		}
	}
}

// returns true if there are more records to be read from the iterator's buffer, otherwise returns false.
func (c *CDCIterator) HasNext(ctx context.Context) bool {
	logger := sdk.Logger(ctx).With().Str("Method", "Has Next").Logger()
	logger.Trace().Msg("Checking iterator has next record...")
	return len(c.buffer) > 0 || !c.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

// returns Next record from the iterator's buffer, otherwise returns error.
func (c *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r := <-c.buffer:
		return c.prepareRecord(r)
	case <-c.tomb.Dead():
		return sdk.Record{}, c.tomb.Err()
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (c *CDCIterator) Stop() {
	// stop the goRoutines
	c.ticker.Stop()
	c.tomb.Kill(errors.New("cdc iterator is stopped"))
}

// returns record in the format of sdk.Record
func (c *CDCIterator) prepareRecord(r Record) (sdk.Record, error) {
	key := strconv.Itoa(r.id)
	if r.deleted {
		position := position.Position{
			Type:      position.TypeCDC,
			Key:       key,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		}
		pos, err := position.ToRecordPosition()
		if err != nil {
			return sdk.Record{}, err
		}

		metadata := make(sdk.Metadata)
		metadata.SetCreatedAt(time.Now())

		return sdk.Util.Source.NewRecordDelete(pos, metadata, sdk.RawData(key)), nil
	}
	createdAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", r.data["createdAt"]))
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error parsing createdAt %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", r.data["updatedAt"]))
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error parsing updatedAt %w", err)
	}
	position, _ := position.Position{
		Type:      position.TypeCDC,
		Key:       key,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}.ToRecordPosition()
	r.data["id"] = key

	metadata := make(sdk.Metadata)
	metadata["id"] = key
	metadata.SetCreatedAt(createdAt)
	metadata["updatedAt"] = strconv.FormatInt(updatedAt.UnixNano(), 10)

	if createdAt != updatedAt {
		return sdk.Util.Source.NewRecordUpdate(
			position, metadata, sdk.RawData(key), nil, sdk.StructuredData(r.data),
		), nil
	}

	return sdk.Util.Source.NewRecordCreate(
		position, metadata, sdk.RawData(key), sdk.StructuredData(r.data),
	), nil
}

// fetches latest leads from marketo and stores them in the buffer.
func (c *CDCIterator) flushLatestLeads(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Method", "flushLatestLeads").Logger()
	logger.Trace().Msg("Starting the flushLatestLeads")
	token, err := c.client.GetNextPageToken(c.lastModified)
	if err != nil {
		logger.Error().Err(err).Msg("Error while getting the next page token")
		return fmt.Errorf("error getting next page token %w", err)
	}
	c.lastModified = time.Now().UTC() // updating last modified time here to avoid missing any records in the next poll.
	changedLeadIds, changedLeadMaps, err := c.GetChangedLeadsIDs(ctx, token)
	if err != nil {
		logger.Error().Err(err).Msg("Error while getting the changed leads")
		return fmt.Errorf("error getting changed leads %w", err)
	}
	deletedLeadIds, err := c.GetDeletedLeadsIDs(ctx, token)
	if err != nil {
		logger.Error().Err(err).Msg("Error while getting the deleted leads")
		return fmt.Errorf("error getting deleted leads %w", err)
	}
	var lastKey = -1 // -1 indicates no last key, so proccess all leads
	if c.lastEntryKey != "" {
		lastKey, err = strconv.Atoi(c.lastEntryKey)
		if err != nil {
			logger.Error().Err(err).Msg("Error while parsing the last entry key")
			return fmt.Errorf("error parsing last entry key %w", err)
		}
	}
	for _, id := range deletedLeadIds {
		c.buffer <- Record{
			id:      id,
			deleted: true,
			data:    nil,
		}
	}
	if len(changedLeadIds) == 0 {
		return nil
	}
	var leads []map[string]interface{}
	var moreResult = true
	token = ""
	for moreResult {
		res, err := c.client.FilterLeads("id", changedLeadIds, c.fields, token)
		if err != nil {
			logger.Error().Err(err).Msg("Error while getting the changed leads")
			return fmt.Errorf("error getting changed leads %w", err)
		}
		moreResult = res.MoreResult
		token = res.NextPageToken
		err = json.Unmarshal(res.Result, &leads)
		if err != nil {
			logger.Error().Err(err).Msg("Error while unmarshalling the changed leads")
			return fmt.Errorf("error unmarshalling changed leads %w", err)
		}
	}

	for _, lead := range leads {
		id := int(lead["id"].(float64))
		if id <= lastKey && changedLeadMaps[id] == ActivityTypeIDNewLead {
			continue
		}
		c.buffer <- Record{
			id:      id,
			deleted: false,
			data:    lead,
		}
	}
	return nil
}

// returns list of deleted leads ids.
func (c *CDCIterator) GetDeletedLeadsIDs(_ context.Context, token string) ([]int, error) {
	response, err := c.client.GetDeletedLeads(token)
	if err != nil {
		return nil, err
	}
	if len(*response) == 0 {
		return nil, nil
	}
	var deletedLeadResults []map[string]interface{}
	err = json.Unmarshal(*response, &deletedLeadResults)
	if err != nil {
		return nil, err
	}
	var leadIds = make([]int, 0)
	for _, deletedLeadResult := range deletedLeadResults {
		var id = int(deletedLeadResult["leadId"].(float64))
		leadIds = append(leadIds, id)
	}
	return leadIds, nil
}

// returns list of changed leads ids.
func (c *CDCIterator) GetChangedLeadsIDs(_ context.Context, token string) ([]int, map[int]int, error) {
	var leadIds = make(map[int]int) // using map to avoid duplicates
	moreResult := true
	for moreResult {
		response, err := c.client.GetLeadChanges(token, c.fields)
		if err != nil {
			return nil, nil, err
		}
		if len(response.Result) == 0 {
			return nil, nil, nil
		}
		moreResult = response.MoreResult
		token = response.NextPageToken
		var leadChangeResults []map[string]interface{}
		err = json.Unmarshal(response.Result, &leadChangeResults)
		if err != nil {
			return nil, nil, err
		}
		for _, leadChangeResult := range leadChangeResults {
			var activityTypeID = leadChangeResult["activityTypeId"].(float64)
			if activityTypeID == ActivityTypeIDNewLead || activityTypeID == ActivityTypeIDChangeDataValue {
				var id = int(leadChangeResult["leadId"].(float64))
				leadIds[id] = int(activityTypeID)
			}
		}
	}
	keys := make([]int, 0, len(leadIds))
	for k := range leadIds {
		keys = append(keys, k)
	}
	// sorting helps in choosing last processed lead which handles
	// the case when there are multiple leads with same createdAt and updatedAt time.
	sort.Ints(keys)
	return keys, leadIds, nil
}
