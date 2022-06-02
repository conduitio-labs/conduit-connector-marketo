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

package iterator

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	marketoclient "github.com/rustiever/conduit-connector-marketo/marketo-client"
	"github.com/rustiever/conduit-connector-marketo/source/position"
	"golang.org/x/sync/errgroup"
)

const (
	// is the maximum number of days between two snapshots. If the gap between two snapshots is greater than this number,
	// API will return an error. This is limitation of the API.
	MaximumDaysGap = 744 // 31 days in Hours
)

var (
	InitialDate time.Time // holds the initial date of the snapshot
)

// to handle snapshot iterator
type SnapshotIterator struct {
	client          *marketoclient.Client
	fields          []string         // holds the fields to be returned from the API
	endpoint        string           // holds the endpoint of the API
	exportID        string           // holds the current processin exportId
	iteratorCount   int              // holds the number of snapshots to be created
	errChan         chan error       // used to send errors
	csvReader       chan *csv.Reader // holds bulk data returned from the API in CSV format
	data            chan []string    // holds the data to be flushed to the conduit
	hasData         chan struct{}    // used to signal that the iterator has data
	lastMaxModified time.Time        // holds the last maxModified date of the snapshot
}

// returns NewSnapshotIterator with supplied parameters, also initiates the pull and flush goroutines.
func NewSnapshotIterator(ctx context.Context, endpoint string, fields []string, client marketoclient.Client, p position.Position) (*SnapshotIterator, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "NewSnapshotIterator").Logger()
	logger.Trace().Msg("Starting the NewSnapshotIterator")
	var err error
	s := &SnapshotIterator{
		endpoint:        endpoint,
		client:          &client,
		fields:          fields,
		errChan:         make(chan error),
		data:            make(chan []string, 100),
		hasData:         make(chan struct{}, 100),
		lastMaxModified: time.Time{},
	}
	eg, ctx := errgroup.WithContext(ctx)
	if InitialDate.IsZero() {
		InitialDate, err = s.getLastProcessedDate(ctx, p)
	}
	if err != nil {
		logger.Error().Err(err).Msg("Error getting initial date")
		return nil, err
	}
	startDateDuration := time.Since(InitialDate)
	s.iteratorCount = int(startDateDuration.Hours()/MaximumDaysGap) + 1
	logger.Info().Msgf("Creating %d snapshots", s.iteratorCount)
	s.csvReader = make(chan *csv.Reader, s.iteratorCount)
	eg.Go(func() error {
		return s.pull(ctx)
	})
	eg.Go(func() error {
		return s.flush(ctx)
	})
	go func() {
		err := eg.Wait()
		logger.Trace().Msg("Errgroup wait finished")
		if err != nil {
			logger.Error().Err(err).Msg("Error waiting for errGroup")
			s.errChan <- err
		}
	}()
	return s, nil
}

// returns true if there are more records to be read from the iterator's buffer, otherwise returns false.
func (s *SnapshotIterator) HasNext(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		err2 := s.stop(ctx)
		if err2 != nil {
			sdk.Logger(ctx).Error().Err(err2).Msg("Error while stopping the SnapshotIterator")
		}
		sdk.Logger(ctx).Info().Msg("Stopping the SnapshotIterator..." + ctx.Err().Error())
		return false
	case _, ok := <-s.hasData:
		if !ok {
			break
		}
		return true
	}
	if len(s.data) > 0 {
		return true
	} else {
		return false
	}
}

// returns Next record from the iterator's buffer, otherwise returns error.
func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "Next").Logger()
	logger.Trace().Msg("Starting the Next method")

	select {
	case <-ctx.Done():
		err2 := s.stop(ctx)
		if err2 != nil {
			logger.Error().Err(err2).Msg("Error while stopping the SnapshotIterator")
		}
		return sdk.Record{}, ctx.Err()
	case err1 := <-s.errChan:
		logger.Error().Err(err1).Msg("Error while pulling from Marketo or flushing to buffer")
		logger.Info().Msg("Stopping the SnapshotIterator...")
		err2 := s.stop(ctx)
		if err2 != nil {
			logger.Error().Err(err2).Msg("Error while stopping the SnapshotIterator")
		}
		return sdk.Record{}, fmt.Errorf("%s and %s", err1.Error(), err2.Error())
	case data, ok := <-s.data:
		if !ok {
			logger.Info().Msg("Buffer is empty")
			return sdk.Record{}, sdk.ErrBackoffRetry
		}
		record, err := s.prepareRecord(ctx, data)
		if err != nil {
			logger.Error().Err(err).Msg("Error while preparing record")
			return sdk.Record{}, err
		}
		return record, nil
	}
}

// stops the processing of the snapshot.
func (s *SnapshotIterator) stop(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Method", "Stop").Logger()
	logger.Trace().Msg("Starting the SnapshotIterator Stop method")
	if s.exportID == "" {
		logger.Trace().Msg("No exportId to cancel")
		return nil
	}
	err := s.client.CancelExportLeads(s.exportID)
	if errors.Is(err, marketoclient.ErrCannotCancel) {
		logger.Err(err).Msg("Cannot cancel export")
		return nil
	} else if err != nil {
		logger.Error().Err(err).Msg("Error while cancelling export")
		return err
	}
	return nil
}

// continuesly pulls the data from the Marketo API.
func (s *SnapshotIterator) pull(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Method", "pull").Logger()
	logger.Trace().Msg("Starting the pull")
	defer close(s.csvReader)
	var startDate, endDate time.Time
	date := InitialDate
	for i := 0; i < s.iteratorCount; i++ {
		startDate = date
		endDate = date.Add(time.Hour * time.Duration(MaximumDaysGap)).Add(-1 * time.Second)
		date = date.Add(time.Hour * time.Duration(MaximumDaysGap))
		logger.Info().Msgf("Pulling data from %s to %s", startDate.Format(time.RFC3339), endDate.Format(time.RFC3339))
		err := s.getLeads(ctx, startDate, endDate)
		if err != nil {
			logger.Error().Err(err).Msg("Error while getting snapshot of leads")
			return err
		}
	}
	return nil
}

// flushes data from csvReader channel to buffer .
func (s *SnapshotIterator) flush(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Method", "flush").Logger()
	logger.Trace().Msg("Starting the flush method")
	defer func() {
		close(s.data)
		close(s.hasData)
	}()
	for reader := range s.csvReader {
		for {
			rec, err := reader.Read()
			if err == io.EOF {
				logger.Trace().Msg("EOF reached")
				break
			}
			if err != nil {
				logger.Err(err).Msg("Error while reading csv")
				return err
			}
			s.data <- rec
			s.hasData <- struct{}{}
		}
	}
	return nil
}

// requests the data from the Marketo API and pushes it to the csvReader channel.
func (s *SnapshotIterator) getLeads(ctx context.Context, startDate, endDate time.Time) error {
	logger := sdk.Logger(ctx).With().Str("Method", "getLeads").Logger()
	logger.Trace().Msg("Starting the getLeads method")
	var err error
	s.exportID, err = s.client.CreateExportLeads(s.fields, startDate.UTC().Format(time.RFC3339), endDate.UTC().Format(time.RFC3339))
	if err != nil {
		logger.Error().Err(err).Msg("Error while creating export")
		return err
	}
	err = marketoclient.WithRetry(ctx, func() (bool, error) {
		_, err := s.client.EnqueueExportLeads(s.exportID)
		if errors.Is(err, marketoclient.ErrEnqueueLimit) {
			logger.Trace().Msg("Enqueue limit reached")
			return true, nil
		}
		if err != nil {
			logger.Err(err).Msg("Error while enqueuing export")
			return false, err
		}
		return false, nil
	})
	if err != nil {
		logger.Err(err).Msg("Error while enqueuing export")
		return err
	}

	err = marketoclient.WithRetry(ctx, func() (bool, error) {
		statusResult, err := s.client.StatusOfExportLeads(s.exportID)
		if err != nil {
			logger.Err(err).Msg("Error while getting status of export")
			return false, err
		}
		if statusResult.Status == "Completed" {
			if statusResult.NumberOfRecords == 0 {
				logger.Trace().Msg("Skipping empty export")
				return false, marketoclient.ErrZeroRecords
			}
			return false, nil
		}
		return true, nil
	})
	if errors.Is(err, marketoclient.ErrZeroRecords) {
		logger.Trace().Msgf("Skipping,Zero records found for %s", s.exportID)
		return nil
	}
	if err != nil {
		logger.Err(err).Msg("Error while getting status of export")
		return err
	}
	bytes, err := s.client.FileExportLeads(ctx, s.endpoint, s.exportID)
	if err != nil {
		logger.Err(err).Msg("Error while getting file of export")
		return err
	}
	csvReader := csv.NewReader(strings.NewReader(string(*bytes)))
	_, err = csvReader.Read() // removing the header
	if err != nil {
		logger.Err(err).Msg("Error while reading csv")
		return err
	}
	logger.Trace().Msg("Sending csv reader to channel")
	s.csvReader <- csvReader

	return nil
}

// prepares and returns record in sdk.Record format. If process fails for any reason, it returns error.
func (s *SnapshotIterator) prepareRecord(ctx context.Context, data []string) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "prepareRecord").Logger()
	logger.Trace().Msg("Starting the prepareRecord method")
	var dataMap = marketoclient.GetDataMap(s.fields, data)
	createdAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", dataMap["createdAt"]))
	if err != nil {
		logger.Err(err).Msg("Error while parsing createdAt")
		return sdk.Record{}, fmt.Errorf("error parsing createdAt %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", dataMap["updatedAt"]))
	if err != nil {
		logger.Err(err).Msg("Error while parsing updatedAt")
		return sdk.Record{}, fmt.Errorf("error parsing updatedAt %w", err)
	}
	if updatedAt.After(s.lastMaxModified) {
		s.lastMaxModified = updatedAt
	}
	position := position.Position{
		Key:       (dataMap["id"].(string)),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		Type:      position.TypeSnapshot,
	}
	pos, err := position.ToRecordPosition()
	if err != nil {
		logger.Err(err).Msg("Error while converting position to record position")
		return sdk.Record{}, fmt.Errorf("error converting position to record position %w", err)
	}
	rec := sdk.Record{
		Payload: sdk.StructuredData(dataMap),
		Metadata: map[string]string{
			"id":        position.Key,
			"createdAt": createdAt.Format(time.RFC3339),
			"updatedAt": updatedAt.Format(time.RFC3339),
		},
		Position: pos,
		Key:      sdk.RawData(position.Key),
	}

	return rec, nil
}

// returns Last date from the supplied position.if p is zero value, then it queries least date from the database.
func (s *SnapshotIterator) getLastProcessedDate(ctx context.Context, p position.Position) (time.Time, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "getInitialDate").Logger()
	logger.Trace().Msg("Starting the getInitialDate method")
	var date = p.CreatedAt.Add(1 * time.Second)
	var err error
	if reflect.ValueOf(p).IsZero() {
		date, err = s.getLeastDate(ctx, *s.client)
		if err != nil {
			sdk.Logger(ctx).Error().Err(err).Msg("Failed to get the oldest date from marketo")
			return time.Time{}, err
		}
	}
	return date, nil
}

// returns least date of all leads.
func (s *SnapshotIterator) getLeastDate(ctx context.Context, client marketoclient.Client) (time.Time, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "GetOldestDateFromMarketo").Logger()
	logger.Trace().Msg("Starting the GetOldestDateFromMarketo")

	folderResult, err := client.GetAllFolders(1)
	if err != nil {
		logger.Error().Err(err).Msg("Error while getting the folders")
		return time.Time{}, err
	}
	oldestTime := time.Now().UTC()
	for _, v := range folderResult {
		date, _, found := cut(v.CreatedAt, "+")
		if !found {
			logger.Error().Msgf("Error while parsing the date %s", v.CreatedAt)
			return time.Time{}, fmt.Errorf("error while parsing the date %s", v.CreatedAt)
		}
		t, err := time.Parse(time.RFC3339, date)
		if err != nil {
			logger.Error().Err(err).Msgf("Error while parsing the date %s", date)
			return time.Time{}, err
		}
		if t.Before(oldestTime) {
			oldestTime = t
		}
	}

	return oldestTime.UTC(), nil
}

// Cut slices s around the first instance of sep,
// returning the text before and after sep.
// The found result reports whether sep appears in s.
// If sep does not appear in s, cut returns s, "", false.
func cut(s, sep string) (before, after string, found bool) {
	if i := strings.Index(s, sep); i >= 0 {
		return s[:i], s[i+len(sep):], true
	}
	return s, "", false
}
