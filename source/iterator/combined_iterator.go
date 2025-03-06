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
	"errors"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	marketoclient "github.com/rustiever/conduit-connector-marketo/marketo-client"
	"github.com/rustiever/conduit-connector-marketo/source/position"
)

type CombinedIterator struct {
	snapshotIterator *SnapshotIterator
	cdcIterator      *CDCIterator

	endpoint      string
	pollingPeriod time.Duration
	fields        []string
	client        marketoclient.Client
}

var ErrDone = errors.New("no more records in iterator")

func NewCombinedIterator(ctx context.Context, endpoint string, pollingPeriod time.Duration, client marketoclient.Client, p position.Position, fields []string, initialDate time.Time) (*CombinedIterator, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "NewCombinedIterator").Logger()
	logger.Trace().Msg("Starting the NewCombinedIterator")

	var err error
	c := &CombinedIterator{
		endpoint:      endpoint,
		pollingPeriod: pollingPeriod,
		client:        client,
		fields:        fields,
	}

	switch p.Type {
	case position.TypeSnapshot:
		logger.Trace().Msg("Starting creating a New Snaphot iterator")

		c.snapshotIterator, err = NewSnapshotIterator(ctx, endpoint, fields, client, p, initialDate)
		if err != nil {
			logger.Error().Err(err).Msg("Error while creating a new snapshot iterator")
			return nil, err
		}

		logger.Trace().Msg("Sucessfully created the New Snaphot iterator")
	case position.TypeCDC:
		logger.Trace().Msg("Starting creating a New CDC iterator")

		c.cdcIterator, err = NewCDCIterator(ctx, &client, pollingPeriod, fields, p.UpdatedAt, p.Key)
		if err != nil {
			logger.Error().Err(err).Msg("Error while creating a new CDC iterator")
			return nil, err
		}

	default:
		// this case should never happen
		return nil, fmt.Errorf("invalid position type (%d)", p.Type)
	}

	return c, nil
}

func (c *CombinedIterator) HasNext(ctx context.Context) bool {
	switch {
	case c.snapshotIterator != nil:
		// case of empty database or end of database
		if !c.snapshotIterator.HasNext(ctx) {
			sdk.Logger(ctx).Info().Msg("Switching to CDC iterator...")
			err := c.switchToCDCIterator(ctx, "") // empty string no last key, so process all records
			if err != nil {
				sdk.Logger(ctx).Err(err).Msg("Error while switching to CDC iterator")
				return false
			}
			return false
		}
		return true
	case c.cdcIterator != nil:
		return c.cdcIterator.HasNext(ctx)
	default:
		return false
	}
}

func (c *CombinedIterator) Next(ctx context.Context) (opencdc.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Method", "Next").Logger()
	logger.Trace().Msg("Starting the Combined Iterator Next")

	switch {
	case c.snapshotIterator != nil:
		record, err := c.snapshotIterator.Next(ctx)
		if err != nil {
			return opencdc.Record{}, err
		}
		if !c.snapshotIterator.HasNext(ctx) {
			logger.Info().Msg("Switching to CDC iterator...")
			err := c.switchToCDCIterator(ctx, string(record.Key.Bytes()))
			if err != nil {
				return opencdc.Record{}, err
			}
			record.Position, err = position.ConvertToCDCPosition(record.Position)
			if err != nil {
				return opencdc.Record{}, err
			}
		}
		return record, nil

	case c.cdcIterator != nil:
		return c.cdcIterator.Next(ctx)
	default:
		logger.Error().Msg("Both the itertors are not initailsed")
		return opencdc.Record{}, errors.New("no initialized iterator")
	}
}

func (c *CombinedIterator) Stop() {
	if c.cdcIterator != nil {
		c.cdcIterator.Stop()
	}
}

func (c *CombinedIterator) switchToCDCIterator(ctx context.Context, fromKey string) error {
	lastModifiedTime := c.snapshotIterator.lastMaxModified
	if lastModifiedTime.IsZero() {
		lastModifiedTime = time.Now().UTC()
	}
	var err error
	c.cdcIterator, err = NewCDCIterator(ctx, &c.client, c.pollingPeriod, c.fields, lastModifiedTime, fromKey)
	if err != nil {
		return fmt.Errorf("could not create cdc iterator: %w", err)
	}
	c.snapshotIterator = nil
	return nil
}
