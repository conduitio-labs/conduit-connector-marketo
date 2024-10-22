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

package source

import (
	"context"
	"fmt"

	"github.com/SpeakData/minimarketo"
	sdk "github.com/conduitio/conduit-connector-sdk"
	globalConfig "github.com/rustiever/conduit-connector-marketo/config"
	marketoclient "github.com/rustiever/conduit-connector-marketo/marketo-client"
	"github.com/rustiever/conduit-connector-marketo/source/config"
	"github.com/rustiever/conduit-connector-marketo/source/iterator"
	"github.com/rustiever/conduit-connector-marketo/source/position"
)

// Source connector
type Source struct {
	sdk.UnimplementedSource
	config   config.SourceConfig
	client   marketoclient.Client
	iterator Iterator
}

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (sdk.Record, error)
	Stop()
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		globalConfig.ClientID: {
			Required:    true,
			Default:     "",
			Description: "The client ID for the Marketo instance.",
		},
		globalConfig.ClientSecret: {
			Required:    true,
			Default:     "",
			Description: "The client secret for the Marketo instance.",
		},
		globalConfig.ClientEndpoint: {
			Required:    true,
			Default:     "",
			Description: "The endpoint for the Marketo instance.",
		},
		config.KeyPollingPeriod: {
			Required:    false,
			Default:     "1m",
			Description: "The polling period CDC mode.",
		},
		config.KeySnapshotInitialDate: {
			Required:    false,
			Default:     "Creation date of the oldest record.",
			Description: "The date from which the snapshot iterator initially starts getting records.",
		},
		config.KeyFields: {
			Required:    false,
			Default:     "id, createdAt, updatedAt, firstName, lastName, email",
			Description: "The fields to be pulled from Marketo",
		},
	}
}

// Configure parses and stores the configurations
// returns an error in case of invalid config
func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Configure").Logger()
	logger.Trace().Msg("Starting Configuring the Source Connector...")

	sourceConfig, err := config.ParseSourceConfig(ctx, cfg)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While parsing the Source Config")
		return fmt.Errorf("couldn't parse the source config: %w", err)
	}
	s.config = sourceConfig
	logger.Trace().Msg("Successfully Configured the Source Connector")
	return err
}

// Open prepare the plugin to start sending records from the given position
func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Open").Logger()
	logger.Trace().Msg("Starting Open the Source Connector...")
	p, err := position.ParseRecordPosition(pos)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While parsing the Position")
		return fmt.Errorf("couldn't parse the position: %w", err)
	}
	logger.Info().Msgf("Requested fields: %s", s.config.Fields)

	config := minimarketo.ClientConfig{
		ID:       s.config.ClientID,
		Secret:   s.config.ClientSecret,
		Endpoint: s.config.ClientEndpoint,
		Debug:    false,
	}
	s.client, err = marketoclient.NewClient(config)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error While Creating the Marketo Client")
		return fmt.Errorf("couldn't create the marketo client: %w", err)
	}
	s.iterator, err = iterator.NewCombinedIterator(ctx, s.config.ClientEndpoint, s.config.PollingPeriod, s.client, p, s.config.Fields, s.config.SnapshotInitialDate)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while create a combined iterator")
		return fmt.Errorf("couldn't create a combined iterator: %w", err)
	}
	logger.Trace().Msg("Successfully Created the Source Connector")
	return nil
}

// Read gets the next record from the Marketo Instance
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Read").Logger()
	logger.Trace().Msg("Starting Read the Source Connector...")

	if !s.iterator.HasNext(ctx) {
		logger.Debug().Msg("No more records to read, sending sdk.ErrorBackoff...")
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while fetching the records")
		return sdk.Record{}, fmt.Errorf("couldn't fetch the records: %w", err)
	}
	return record, nil
}

func (s *Source) Ack(ctx context.Context, pos sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(pos)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(_ context.Context) error {
	if s.iterator != nil {
		s.iterator.Stop()
	}
	return nil
}
