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
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rustiever/conduit-connector-marketo/source"
	sourceConfig "github.com/rustiever/conduit-connector-marketo/source/config"
	"github.com/rustiever/conduit-connector-marketo/source/position"
)

func TestSource_SuccessfullSnapshot(t *testing.T) {
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	src := newTestSource()
	testLeads, err := client.addLeads(10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := cleanUp(client)
		if err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	defer func() {
		_ = src.Teardown(ctx)
	}()
	err = configAndOpen(ctx, src, nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	for _, lead := range testLeads {
		rec := nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_SnapshotRestart(t *testing.T) {
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	src := newTestSource()
	startTime := time.Now().UTC()
	testLeads, err := client.addLeads(10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := cleanUp(client)
		if err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	defer func() {
		_ = src.Teardown(ctx)
	}()
	pos, err := json.Marshal(position.Position{
		Key:       "1",
		CreatedAt: startTime,
		UpdatedAt: startTime,
		Type:      position.TypeSnapshot,
	})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	err = configAndOpen(ctx, src, pos)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	for _, lead := range testLeads {
		rec := nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_EmptyDatabase(t *testing.T) {
	src := newTestSource()
	ctx := context.Background()
	defer func() {
		_ = src.Teardown(ctx)
	}()
	err := configAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
}

func TestSource_StartCDCAfterEmptyBucket(t *testing.T) {
	ctx := context.Background()
	src := newTestSource()
	defer func() {
		_ = src.Teardown(ctx)
	}()
	err := configAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := cleanUp(client)
		if err != nil {
			t.Error(err)
		}
	})
	testLeads, err := client.addLeads(5)
	if err != nil {
		t.Fatal(err)
	}
	for _, lead := range testLeads {
		rec := nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
}

func TestSource_NonExistentDatabase(t *testing.T) {
	src := newTestSource()
	ctx := context.Background()
	defer func() {
		_ = src.Teardown(ctx)
	}()
	cfg := getConfigs()
	cfg[sourceConfig.SourceConfigClientID] = "non-existent"
	cfg[sourceConfig.SourceConfigClientSecret] = "non-existent"
	err := src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	err = src.Open(ctx, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSource_CDC_ReadRecordsUpdate(t *testing.T) {
	src := newTestSource()
	ctx := context.Background()
	defer func() {
		_ = src.Teardown(ctx)
	}()
	err := configAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	testLeads, err := client.addLeads(1)
	t.Cleanup(func() {
		err := cleanUp(client)
		if err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := src.Read(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	assert(t, &rec, testLeads[0])
	updatedLeads, err := updateLeads(client, testLeads[0]["email"].(string))
	if err != nil {
		t.Fatal(err)
	}
	rec = nextRecord(ctx, src, t)
	var record map[string]interface{}
	err = json.Unmarshal(rec.Payload.After.Bytes(), &record)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if record["email"] != updatedLeads["email"] && record["lastName"] != updatedLeads["lastName"] {
		t.Errorf("expected %v, got %v", updatedLeads, record)
	}
}

func TestCDC_Delete(t *testing.T) {
	ctx := context.Background()
	src := newTestSource()
	defer func() {
		_ = src.Teardown(ctx)
	}()
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	err = configAndOpen(ctx, src, nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	t.Cleanup(func() {
		err := cleanUp(client)
		if err != nil {
			t.Error(err)
		}
	})
	testLeads, err := client.addLeads(1)
	if err != nil {
		t.Fatal(err)
	}
	var rec opencdc.Record
	for _, lead := range testLeads {
		rec = nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	_, err = src.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Errorf("expected error %v, got %v", sdk.ErrBackoffRetry, err)
	}
	var record map[string]interface{}
	err = json.Unmarshal(rec.Payload.After.Bytes(), &record)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	leadID := record["id"].(string)
	err = client.deleteLeadsByIDs([]string{leadID})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	rec = nextRecord(ctx, src, t)
	if string(rec.Key.Bytes()) != leadID {
		t.Errorf("expected %v, got %v", leadID, record["id"])
	}
}

func TestSource_CDC_ReadRecordsInsertAfterTeardown(t *testing.T) {
	src := newTestSource()
	ctx := context.Background()
	err := configAndOpen(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	testLeads, err := client.addLeads(3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := cleanUp(client)
		if err != nil {
			t.Error(err)
		}
	})
	var rec opencdc.Record
	for _, lead := range testLeads {
		rec = nextRecord(ctx, src, t)
		assert(t, &rec, lead)
	}
	lastPosition := rec.Position
	_ = src.Teardown(ctx)
	src1 := newTestSource()
	defer func() {
		_ = src1.Teardown(ctx)
	}()
	err = configAndOpen(ctx, src1, lastPosition)
	if err != nil {
		t.Fatal(err)
	}
	testLeads, err = client.addLeads(1)
	if err != nil {
		t.Fatal(err)
	}
	rec = nextRecord(ctx, src1, t)
	assert(t, &rec, testLeads[0])
}

func TestOpenSource_FailsParsePosition(t *testing.T) {
	ctx := context.Background()
	source := &source.Source{}
	defer func() {
		_ = source.Teardown(ctx)
	}()
	err := source.Configure(ctx, getConfigs())
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, []byte("Invalid Position"))
	expectedErr := "invalid character 'I' looking for beginning of value"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected want error is %q but got %v", expectedErr, err)
	}
}

func TestOpenSource_InvalidPositionType(t *testing.T) {
	ctx := context.Background()
	source := &source.Source{}
	err := source.Configure(ctx, getConfigs())
	if err != nil {
		t.Fatal(err)
	}
	p, err := json.Marshal(position.Position{
		Key:       "key",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Type:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(ctx, p)
	expectedErr := "couldn't create a combined iterator: invalid position type (2)"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected want error is %q but got %v", expectedErr, err)
	}
}

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := &source.Source{}
	err := con.Configure(context.Background(), make(map[string]string))
	if err == nil {
		t.Errorf("expected no error, got %v", err)
	}
	if strings.HasPrefix(err.Error(), "config is invalid:") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}
