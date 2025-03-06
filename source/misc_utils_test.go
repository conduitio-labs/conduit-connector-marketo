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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SpeakData/minimarketo"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/goombaio/namegenerator"
	"github.com/rustiever/conduit-connector-marketo/source"
	sourceConfig "github.com/rustiever/conduit-connector-marketo/source/config"
)

// actionTypes for createOrUpdate API endpoint.
const (
	CreateOnly = "createOnly"
	UpdateOnly = "updateOnly"
)

var (
	ClinetID       = os.Getenv("MARKETO_CLIENT_ID")
	ClientSecret   = os.Getenv("MARKETO_CLIENT_SECRET")
	ClientEndpoint = os.Getenv("MARKETO_CLIENT_ENDPOINT")
	Seed           = time.Now().UTC().UnixNano()
	Fields         = []string{"firstName", "lastName", "email", "createdAt", "updatedAt"} // fields to be returned by the API
	TestLeads      []string
)

// custom wrapper client for minimarketo client.
type Client struct {
	minimarketo.Client
}

// returns new marketo client with new token.
func newClient(config minimarketo.ClientConfig) (Client, error) {
	client, err := minimarketo.NewClient(config)
	if err != nil {
		return Client{}, err
	}
	return Client{client}, nil
}

// deletes leads be ID from marketo rest api.
func (c Client) deleteLeadsByIDs(ids []string) error {
	path := fmt.Sprintf("/rest/v1/leads/delete.json?id=%s", strings.Join(ids, ","))
	response, err := c.Post(path, nil)
	if err != nil {
		return err
	}
	if !response.Success {
		return fmt.Errorf("%+v", response.Errors)
	}
	return nil
}

// creates or updates leads in marketo rest api.
func (c Client) createOrUpdateLeads(actionType string, leads []map[string]interface{}) error {
	reqBody, err := json.Marshal(map[string]interface{}{
		"action": actionType,
		"input":  leads,
	})
	if err != nil {
		return err
	}
	path := "/rest/v1/leads.json"
	response, err := c.Post(path, reqBody)
	if err != nil {
		return err
	}
	if !response.Success {
		return fmt.Errorf("%+v", response.Errors)
	}
	return nil
}

// returnss nextPageToken from marketo rest api.
func (c Client) getNextPageToken(sinceTime time.Time) (string, error) {
	formattedTime := sinceTime.UTC().Format(time.RFC3339)
	path := fmt.Sprintf("/rest/v1/activities/pagingtoken.json?sinceDatetime=%s", formattedTime)
	response, err := c.Get(path)
	if err != nil {
		return "", err
	}
	if !response.Success {
		return "", fmt.Errorf("%+v", response.Errors)
	}
	return response.NextPageToken, nil
}

// returns updated leads from marketo rest api.
func (c Client) getLeadChanges(nextPageToken string, fields []string) (*minimarketo.Response, error) {
	path := fmt.Sprintf("/rest/v1/activities/leadchanges.json?nextPageToken=%s&fields=%s", nextPageToken, strings.Join(fields, ","))
	response, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	if !response.Success {
		return nil, fmt.Errorf("%+v", response.Errors)
	}
	return response, nil
}

// returns filterd leads from marketo rest api.
func (c Client) filterLeads(fileterType string, filterValues []string) (*json.RawMessage, error) {
	path := fmt.Sprintf("/rest/v1/leads.json?filterType=%s&filterValues=%s", fileterType, strings.Join(filterValues, ","))
	response, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	if !response.Success {
		return nil, fmt.Errorf("%+v", response.Errors)
	}
	return &response.Result, nil
}

// returns Lead record from marketo rest api.
func (c Client) getLeadByID(id int, fields []string) (*json.RawMessage, error) {
	path := fmt.Sprintf("/rest/v1/lead/%d.json?fields=%s", id, strings.Join(fields, ","))
	response, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	if !response.Success {
		return nil, fmt.Errorf("%+v", response.Errors)
	}
	return &response.Result, nil
}

// returns Test source.
func newTestSource() *source.Source {
	return &source.Source{}
}

// returns configs for testing.
func getConfigs() map[string]string {
	cfg := map[string]string{}
	cfg[sourceConfig.SourceConfigClientID] = ClinetID
	cfg[sourceConfig.SourceConfigClientSecret] = ClientSecret
	cfg[sourceConfig.SourceConfigClientEndpoint] = ClientEndpoint
	cfg[sourceConfig.SourceConfigPollingPeriod] = "10s"
	cfg[sourceConfig.SourceConfigSnapshotInitialDate] = time.Now().UTC().Format(time.RFC3339)
	return cfg
}

// returns new client.
func getClient() (Client, error) {
	client, err := newClient(minimarketo.ClientConfig{
		ID:       ClinetID,
		Secret:   ClientSecret,
		Endpoint: ClientEndpoint,
	})
	if err != nil {
		return Client{}, err
	}
	return client, nil
}

// asserts actual record against expected lead.
func assert(t *testing.T, actual *opencdc.Record, expected map[string]interface{}) {
	var record map[string]interface{}
	err := json.Unmarshal(actual.Payload.After.Bytes(), &record)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if expected["firstName"] != record["firstName"] {
		t.Errorf("expected firstName %v, got %v", expected["firstName"], record["firstName"])
	}
	if expected["lastName"] != record["lastName"] {
		t.Errorf("expected lastName %v, got %v", expected["lastName"], record["lastName"])
	}
	if expected["email"] != record["email"] {
		t.Errorf("expected email %v, got %v", expected["email"], record["email"])
	}
}

// generates a n number of leads and adds them to the database, also returns leads.
func (c Client) addLeads(count int) ([]map[string]interface{}, error) {
	startTime := time.Now().UTC()
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)
	var leads []map[string]interface{}
	for i := 0; i < count; i++ {
		firstname := nameGenerator.Generate()
		leads = append(leads, map[string]interface{}{
			"firstName": firstname,
			"lastName":  nameGenerator.Generate(),
			"email":     firstname + "@meroxa.com",
		})
	}
	err := c.createOrUpdateLeads(CreateOnly, leads)
	if err != nil {
		return nil, err
	}
	err = updateTestLeadsSlice(c, startTime)
	if err != nil {
		return nil, fmt.Errorf("error updating test leads slice: %w", err)
	}
	return leads, nil
}

func updateTestLeadsSlice(client Client, startTime time.Time) error {
	token, err := client.getNextPageToken(startTime)
	if err != nil {
		return err
	}
	res, err := client.getLeadChanges(token, Fields)
	if err != nil {
		return err
	}
	if len(res.Result) == 0 {
		return nil
	}
	leadResult := []map[string]interface{}{}
	err = json.Unmarshal(res.Result, &leadResult)
	if err != nil {
		return err
	}
	for _, lead := range leadResult {
		TestLeads = append(TestLeads, fmt.Sprint(lead["leadId"].(float64)))
	}
	return nil
}

// updates the leads for given LeadID.
func updateLeads(client Client, emailID string) (map[string]interface{}, error) {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)
	leads := map[string]interface{}{
		"lastName": nameGenerator.Generate(),
		"email":    emailID,
	}
	err := client.createOrUpdateLeads(UpdateOnly, []map[string]interface{}{leads})
	if err != nil {
		return nil, err
	}
	return leads, nil
}

// gets next record from the source.
func nextRecord(ctx context.Context, src *source.Source, t *testing.T) (rec opencdc.Record) {
	var err error
	for {
		rec, err = src.Read(ctx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			continue
		}
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		break
	}
	return
}

// deletes all test leads from marketo API.
func cleanUp(client Client) error {
	if len(TestLeads) == 0 {
		return nil
	}
	err := client.deleteLeadsByIDs(TestLeads)
	if err != nil {
		return err
	}
	return nil
}

// configures the source with the given configs and establishes a connection to Marketo.
func configAndOpen(ctx context.Context, s *source.Source, pos opencdc.Position) error {
	err := s.Configure(ctx, getConfigs())
	if err != nil {
		return err
	}
	err = s.Open(ctx, pos)
	if err != nil {
		return err
	}
	return nil
}
