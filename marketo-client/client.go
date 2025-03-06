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

package marketoclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SpeakData/minimarketo"
	"github.com/jpillora/backoff"
)

var (
	ErrEnqueueLimit = errors.New("enqueue limit reached")
	ErrZeroRecords  = errors.New("no records found")
	ErrCannotCancel = errors.New("cannot cancel export, since it is already in completed state")
)

// custom wrapper client for minimarketo client.
type Client struct {
	minimarketo.Client
}

// returns new marketo client with new token.
func NewClient(config minimarketo.ClientConfig) (Client, error) {
	client, err := minimarketo.NewClient(config)
	if err != nil {
		return Client{}, err
	}
	return Client{client}, nil
}

// creates New exportLeads job for given time range with requested fields. Maximum time range will be 31 days.
// return export id and error.
func (c Client) CreateExportLeads(fields []string, startDate string, endDate string) (string, error) {
	reqBody, err := json.Marshal(map[string]interface{}{
		"filter": map[string]interface{}{
			"createdAt": map[string]string{
				"startAt": startDate,
				"endAt":   endDate,
			},
		},
		"fields": fields,
	})
	if err != nil {
		return "", err
	}
	path := "/bulk/v1/leads/export/create.json"
	response, err := c.Post(path, reqBody)
	if err != nil {
		return "", err
	}
	if !response.Success {
		return "", fmt.Errorf("%+v", response.Errors)
	}
	var result []CreateExportResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		return "", err
	}
	if len(result) != 1 {
		return "", fmt.Errorf("unexpected response from marketo rest api:%+v", result)
	}
	return result[0].ExportID, nil
}

type CreateExportResult struct {
	ExportID  string    `json:"exportId"`
	Format    string    `json:"format"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

// enqueues export job.
func (c Client) EnqueueExportLeads(exportID string) (string, error) {
	path := fmt.Sprintf("/bulk/v1/leads/export/%s/enqueue.json", exportID)
	response, err := c.Post(path, nil)
	if err != nil {
		return "", err
	}
	if !response.Success {
		if response.Errors[0].Code == "1029" {
			return "", ErrEnqueueLimit
		}
		return "", fmt.Errorf("%+v", response.Errors)
	}
	return exportID, nil
}

// returns current status of export job with error.
func (c Client) StatusOfExportLeads(exportID string) (StatusOfExportResult, error) {
	path := fmt.Sprintf("/bulk/v1/leads/export/%s/status.json", exportID)
	response, err := c.Get(path)
	if err != nil {
		return StatusOfExportResult{}, err
	}
	if !response.Success {
		return StatusOfExportResult{}, fmt.Errorf("%+v", response.Errors)
	}
	var result []StatusOfExportResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		return StatusOfExportResult{}, err
	}
	if len(result) != 1 {
		return StatusOfExportResult{}, fmt.Errorf("unexpected response from marketo rest api:%+v", result)
	}
	return result[0], nil
}

type StatusOfExportResult struct {
	ExportID        string    `json:"exportId"`
	Format          string    `json:"format"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"createdAt"`
	QueuedAt        time.Time `json:"queuedAt"`
	StartedAt       time.Time `json:"startedAt"`
	FinishedAt      time.Time `json:"finishedAt"`
	NumberOfRecords int       `json:"numberOfRecords"`
	FileSize        int       `json:"fileSize"`
	FileChecksum    string    `json:"fileChecksum"`
}

// cancels export job.
func (c Client) CancelExportLeads(exportID string) error {
	path := fmt.Sprintf("/bulk/v1/leads/export/%s/cancel.json", exportID)
	response, err := c.Post(path, nil)
	if err != nil {
		return err
	}
	if !response.Success {
		if response.Errors[0].Code == "1029" {
			return ErrCannotCancel
		}
		return fmt.Errorf("%+v", response.Errors)
	}
	return nil
}

// returns export job result in CSV format.
func (c Client) FileExportLeads(ctx context.Context, endpoint string, exportID string) (*[]byte, error) {
	path := fmt.Sprintf("/bulk/v1/leads/export/%s/file.json", exportID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	token, err := c.GetAuthToken()
	if err != nil {
		return nil, fmt.Errorf("failed to get auth token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer"+token)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get response: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return &body, nil
}

// returns token for marketo rest api.
func (c Client) GetAuthToken() (string, error) {
	if c.GetTokenInfo().Expires.Before(time.Now().UTC()) {
		_, err := c.RefreshToken()
		if err != nil {
			return "", err
		}
	}
	return c.GetTokenInfo().Token, nil
}

// returns all folders from marketo.
func (c Client) GetAllFolders(maxDepth int) ([]FolderResult, error) {
	var folderResult []FolderResult
	path := fmt.Sprintf("/rest/asset/v1/folders.json?maxDepth=%v", maxDepth)
	res, err := c.Get(path)
	if err != nil {
		return []FolderResult{}, err
	}
	err = json.Unmarshal(res.Result, &folderResult)
	if err != nil {
		return []FolderResult{}, err
	}
	return folderResult, nil
}

type FolderResult struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	CreatedAt   string      `json:"createdAt"`
	UpdatedAt   string      `json:"updatedAt"`
	URL         interface{} `json:"url"`
	FolderID    struct {
		ID   int    `json:"id"`
		Type string `json:"type"`
	} `json:"folderId"`
}

// returnss nextPageToken from marketo rest api.
func (c Client) GetNextPageToken(sinceTime time.Time) (string, error) {
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
func (c Client) GetLeadChanges(nextPageToken string, fields []string) (*minimarketo.Response, error) {
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

// returns deleted leads from marketo rest api.
func (c Client) GetDeletedLeads(nextPageToken string) (*json.RawMessage, error) {
	path := fmt.Sprintf("/rest/v1/activities/deletedleads.json?nextPageToken=%s", nextPageToken)
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
func (c Client) GetLeadByID(id int, fields []string) (*json.RawMessage, error) {
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

// creates map from given keys and values.
func GetDataMap(keys []string, values []string) map[string]interface{} {
	dataMap := make(map[string]interface{})
	for i, key := range keys {
		dataMap[key] = values[i]
	}
	return dataMap
}

// retries the function until it returns false or an error.
type RetryFunc func() (bool, error)

// retries supplied function using retry backoff strategy.
func WithRetry(ctx context.Context, r RetryFunc) error {
	b := &backoff.Backoff{
		Max:    2 * time.Minute,
		Min:    10 * time.Second,
		Factor: 1.1,
		// Jitter: true,
	}
	for {
		retry, err := r()
		if err != nil {
			return err
		}
		if retry {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(b.Duration()):
				d := b.Duration()
				if d == b.Max {
					b.Reset()
				}
				continue
			}
		} else if !retry {
			break
		}
	}
	return nil
}

// returns filterd leads from marketo rest api.
func (c Client) FilterLeads(fileterType string, filterValues []int, fields []string, nextPageToken string) (*minimarketo.Response, error) {
	leads := []string{}
	for _, v := range filterValues {
		leads = append(leads, strconv.Itoa(v))
	}
	var path string
	path = fmt.Sprintf("/rest/v1/leads.json?filterType=%s&filterValues=%s&fields=%s", fileterType, strings.Join(leads, ","), strings.Join(fields, ","))
	if nextPageToken != "" {
		path = fmt.Sprintf("/rest/v1/lead/filter.json?filterType=%s&filterValues=%s&fields=%s&nextPageToken=%s", fileterType, strings.Join(leads, ","), strings.Join(fields, ","), nextPageToken)
	}
	response, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	if !response.Success {
		return nil, fmt.Errorf("%+v", response.Errors)
	}
	return response, nil
}
