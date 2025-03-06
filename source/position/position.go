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

package position

import (
	"encoding/json"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

const (
	TypeSnapshot IteratorType = iota
	TypeCDC
)

type IteratorType int

type Position struct {
	Key       string       `json:"Key"`
	CreatedAt time.Time    `json:"CreatedAt"`
	UpdatedAt time.Time    `json:"UpdatedAt"`
	Type      IteratorType `json:"Type"`
}

func (p Position) ToRecordPosition() (opencdc.Position, error) {
	return json.Marshal(p)
}

func ParseRecordPosition(p opencdc.Position) (Position, error) {
	if p == nil {
		// empty Position would have the fields with their default values
		return Position{}, nil
	}
	var pos Position
	err := json.Unmarshal(p, &pos)
	if err != nil {
		return Position{}, err
	}
	return pos, nil
}

func ConvertToCDCPosition(p opencdc.Position) (opencdc.Position, error) {
	cdcPos, err := ParseRecordPosition(p)
	if err != nil {
		return opencdc.Position{}, err
	}
	cdcPos.Type = TypeCDC
	return cdcPos.ToRecordPosition()
}
