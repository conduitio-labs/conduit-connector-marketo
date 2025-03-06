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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

func Test_ParseRecordPosition(t *testing.T) {
	positionTests := []struct {
		name    string
		wantErr bool
		in      opencdc.Position
		out     Position
	}{
		{
			name:    "Nil Position return empty Position with default values",
			wantErr: false,
			in:      nil,
			out:     Position{},
		},
		{
			name:    "Empty Postion",
			wantErr: true,
			in:      opencdc.Position{},
			out:     Position{},
		},
		{
			name:    "Malformed Position",
			wantErr: true,
			in:      []byte("s_1_1_1_1"),
			out:     Position{},
		},
		{
			name:    "cdc type position",
			wantErr: false,
			in:      []byte("{\"key\":\"test\",\"createdAt\":\"2020-01-01T04:12:27Z\",\"updatedAt\":\"2020-01-01T04:12:27Z\",\"type\":1}"),
			out: Position{
				Key:       "test",
				Type:      TypeCDC,
				UpdatedAt: time.Date(2020, 1, 1, 4, 12, 27, 0, time.UTC),
				CreatedAt: time.Date(2020, 1, 1, 4, 12, 27, 0, time.UTC),
			},
		},
		{
			name:    "invalid timestamp returns error",
			wantErr: true,
			in:      []byte("{\"key\":\"test\",\"createdAt\":\"0012-01-01T0:05:10Z\",\"updatedAt\":\"2022-05-01T011:0:00Z\",\"type\":1}"),
			out:     Position{},
		},
		{
			name:    "invalid prefix character",
			wantErr: true,
			in:      []byte("z_key"),
			out:     Position{},
		},
	}
	for _, tt := range positionTests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ParseRecordPosition(tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRecordPosition error = %v , wantErr = %v", err, tt.wantErr)
			} else if p != tt.out {
				t.Errorf("ParseRecordPosition(): Got : %+v,Expected : %+v", p, tt.out)
			}
		})
	}
}
