// Copyright (c) 2023 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package node

import (
	"context"
	"net/url"

	"github.com/minio/minio-go/v7"
)

// Node represents an endpoint to S3 object store
type Node struct {
	EndpointURL *url.URL
	Client      *minio.Client
	HCFn        func() bool
	HCCanceler  context.CancelFunc
}

func (n *Node) IsOffline() bool {
	return n.HCFn()
}

type NodeSlc struct {
	Nodes             []*Node
	Prefixes          []string
	Bucket            string
	VersioningEnabled bool
}
