// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package backend

import (
	"core/errors"
)

// 获取下一个DB
func (n *Node) getNextSlave() (*DB, error) {

	n.RLock()
	defer n.RUnlock()

	if n.validSlaveNum == 0 {
		return nil, errors.ErrNoDatabase
	}
	n.lastSlaveIndex++

	if n.lastSlaveIndex >= n.validSlaveNum {
		n.lastSlaveIndex = 0
	}

	return n.Slave[n.lastSlaveIndex], nil
}
