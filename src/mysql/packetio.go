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

package mysql

import (
	"bufio"
	"fmt"
	"io"
	"net"
)

const (
	defaultReaderSize = 8 * 1024
)

// 在普通io的基础上增加了协议相关的元素
type PacketIO struct {
	rb       *bufio.Reader
	wb       io.Writer

	Sequence uint8
}

// 这里conn是面向Client的?
func NewPacketIO(conn net.Conn) *PacketIO {
	p := new(PacketIO)

	// 为数据读取添加了8k的buffer
	p.rb = bufio.NewReaderSize(conn, defaultReaderSize)
	p.wb = conn

	p.Sequence = 0

	return p
}

func (p *PacketIO) ReadPacket() ([]byte, error) {
	// 如何读取Packet呢?
	header := []byte{0, 0, 0, 0}

	// 先读取header
	if _, err := io.ReadFull(p.rb, header); err != nil {
		return nil, ErrBadConn
	}

	// 得到length, sequence
	length := int(uint32(header[0]) | uint32(header[1]) << 8 | uint32(header[2]) << 16)
	if length < 1 {
		return nil, fmt.Errorf("invalid payload length %d", length)
	}

	sequence := uint8(header[3])

	// seqence必须连贯, 模式是一问一答
	if sequence != p.Sequence {
		return nil, fmt.Errorf("invalid sequence %d != %d", sequence, p.Sequence)
	}

	p.Sequence++

	// 读取数据
	data := make([]byte, length)
	if _, err := io.ReadFull(p.rb, data); err != nil {
		return nil, ErrBadConn
	} else {
		// 数据是否过长, 如果太长，则需要读取后面的Packet
		if length < MaxPayloadLen {
			return data, nil
		}

		// TODO: 这里的实现有点点不太好，通过递归来做，可能存在一点点性能问题
		var buf []byte
		buf, err = p.ReadPacket()
		if err != nil {
			return nil, ErrBadConn
		} else {
			return append(data, buf...), nil
		}
	}
}

//data already have header
func (p *PacketIO) WritePacket(data []byte) error {
	length := len(data) - 4

	// data的前4个字节是空出来的
	// 16M - 4
	for length >= MaxPayloadLen {
		// Packet格式: <Length, Sequence, Bytes>
		// 其中：Length 最大为: 0xffffff, 也就是: 1 << 24 - 1
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff
		data[3] = p.Sequence

		if n, err := p.wb.Write(data[:4 + MaxPayloadLen]); err != nil {
			// 写失败
			return ErrBadConn
		} else if n != (4 + MaxPayloadLen) {
			// 写失败
			return ErrBadConn
		} else {
			// 写成功了，在切换到下一部分的payload
			p.Sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	// 填写实际的数据(Little Endian格式?)
	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.Sequence

	if n, err := p.wb.Write(data); err != nil {
		return ErrBadConn
	} else if n != len(data) {
		return ErrBadConn
	} else {
		p.Sequence++
		return nil
	}
}

//
// WritePacket 中没有total参数，每一个不同的Packet是单独发送的；而这里所有的Packet都被先保存到: total中
//
func (p *PacketIO) WritePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	if data == nil {
		// only flush the buffer
		if direct == true {
			n, err := p.wb.Write(total)

			// 发送错误
			if err != nil {
				return nil, ErrBadConn
			}
			if n != len(total) {
				return nil, ErrBadConn
			}
		}
		// 发送OK
		return total, nil
	}

	length := len(data) - 4
	for length >= MaxPayloadLen {

		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.Sequence
		// data的数据被拷贝到total中
		total = append(total, data[:4 + MaxPayloadLen]...)

		p.Sequence++
		length -= MaxPayloadLen
		data = data[MaxPayloadLen:]
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.Sequence

	total = append(total, data...)
	p.Sequence++

	// 最终一口气发送total的数据，并且返回发送成功的[]byte, 如果失败，则直接返回BadConn
	if direct {
		if n, err := p.wb.Write(total); err != nil {
			return nil, ErrBadConn
		} else if n != len(total) {
			return nil, ErrBadConn
		}
	}
	return total, nil
}
