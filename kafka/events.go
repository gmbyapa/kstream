/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package kafka

import (
	"fmt"
)

type Event interface {
	String() string
}

type PartitionEnd struct {
	Tps []TopicPartition
}

func (p *PartitionEnd) String() string {
	return fmt.Sprintf(`%v`, p.Tps)
}

func (p *PartitionEnd) TopicPartitions() []TopicPartition {
	return p.Tps
}

type Error struct {
	Err error
}

func (p *Error) String() string {
	return fmt.Sprint(`consumer error`, p.Err)
}

func (p *Error) Error() string {
	return fmt.Sprint(`consumer error`, p.Err)
}
