package main

import (
	"github.com/hashicorp/raft"
)

type snapshot struct {
	cm *cacheManager
}

// Persist saves the FSM snapshot out to the given sink.
// 生成一个快照结构
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	snapshotBytes, err := s.cm.Marshal()
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(snapshotBytes); err != nil {
		sink.Cancel()
		return err
	}

	if err := sink.Close(); err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

// 快照清理
func (f *snapshot) Release() {}
