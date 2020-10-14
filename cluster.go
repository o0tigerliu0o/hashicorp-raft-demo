package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	hclog "github.com/hashicorp/go-hclog"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// raft节点信息
type raftNodeInfo struct {
	raft           *raft.Raft  //
	fsm            *FSM
	leaderNotifyCh chan bool
}

/*
	Transport是raft集群内部节点之间的通信渠道，节点之间需要通过这个通道来进行日志同步、leader选举等。
	hashicorp/raft内部提供了两种方式来实现，一种是通过TCPTransport，基于tcp，可以跨机器跨网络通信；
	另一种是InmemTransport，不走网络，在内存里面通过channel来通信。显然一般情况下都使用TCPTransport即可，
	在stcache里也采用tcp的方式。
*/
func newRaftTransport(opts *options) (*raft.NetworkTransport, error) {
	address, err := net.ResolveTCPAddr("tcp4", opts.raftTCPAddress)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(address.String(), address, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	return transport, nil
}

func newRaftNode(opts *options, ctx *stCachedContext) (*raftNodeInfo, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(opts.raftTCPAddress)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Output:os.Stderr,
		Name:"raft",
		Level:log.Ldate|log.Ltime,
	})
	raftConfig.SnapshotInterval = 20 * time.Second
	raftConfig.SnapshotThreshold = 2
	// 创建一个带缓存的chan赋值给raftConfig.NotifyCh，用于监听当前实例是否发生角色变化
	leaderNotifyCh := make(chan bool, 1)
	raftConfig.NotifyCh = leaderNotifyCh

	// 创建raft节点内部的通信通道，用于调用raft服务，监听raft相关的消息
	transport, err := newRaftTransport(opts)
	if err != nil {
		return nil, err
	}

	// 创建数据存储目录
	if err := os.MkdirAll(opts.dataDir, 0700); err != nil {
		return nil, err
	}

	// 创建每个节点的状态机，用于维护及创建数据快照
	fsm := &FSM{
		ctx: ctx,
		log: log.New(os.Stderr, "FSM: ", log.Ldate|log.Ltime),
	}

	// SnapshotStore用来存储快照信息，当前使用文件持久化存储（FileSnapshotStore）
	snapshotStore, err := raft.NewFileSnapshotStore(opts.dataDir, 1, os.Stderr)
	if err != nil {
		return nil, err
	}
	// 初始化raft log存储
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.dataDir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}
	// 初始化节点状态信息存储
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.dataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}

	// 创建raft节点
	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	// 初始化raft集群
	// 集群最开始的时候只有一个节点，我们让第一个节点通过bootstrap的方式启动，它启动后成为leader
	if opts.bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		// 初始化raft集群
		raftNode.BootstrapCluster(configuration)
	}

	return &raftNodeInfo{raft: raftNode, fsm: fsm, leaderNotifyCh: leaderNotifyCh}, nil
}

// joinRaftCluster joins a node to raft cluster
// 后续的节点启动的时候需要加入集群，启动的时候指定第一个节点的地址，
// 并发送请求加入集群,通过http请求方式申请当前节点加入raft集群
func joinRaftCluster(opts *options) error {
	url := fmt.Sprintf("http://%s/join?peerAddress=%s", opts.joinAddress, opts.raftTCPAddress)
	// get方式调用leader节点的join方法加入集群
	resp, err := http.Get(url)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if string(body) != "ok" {
		return errors.New(fmt.Sprintf("Error joining cluster: %s", body))
	}

	return nil
}
