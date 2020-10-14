package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
)

type stCached struct {
	hs   *httpServer   // http server用于处理客户端请求
	opts *options      // 用户在主进程启动时传入的数据
	log  *log.Logger   // 程序日志
	cm   *cacheManager // 业务数据缓存
	raft *raftNodeInfo // 节点raft信息
}

type stCachedContext struct {
	st *stCached
}

func main() {
	// 初始化缓存服务器
	st := &stCached{
		opts: NewOptions(),
		log:  log.New(os.Stderr, "stCached: ", log.Ldate|log.Ltime),
		cm:   NewCacheManager(),
	}
	ctx := &stCachedContext{st}

	var l net.Listener
	var err error

	// 启动监听，接收k-v读取请求
	l, err = net.Listen("tcp", st.opts.httpAddress)
	if err != nil {
		st.log.Fatal(fmt.Sprintf("listen %s failed: %s", st.opts.httpAddress, err))
	}
	st.log.Printf("http server listen:%s", l.Addr())

	logger := log.New(os.Stderr, "httpserver: ", log.Ldate|log.Ltime)
	httpServer := NewHttpServer(ctx, logger)
	st.hs = httpServer
	// 起一个协程不断的监听http请求
	go func() {
		http.Serve(l, httpServer.mux)
	}()

	// 初始化raft
	raft, err := newRaftNode(st.opts, ctx)
	if err != nil {
		st.log.Fatal(fmt.Sprintf("new raft node failed:%v", err))
	}
	st.raft = raft

	// 尝试根据输入的地址加入到raft集群中
	if st.opts.joinAddress != "" {
		err = joinRaftCluster(st.opts)
		if err != nil {
			st.log.Fatal(fmt.Sprintf("join raft cluster failed:%v", err))
		}
	}

	// monitor leadership
	// 检测当前raft角色，如果为leader，则将本节点置为可写
	for {
		select {
		case leader := <-st.raft.leaderNotifyCh:
			if leader {
				st.log.Println("become leader, enable write api")
				st.hs.setWriteFlag(true)
			} else {
				st.log.Println("become follower, close write api")
				st.hs.setWriteFlag(false)
			}
		}
	}
}
