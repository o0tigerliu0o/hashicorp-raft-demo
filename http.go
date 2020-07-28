package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

const (
	ENABLE_WRITE_TRUE  = int32(1)
	ENABLE_WRITE_FALSE = int32(0)
)

type httpServer struct {
	ctx         *stCachedContext
	log         *log.Logger
	mux         *http.ServeMux
	enableWrite int32
}

// http服务声明
func NewHttpServer(ctx *stCachedContext, log *log.Logger) *httpServer {
	mux := http.NewServeMux()
	s := &httpServer{
		ctx:         ctx,
		log:         log,
		mux:         mux,
		enableWrite: ENABLE_WRITE_FALSE,
	}

	// http请求转发配置
	mux.HandleFunc("/set", s.doSet)
	mux.HandleFunc("/get", s.doGet)
	mux.HandleFunc("/join", s.doJoin)
	return s
}

// 写入stcache前验证 写权限
func (h *httpServer) checkWritePermission() bool {
	return atomic.LoadInt32(&h.enableWrite) == ENABLE_WRITE_TRUE
}

// 当前实例主线角色转换时，调整对应的写权限
func (h *httpServer) setWriteFlag(flag bool) {
	if flag {
		atomic.StoreInt32(&h.enableWrite, ENABLE_WRITE_TRUE)
	} else {
		atomic.StoreInt32(&h.enableWrite, ENABLE_WRITE_FALSE)
	}
}

// 根据key获取value
func (h *httpServer) doGet(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()
	// 从http请求中获取key对应的输入
	key := vars.Get("key")
	if key == "" {
		h.log.Println("doGet() error, get nil key")
		fmt.Fprint(w, "")
		return
	}

	ret := h.ctx.st.cm.Get(key)
	fmt.Fprintf(w, "%s\n", ret)
}

// doSet saves data to cache, only raft master node provides this api
// 将值存入stcache
/*
	当集群的leader故障后，集群的其他节点能够感知到，并申请成为leader，在各个follower中进行投票，
	最后选取出一个新的leader。leader选举是属于raft协议的内容，不需要应用程序操心，但是对有些场景
	而言，应用程序需要感知leader状态，比如对stcache而言，理论上只有leader才能处理set请求来写数
	据，follower应该只能处理get请求查询数据。为了模拟说明这个情况，我们在stcache里面我们设置一个
	写标志位，当本节点是leader的时候标识位置true，可以处理set请求，否则标识位为false，不能处理
	set请求。
*/
func (h *httpServer) doSet(w http.ResponseWriter, r *http.Request) {
	// 验证是不是有写权限
	if !h.checkWritePermission() {
		fmt.Fprint(w, "write method not allowed\n")
		return
	}
	vars := r.URL.Query()
	// 从http请求中获取key、value对应的输入
	key := vars.Get("key")
	value := vars.Get("value")
	if key == "" || value == "" {
		h.log.Println("doSet() error, get nil key or nil value")
		fmt.Fprint(w, "param error\n")
		return
	}

	// 构造event
	event := logEntryData{Key: key, Value: value}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		h.log.Printf("json.Marshal failed, err:%v", err)
		fmt.Fprint(w, "internal error\n")
		return
	}

	// 将event存入stcache
	applyFuture := h.ctx.st.raft.raft.Apply(eventBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		h.log.Printf("raft.Apply failed:%v", err)
		fmt.Fprint(w, "internal error\n")
		return
	}

	fmt.Fprintf(w, "ok\n")
}

// doJoin handles joining cluster request
// 将当前节点加入集群
// 先启动的节点收到请求后，获取对方的地址（指raft集群内部通信的tcp地址），
// 然后调用AddVoter把这个节点加入到集群即可。申请加入的节点会进入follower状态，
// 这以后集群节点之间就可以正常通信，leader也会把数据同步给follower。
func (h *httpServer) doJoin(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	peerAddress := vars.Get("peerAddress")
	if peerAddress == "" {
		h.log.Println("invalid PeerAddress")
		fmt.Fprint(w, "invalid peerAddress\n")
		return
	}
	// 调用raft AddVoter函数将节点加入集群
	addPeerFuture := h.ctx.st.raft.raft.AddVoter(raft.ServerID(peerAddress), raft.ServerAddress(peerAddress), 0, 0)
	if err := addPeerFuture.Error(); err != nil {
		h.log.Printf("Error joining peer to raft, peeraddress:%s, err:%v, code:%d", peerAddress, err, http.StatusInternalServerError)
		fmt.Fprint(w, "internal error\n")
		return
	}
	fmt.Fprint(w, "ok")
}
