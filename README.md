# hashicorp-raft-demo
A simple cache server showing how to use hashicorp/raft
## create a cluster
### create leader
hashicorp-raft-demo --http=127.0.0.1:6000 --raft=127.0.0.1:7000 --node=node1 --bootstrap
### create follower
hashicorp-raft-demo --http=127.0.0.1:6001 --raft=127.0.0.1:7001 --node=node2 --join=127.0.0.1:6000  
hashicorp-raft-demo --http=127.0.0.1:6002 --raft=127.0.0.1:7002 --node=node3 --join=127.0.0.1:6000
## refer link:https://cloud.tencent.com/developer/article/1183490

## 文件介绍
cache.go:业务功能  
cluster.go:节点集群构建  
fsm.go:节点数据快照处理  
http.go:业务及节点管理http请求接收与转发  
options.go:bin传入参数解析  
snapshot.go:数据快照生成与释放  