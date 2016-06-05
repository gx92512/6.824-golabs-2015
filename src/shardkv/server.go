package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


const (
    GET = "GET"
    PUT = "PUT"
    APPEND = "APPEND"
    START_RECONFIG = "START_RECONFIG"
    END_RECONFIG = "END_RECONFIG"
    SEND_SHARD = "SEND_SHARD"
    RECV_SHARD = "RECV_SHARD"
    HEART_BEAT = "HEART_BEAT"
)

type ReqR struct {      // Request Record in cache
    Key string
    Value string
}

type Op struct {
    // Your definitions here.
    UID int64       // UID of get/put request
    UID_CFG string  // UID of reconfig action
    OpType string

    Key string
    Value string

    ShardNum int
    Num int
    Shard map[string]string
    RequestCache map[int64]ReqR

    Config shardmaster.Config
    NextConfig shardmaster.Config
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
    inReconfig bool
    shards [shardmaster.NShards]int64
    config shardmaster.Config
    nextConfig shardmaster.Config
    insNum int

    kvTable map[string]string
    requestCache map[int64]ReqR
    reconfigCache map[string]bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    // if in the cache, directly reply
    record, exists := kv.requestCache[args.UID]
    if exists {
        reply.Err = OK
        reply.Value = record.Value
        return nil
    }

    // write this op to the paxos log and wait this op is done
    getOp := &Op{}
    getOp.OpType = GET
    getOp.UID = args.UID
    getOp.UID_CFG = ""
    getOp.Key = args.Key
    kv.ExecuteOp(getOp)

    // reply "not ready" if in reconfig
    if kv.inReconfig {
        reply.Err = ErrNotReady
        return nil
    }

    // reply "wrong group" if shard is not here
    if kv.config.Shards[key2shard(args.Key)] != kv.gid {
        reply.Err = ErrWrongGroup
        return nil
    }

    // otherwise the op should be applied
    // a little bit redundant, re-check the cache
    record, exists = kv.requestCache[args.UID]
    if !exists {
        // BUG!
        fmt.Printf("ERROR: G%v S%v get op not applied!! K=%v UID=%v\n", kv.gid,
                    kv.me, args.Key, args.UID)
    }
    value2, exists2 := kv.kvTable[args.Key]
    if exists2 {
        reply.Err = OK
        reply.Value = value2
    } else {
        reply.Err = ErrNoKey
        reply.Value = ""
    }
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    // if in the cache, directly reply
    _, exists := kv.requestCache[args.UID]
    if exists {
        reply.Err = OK
        return nil
    }

    // write this op to the paxos log and wait this op is done
    putOp := &Op{}
    if args.Op == "Put"{
        putOp.OpType = PUT
    }else{
        putOp.OpType = APPEND
    }
    putOp.UID = args.UID
    putOp.UID_CFG = ""
    putOp.Key = args.Key
    putOp.Value = args.Value
    kv.ExecuteOp(putOp)

    // reply "not ready" if in reconfig
    if kv.inReconfig {
        reply.Err = ErrNotReady
        return nil
    }

    // reply "wrong group" if shard is not here
    if kv.config.Shards[key2shard(args.Key)] != kv.gid {
        reply.Err = ErrWrongGroup
        return nil
    }

    // otherwise the op should be applied
    // a little bit redundant, re-check the cache
    _, exists = kv.requestCache[args.UID]
    if !exists {
        // BUG!
        fmt.Printf("ERROR: G%v S%v put op not applied!! K=%v UID=%v\n", kv.gid,
                    kv.me, args.Key, args.UID)
    }
    reply.Err = OK

	return nil
}

func (kv *ShardKV) Receive(args *ReceiveArgs, reply *ReceiveReply) error {

    kv.mu.Lock()
    defer kv.mu.Unlock()

    // if in the cache, directly reply
    _, exists := kv.reconfigCache[args.UID_CFG]
    if exists {
        reply.Err = OK
        return nil
    }

    // write this op to the paxos log and wait this op is done
    recvOp := &Op{}
    recvOp.OpType = RECV_SHARD
    recvOp.UID_CFG = args.UID_CFG
    recvOp.Num = args.Num
    recvOp.ShardNum = args.ShardNum
    recvOp.Shard = args.Shard
    recvOp.RequestCache = args.RequestCache
    kv.ExecuteOp(recvOp)

    // if "not ready"
    if kv.inReconfig == false || kv.config.Num < args.Num {
        reply.Err = ErrNotReady
        return nil
    }

    // re-check the configuration num is correct
    if kv.config.Num != recvOp.Num {
        // BUG!
        fmt.Printf("ERROR: G%v S%v recv op not applied!! Op.Num=%v CFG#=%v\n", kv.gid,
                    kv.me, recvOp.Num, kv.config.Num)
        reply.Err = ErrNotReady
        return nil
    }

    reply.Err = OK

    return nil
}

//
//
//
func (kv *ShardKV) StartReconfig(config shardmaster.Config,
                                 newConfig shardmaster.Config) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    startOp := Op{}
    startOp.OpType = START_RECONFIG
    startOp.UID_CFG = START_RECONFIG + "#" + strconv.Itoa(newConfig.Num)
    startOp.Config = config
    startOp.NextConfig = newConfig

    // this operation is already executed
    _, exists := kv.reconfigCache[startOp.UID_CFG]
    if exists {
        return
    }

    kv.ExecuteOp(&startOp)
}

func (kv *ShardKV) EndReconfig(config shardmaster.Config,
                                 newConfig shardmaster.Config) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    endOp := Op{}
    endOp.OpType = END_RECONFIG
    endOp.UID_CFG = END_RECONFIG + "#" + strconv.Itoa(newConfig.Num)
    endOp.Config = config
    endOp.NextConfig = newConfig

    // this operation is already executed
    _, exists := kv.reconfigCache[endOp.UID_CFG]
    if exists {
        return
    }

    kv.ExecuteOp(&endOp)
}


func (kv *ShardKV) StartSend(config shardmaster.Config,
                             newConfig shardmaster.Config) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    sendOp := &Op{}
    sendOp.OpType = SEND_SHARD
    sendOp.UID_CFG = SEND_SHARD + "#" + strconv.Itoa(newConfig.Num)

    // this operation is already executed
    _, exists := kv.reconfigCache[sendOp.UID_CFG]
    if exists {
        return
    }

    kv.ExecuteOp(sendOp)
}

func (kv *ShardKV) WaitRecv(config shardmaster.Config,
                            newConfig shardmaster.Config) {
    //fmt.Printf("G%v S%v start to wait for recv ops. %v -> %v \n",
    //            kv.gid, kv.me, config.Num, newConfig.Num)

    // check kv.config or kv.shards
    for {
        kv.mu.Lock()

        // special case when recfg 0 -> 1
        if kv.config.Num == 0 {
            kv.mu.Unlock()
            break
        }

        // already advanced to future configs
        if kv.config.Num > config.Num {
            kv.mu.Unlock()
            break
        }

        // gets all shards specifiedin newConfig?
        ok := true
        for i := 0; i < shardmaster.NShards; i++ {
            if newConfig.Shards[i] == kv.gid &&
                kv.shards[i] != kv.gid {
                ok = false
            }
        }
        if ok {
            kv.mu.Unlock()
            break
        }

        beatOp := &Op{}
        beatOp.UID = nrand()
        beatOp.OpType = HEART_BEAT
        kv.ExecuteOp(beatOp)

        kv.mu.Unlock()


        time.Sleep(100 * time.Millisecond)
    }

    //fmt.Printf("G%v S%v finish wait for recv ops. %v -> %v \n",
    //            kv.gid, kv.me, config.Num, newConfig.Num)
}

func (kv *ShardKV) ExecuteOp(op *Op) {
    for {
        kv.px.Start(kv.insNum, *op)

        to := 10 * time.Millisecond
        for {
            decided, res := kv.px.Status(kv.insNum)
            if decided == paxos.Decided{
                kv.insNum += 1
                op2 := res.(Op)
                if op2.UID_CFG == "" {
                    if op2.OpType == GET {
                        kv.ExecuteGet(&op2)
                    }
                    if op2.OpType == PUT {
                        kv.ExecutePut(&op2)
                    }
                    if op2.OpType == APPEND {
                        kv.ExecuteAppend(&op2)
                    }
                    if op2.OpType == HEART_BEAT {
                    }
                    kv.px.Done(kv.insNum - 1)
                    if op.UID_CFG == "" && op.UID == op2.UID {
                        return
                    }
                } else {
                    if op2.OpType == START_RECONFIG {
                        kv.ExecuteStart(&op2)
                    }
                    if op2.OpType == END_RECONFIG {
                        kv.ExecuteEnd(&op2)
                    }
                    if op2.OpType == SEND_SHARD {
                        kv.ExecuteSend(&op2)
                    }
                    if op2.OpType == RECV_SHARD {
                        kv.ExecuteRecv(&op2)
                    }
                    kv.px.Done(kv.insNum - 1)
                    if op.UID_CFG != "" && op.UID_CFG == op2.UID_CFG {
                        return
                    }
                }
                break
            }

            time.Sleep(to)
            if to < 10 * time.Second {
                to *= 2
            }
        }
    }
}

func (kv *ShardKV) ExecuteStart(op *Op) {
    //fmt.Printf("G%v S%v execute start cfg#%v->%v %v %v\n", kv.gid,
    //    kv.me, op.Config.Num, op.NextConfig.Num, op.Config.Shards, op.NextConfig.Shards)

    if op.Config.Num != kv.config.Num {
        fmt.Printf("ERROR: G%v S%v configs do not match!!\n%v\n%v\n", kv.gid,
                    kv.me, kv.config, op.Config)
        return
    }
    kv.inReconfig = true
    kv.nextConfig = op.NextConfig
    kv.shards = kv.config.Shards
    kv.reconfigCache[op.UID_CFG] = true
}

func (kv *ShardKV) ExecuteEnd(op *Op) {
    //fmt.Printf("G%v S%v execute end cfg#%v->%v UID=%v\n", kv.gid,
    //    kv.me, op.Config.Num, op.NextConfig.Num, op.UID_CFG)

    if op.Config.Num != kv.config.Num {
        fmt.Printf("ERROR: G%v S%v configs do not match!!\n%v\n%v\n", kv.gid,
                    kv.me, kv.config, op.Config)
        return
    }
    kv.inReconfig = false
    kv.config = kv.nextConfig
    kv.reconfigCache[op.UID_CFG] = true
}

func (kv *ShardKV) ExecuteRecv(op *Op) {
    //fmt.Printf("G%v S%v execute recv op cfg#%v shard#%v UID=%v", kv.gid,
    //    kv.me, op.Num, op.ShardNum, op.UID_CFG)

    if kv.inReconfig == false || op.Num > kv.config.Num {
        //fmt.Printf("\nERROR: G%v S%v tries to receive when not in reconfig!!\n%v\n%v\n", kv.gid,
        //            kv.me, kv.config, kv.nextConfig)
        //fmt.Printf("\tnot ready\n")
        return
    }

    if op.Num < kv.config.Num {
        //fmt.Printf("\nERROR: G%v S%v config number do not match in recv!!\n%v\n%v\n", kv.gid,
        //            kv.me, op.Num, kv.nextConfig)
        return
    }

    sid := op.ShardNum
    if kv.config.Shards[sid] == kv.nextConfig.Shards[sid] || kv.nextConfig.Shards[sid] != kv.gid {
        fmt.Printf("\nERROR: G%v S%v receives wrong shard # %v!!\n%v\n%v\n", kv.gid,
                    kv.me, sid, kv.config, kv.nextConfig)
        return
    }
    for key, value := range op.Shard {
        kv.kvTable[key] = value
    }
    for uid, record := range op.RequestCache {
        kv.requestCache[uid] = record
    }
    kv.shards[sid] = kv.gid
    kv.reconfigCache[op.UID_CFG] = true
    //fmt.Printf("\t%v %v %v\n", kv.config.Shards, kv.shards, kv.nextConfig.Shards)
}

func (kv *ShardKV) ExecuteSend(op *Op) {
    if kv.inReconfig == false {
        fmt.Printf("ERROR: G%v S%v tries to send when not in reconfig!!\n%v\n%v\n", kv.gid,
                    kv.me, kv.config, kv.nextConfig)
        return
    }
    for i := 0; i < shardmaster.NShards; i++ {
        if kv.config.Shards[i] == kv.gid && kv.nextConfig.Shards[i] != kv.gid {
            recvArgs := &ReceiveArgs{}
            recvArgs.ShardNum = i
            recvArgs.Num = kv.config.Num;
            recvArgs.UID_CFG = RECV_SHARD + "#" + strconv.Itoa(kv.nextConfig.Num) + "#" + strconv.Itoa(i)

            recvArgs.Shard = map[string]string{}
            recvArgs.RequestCache = map[int64]ReqR{}
            for key, value := range kv.kvTable {
                if key2shard(key) == i {
                    recvArgs.Shard[key] = value
                }
            }
            for uid, record := range kv.requestCache {
                if key2shard(record.Key) == i {
                    recvArgs.RequestCache[uid] = record
                }
            }

            toGid := kv.nextConfig.Shards[i]
            servers, exists := kv.nextConfig.Groups[toGid]
            if exists == false {
                fmt.Printf("ERROR: G%v S%v GID %v not in the next config!!\n%v\n%v\n", kv.gid,
                    kv.me, kv.config, kv.nextConfig)
            } else {
                go func(servers []string, args *ReceiveArgs) {
                    for {
                        done := false
                        for _, server := range servers {
                            var recvReply ReceiveReply
                            ok := call(server, "ShardKV.Receive", args, &recvReply)
                            if ok  && recvReply.Err == OK {
                                done = true
                                break
                            }
                        }
                        if done {
                            break
                        }
                        time.Sleep(100 * time.Millisecond)
                    }
                }(servers, recvArgs)
            }

            for key, _ := range recvArgs.Shard {
                delete(kv.kvTable, key)
            }

            for uid, _ := range recvArgs.RequestCache {
                delete(kv.requestCache, uid)
            }
        }
    }
    kv.reconfigCache[op.UID_CFG] = true
}


func (kv *ShardKV) ExecuteGet(op *Op) {
    if kv.inReconfig {
        return
    }
    shardId := key2shard(op.Key)
    if kv.config.Shards[shardId] != kv.gid {
        return
    }

    value, exists := kv.kvTable[op.Key]
    if exists {
        kv.requestCache[op.UID] = ReqR{op.Key, value}
    } else {
        kv.requestCache[op.UID] = ReqR{op.Key, ""}
    }
}

func (kv *ShardKV) ExecutePut(op *Op) {
    if kv.inReconfig {
        return
    }
    shardId := key2shard(op.Key)
    if kv.config.Shards[shardId] != kv.gid {
        return
    }

    kv.kvTable[op.Key] = op.Value
    kv.requestCache[op.UID] = ReqR{op.Key, "@@" + op.Value}
}

func (kv *ShardKV) ExecuteAppend(op *Op) {
    if kv.inReconfig {
        return
    }
    shardId := key2shard(op.Key)
    if kv.config.Shards[shardId] != kv.gid {
        return
    }

    kv.kvTable[op.Key] = kv.kvTable[op.Key] + op.Value
    kv.requestCache[op.UID] = ReqR{op.Key, "@@" + op.Value}
}

func (kv *ShardKV) tick() {
    var config shardmaster.Config
    var newConfig shardmaster.Config

    kv.mu.Lock()
    config = kv.config
    kv.mu.Unlock()

//    if config.Num == 0 {
//        newConfig = kv.sm.Query(-1)
//    } else {
        newConfig = kv.sm.Query(config.Num + 1)
//    }

    if config.Num == newConfig.Num {
        return
    }

    // figure out whether this group is sending shards or receiving shards
    bSend := false
    bRecv := false
    for i := 0; i < shardmaster.NShards; i++ {
        if newConfig.Shards[i] != config.Shards[i] {
            if config.Shards[i] == kv.gid {
                bSend = true
            }
            if newConfig.Shards[i] == kv.gid {
                bRecv = true
            }
        }
    }

    if bSend && bRecv {
        fmt.Printf("ERROR: G%v S%v is both receiving and sending!!!\n",
                    kv.gid, kv.me)
        return
    }

    // start reconfig
    kv.StartReconfig(config, newConfig)

    // this group is sending shards
    if bSend {
        kv.StartSend(config, newConfig)
    }

    // this group is receiving shards
    if bRecv {
        kv.WaitRecv(config, newConfig)
    }

    // end reconfig
    kv.EndReconfig(config, newConfig)
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
    gob.Register(ReqR{})
    gob.Register(shardmaster.Config{})
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

    kv.kvTable = map[string]string{}
    kv.requestCache = map[int64]ReqR{}
    kv.reconfigCache = map[string]bool{}

    rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)


	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
