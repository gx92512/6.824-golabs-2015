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


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
    Key string
    Value string
    Op string
    Config Config
    Cseq int64
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
    logs       []Op
    done       map[int64]bool
    data       map[string]string
    config     shardmaster.Config
}

func (kv *ShardKV) waitPaxos(seq int) Op{
    to := 10 * time.Millisecond
    for {
        status, v := kv.px.Status(seq)
        if status == paxos.Decided{
            value := v.(Op)
            return value
        }
        time.Sleep(to)
        if to < 10 * time.Second {
            to*= 2
        }
    }
}

func (kv *ShardKV) SendShard(servers []string, config Config, shard int, data map[string]string){
    for {
        for _,server := range servers {
            args := &ShardArgs{Shard: shard, Data: data, Config: config}
            var reply ShardReply
            ok := call(server, "ShardKV.ReceiveShard", args, &reply)
            if ok && reply.Err == OK{
                return
            }
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func (kv *ShardKV) ReceiveShard(args *ShardArgs, reply *ShardReply){
    kv.mu.Lock()
    logete := Op{Op: "ReceiveShard", Data: args.Data, Shard: args.Shard, Config: args.Config}
    kv.Process(logete)
    kv.mu.Unlock()
}

func (kv *ShardKV) RecvData(data map[string][string]){
    for k,v := range data{
        kv.data[k] = v
    }
}

func (kv *ShardKV) Insert(value Op) {
    switch value.Op {
    case "Put":
        kv.data[value.Key] = value.Value
    case "Append":
        kv.data[value.Key] += value.Value
    case "Reconfig":
        kv.needShard = make(map[int]bool)
        for shard := 0; shard < NShards; shard++{
            if kv.config.Shards[shard] == kv.gid && value.Config.Shards[shard] != kv.gid {
                data := make(map[string][string])
                for k,v := range kv.data{
                    if key2shard[k] == shard{
                        data[k] = v
                    }
                }
                kv.SendShard(value.Config.Groups[value.Config.Shards[shard]], value.Config, shard, data)
            }else if kv.config.Shards[shard] != kv.gid && value.Config.Shards[shard] == kv.gid {
                kv.needShard = true
            }
        }
        kv.config = value.Config
    case "ReceiveShard":
        kv.RecvData(value.Data)
        kv.needShard = false
    }
}

func (kv *ShardKV) Process(logete Op) {
    finish := false
    var value Op
    for !finish {
        seq := kv.lastSeq
        status, val := kv.px.Status(seq)
        if status == paxos.Decided{
            value = val.(Op)
        }else {
            //fmt.Printf("in Process %d\n", seq)
            kv.px.Start(seq, logete)
            value = kv.waitPaxos(seq)
        }
        finish = logete.Cseq == value.Cseq
        kv.Insert(value)
        kv.px.Done(kv.lastSeq)
        //fmt.Printf("call Done %d  %d\n", kv.lastSeq, kv.me)
        kv.lastSeq += 1
    }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
    kv.mu.Lock()
    _,ok := kv.done[args.Cseq]
    if ok {
        reply.Err = OK
    } else {
        if kv.config.Shards[args.Shard]{
            reply.Err = ErrWrongGroup
        }else{
            logete := Op{Key: args.Key, Value: args.Value, Op: args.Op, Cseq: args.Cseq}
            kv.Process(logete)
        }
    }
    kv.mu.Unlock()
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
    kv.mu.Lock()
    nconfig := kv.sm.Query(kv.config.Num + 1)
    if nconfig.Num == kv.config.Num + 1 {
        logete := Op{Op: "Reconfig", Config: nconfig}
        kv.Process(logete)
    }
    kv.mu.Unlock()
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
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

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
