package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "time"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
    Op string
    Gid int64
    Num int
    Servers []string
    Shard int
    Me int
}

func (sm *ShardMaster) waitPaxos(seq int) Op{
    to := 10 * time.Millisecond
    for {
        status, v := sm.px.Status(seq)
        if status == paxos.Decided {
            value := v.(Op)
            return value
        }
        time.Sleep(to)
        if to < 10 * time.Second{
            to *= 2
        }
    }
}

func (sm *ShardMaster) Dojoin(value Op) {
    length := len(sm.configs)
    lconf := sm.configs[length - 1]
    nconf := Config{Num: length, Shards: lconf.Shards}
    nconf.Groups = make(map[int64][]string)
    for k,v := range lconf.Groups {
        nconf.Groups[k] = v
    }
    nconf.Groups[value.Gid] = value.Servers
    gnum := len(nconf.Groups)
    snum := len(lconf.Shards)
    if gnum > snum{
        gnum = snum
    }
    avg := snum / gnum
    yu := snum % gnum
    //fmt.Printf("Dojoin %d  %d  %d\n", value.Gid, gnum, snum)
    var dict map[int64]int
    dict = make(map[int64]int)
    for i,v := range nconf.Shards{
        _,ok := dict[v]
        if !ok {
            dict[v] = 0
        }
        if dict[v] == avg && yu > 0{
            dict[v] += 1
            yu -= 1
        }else if dict[v] > avg || (dict[v] == avg && yu == 0){
            nconf.Shards[i] = value.Gid
        }else{
            dict[v] += 1
        }
    }
    for i,v := range nconf.Shards{
        if v == 0{
            nconf.Shards[i] = value.Gid
        }
    }
    sm.configs = append(sm.configs, nconf)

    /*
    for k,v := range lconf.Shards{
        fmt.Printf("%d: %d, ", k, v)
    }
    fmt.Printf("\n")
    for k,v := range nconf.Shards{
        fmt.Printf("%d: %d, ", k, v)
    }
    fmt.Printf("\n")
    counts := map[int64]int{}
    for _, g := range nconf.Shards {
        counts[g] += 1
    }
    for k,v := range counts{
        fmt.Printf("%d: %d\n", k, v)
    }
    */

}

func (sm *ShardMaster) Doleave(value Op) {
    //fmt.Printf("Doleave: %d\n", value.Gid)
    length := len(sm.configs)
    lconf := sm.configs[length - 1]
    nconf := Config{Num: length, Shards: lconf.Shards}
    nconf.Groups = make(map[int64][]string)
    for k,v := range lconf.Groups {
        if k != value.Gid{
            nconf.Groups[k] = v
        }
    }
    var dict map[int64]int
    dict = make(map[int64]int)
    for k := range nconf.Groups{
        _,ok := dict[k]
        if !ok {
            dict[k] = 0
        }
    }
    for _,v := range nconf.Shards{
        if v == value.Gid{
            continue
        }
        dict[v] += 1
    }
    for i,v := range nconf.Shards{
        minnum := 100
        mink := int64(0)
        if v == value.Gid{
            for k,va := range dict{
                if va < minnum{
                    minnum = va
                    mink = k
                }
            }
            nconf.Shards[i] = mink
            dict[mink] += 1
        }
    }
    sm.configs = append(sm.configs, nconf)

    /*
    for k,v := range lconf.Shards{
        fmt.Printf("%d: %d, ", k, v)
    }
    fmt.Printf("\n")
    for k,v := range nconf.Shards{
        fmt.Printf("%d: %d, ", k, v)
    }
    fmt.Printf("\n")
    counts := map[int64]int{}
    for _, g := range nconf.Shards {
        counts[g] += 1
    }
    for k,v := range counts{
        fmt.Printf("%d: %d\n", k, v)
    }
    */
}

func (sm *ShardMaster) Domove(value Op) {
    length := len(sm.configs)
    lconf := sm.configs[length - 1]
    nconf := Config{Num: length, Shards: lconf.Shards}
    nconf.Groups = make(map[int64][]string)
    for k,v := range lconf.Groups {
        nconf.Groups[k] = v
    }
    nconf.Shards[value.Shard] = value.Gid
    sm.configs = append(sm.configs, nconf)
}

func (sm *ShardMaster) Doquery(value Op) {
    length := len(sm.configs)
    lconf := sm.configs[length - 1]
    nconf := Config{Num: length, Shards: lconf.Shards}
    nconf.Groups = make(map[int64][]string)
    for k,v := range lconf.Groups {
        nconf.Groups[k] = v
    }
    sm.configs = append(sm.configs, nconf)
}

func (sm *ShardMaster) Insert(value Op) {
    switch value.Op {
    case "Join":
        sm.Dojoin(value)
    case "Leave":
        sm.Doleave(value)
    case "Move":
        sm.Domove(value)
    case "Query":
        sm.Doquery(value)
    }
}

func (sm *ShardMaster) Process(logete Op) {
    finish := false
    var value Op
    for !finish {
        seq := len(sm.configs)
        //fmt.Printf("in process %d\n", seq)
        status, val := sm.px.Status(seq)
        if status != paxos.Decided{
            sm.px.Start(seq, logete)
            value = sm.waitPaxos(seq)
        }else {
            value = val.(Op)
        }
        finish = value.Me == sm.me
        sm.Insert(value)
        sm.px.Done(seq)
    }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
    sm.mu.Lock()
    logete := Op{Op: "Join", Gid: args.GID, Servers: args.Servers, Me: sm.me}
    sm.Process(logete)
    sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
    sm.mu.Lock()
    logete := Op{Op: "Leave", Gid: args.GID, Me: sm.me}
    sm.Process(logete)
    sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
    sm.mu.Lock()
    logete := Op{Op: "Move", Gid: args.GID, Shard: args.Shard, Me: sm.me}
    sm.Process(logete)
    sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
    sm.mu.Lock()
    logete := Op{Op: "Query", Num: args.Num, Me: sm.me}
    sm.Process(logete)
    index := args.Num
    if index == -1 || index >= len(sm.configs){
        index = len(sm.configs) - 1
    }
    reply.Config = sm.configs[index]
    sm.mu.Unlock()

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
