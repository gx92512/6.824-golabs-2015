package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "time"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Op string
    Key string
    Value string
    Cseq int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
    logs       []Op
    done       map[int64]bool
    lastSeq    int
}


func (kv *KVPaxos) waitPaxos(seq int) Op{
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


func (kv *KVPaxos) Process(logete Op) {
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
        kv.logs = append(kv.logs, value)
        kv.done[value.Cseq] = true
        kv.px.Done(kv.lastSeq)
        //fmt.Printf("call Done %d  %d\n", kv.lastSeq, kv.me)
        kv.lastSeq += 1
    }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
    kv.mu.Lock()
    _,ok := kv.done[args.Cseq]
    if !ok{
        logete := Op{Op: GET, Key: args.Key, Cseq: args.Cseq}
        kv.Process(logete)
    }
    reply.Err = ErrNoKey
    //fmt.Printf("get k: %s\n", args.Key)
    for _,v := range kv.logs{
        //fmt.Printf("get log %s, %s, %s\n", v.Key, v.Op, v.Value)
        if v.Key == args.Key && v.Op != GET{
            reply.Err = OK
            if v.Op == PUT {
                reply.Value = v.Value
            }else {
                reply.Value += v.Value
            }
            //fmt.Printf("get v: %s\n", reply.Value)
        }
    }
    //fmt.Printf("get %d %s %s\n", kv.me, args.Key, reply.Value)
    kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
    kv.mu.Lock()
    _,ok := kv.done[args.Cseq]
    if ok{
        reply.Err = OK
    }else{
        logete := Op{Op: args.Op, Key: args.Key, Value: args.Value, Cseq: args.Cseq}
        kv.Process(logete)
    }
    kv.mu.Unlock()

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
    kv.logs = []Op{}
    kv.done = make(map[int64]bool)
    kv.lastSeq = 1

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
