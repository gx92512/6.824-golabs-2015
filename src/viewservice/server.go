package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
    stime map[string]time.Time
    cview View
    nview View
    acked bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
    vs.stime[args.Me] = time.Now()
    //fmt.Printf("viewsever %s  %s  %s  %d  %d\n",  vs.cview.Primary, vs.cview.Backup, args.Me, args.Viewnum, vs.cview.Viewnum)
    //fmt.Println(vs.acked)
    if vs.cview.Viewnum == 0{
        vs.cview.Viewnum = 1
        vs.cview.Primary = args.Me
        vs.cview.Backup = ""
        vs.acked = false
        reply.View = vs.cview
    }else{
        if args.Me == vs.cview.Primary{
            if args.Viewnum == vs.cview.Viewnum{
                vs.acked = true
            }else if args.Viewnum == 0{
                vs.cview.Primary = vs.cview.Backup
                vs.cview.Backup = ""
                for k, _ := range vs.stime{
                    if k != vs.cview.Primary && k != vs.cview.Backup{
                        vs.cview.Backup = k
                    }
                }
                vs.acked = false
                vs.cview.Viewnum += 1
            }
            //vs.cview = vs.nview
        }
        if args.Me != vs.cview.Primary && vs.cview.Backup == ""{
            if vs.acked{
                vs.cview.Backup = args.Me
                vs.cview.Primary = vs.cview.Primary
                vs.cview.Viewnum = vs.cview.Viewnum + 1
                vs.acked = false
            }
        }
        reply.View = vs.cview
    }

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
    reply.View = vs.cview

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
    t := time.Now()
    s := ""
    flag := false
    for k, v := range vs.stime{
        if t.Sub(v) > DeadPings * PingInterval{
            if k == vs.cview.Primary && vs.acked{
                //fmt.Printf("in tick: %s\n", vs.cview.Primary)
                vs.cview.Primary = vs.cview.Backup
                vs.cview.Backup = ""
                flag = true
                vs.acked = false
                //fmt.Printf("in tick: %s\n", vs.cview.Primary)
                vs.cview.Viewnum += 1
            }else if k == vs.cview.Backup && vs.acked{
                vs.cview.Backup = ""
                flag = true
                vs.acked = false
                vs.cview.Viewnum += 1
            }
            delete(vs.stime, k)
        }else{
            if k != vs.cview.Primary && k != vs.cview.Backup{
                s = k
            }
        }
    }
    if vs.cview.Backup == "" && flag{
        vs.cview.Backup = s
    }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
    vs.stime = make(map[string]time.Time)
    vs.acked = false
    vs.cview.Viewnum = 0
    vs.cview.Primary = ""
    vs.cview.Backup = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
