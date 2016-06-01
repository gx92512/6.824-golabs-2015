package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type info struct {
    state      Fate
    //my_n       string
    n_h        num
    n_a        num
    v_a        interface{}
}

type num struct {
    n          int
    m          int
}


type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
    instances  map[int]*info
    nums       map[int]*num
    done       []int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//


func (px *Paxos) Setn(seq int) {
    px.mu.Lock()
    _, ok1 := px.nums[seq]
    if !ok1{
        px.nums[seq] = &num{n : 1, m : px.me}
    }
    v, ok2 := px.instances[seq]
    if ok2{
        if px.nums[seq].n < v.n_h.n{
            px.nums[seq].n = v.n_h.n
        }
    }
    px.nums[seq].n += 1
    px.mu.Unlock()
}

func (px *Paxos) Proposer(seq int, v interface{}) {
    for {
        px.Setn(seq)
        //fmt.Printf("after Setn %d\n", px.nums[seq].n)
        ok, value, max_a := px.Prepare(seq, v)
        if ok{
            //fmt.Println(11111)
            ok2, max_a2 := px.Accept(seq, px.nums[seq], value)
            if ok2{
                //fmt.Println(22222)
                px.Decide(seq, *px.nums[seq], value)
            }else{
                px.nums[seq].n = max_a2
            }
        }else{
            px.nums[seq].n = max_a
        }
        state, _ := px.Status(seq)
        //fmt.Println(state)
        if state == Decided {
            break
        }
    }
}


func (px *Paxos) Prepare(seq int, v interface{}) (bool, interface{}, int){
    args := &Prepareargs{}
    args.Instance = seq
    //args.Pn = px.nums[seq]
    args.Pn_n = px.nums[seq].n
    args.Pn_m = px.nums[seq].m
    sum := 0
    max_a := px.nums[seq].n
    max_n_a := &num{}
    value := v
    for index, peer := range px.peers{
        //fmt.Printf("Prepare to %s\n", peer)
        var reply Preparereply
        if index == px.me{
            px.Preparereq(args, &reply)
        }else{
            call(peer, "Paxos.Preparereq", args, &reply)
        }
        if reply.Err == OK{
            sum += 1
            //if Ismax(reply.N_a, max_n_a){
            if reply.N_a_n > max_n_a.n || (reply.N_a_n == max_n_a.n && reply.N_a_m > max_n_a.m){
                max_n_a.n = reply.N_a_n
                max_n_a.m = reply.N_a_m
                value = reply.V_a
            }
        }else if reply.Err == Reject{
            if reply.N_h_n > max_a{
                max_a = reply.N_h_n
            }
        }
    }
    return px.IsMajority(sum), value, max_a
}

func (px *Paxos) Accept(seq int, n *num, v interface{}) (bool, int) {
    args := &Acceptargs{}
    args.Instance = seq
    args.N_a_n = n.n
    args.N_a_m = n.m
    args.Value = v
    sum := 0
    max_a := n.n
    for index, peer := range px.peers{
        var reply Acceptreply
        if index == px.me{
            px.Acceptreq(args, &reply)
        }else{
            call(peer, "Paxos.Acceptreq", args, &reply)
        }
        if reply.Err == OK{
            sum += 1
        }else if reply.Err == Reject{
            if reply.N_h_n > max_a{
                max_a = reply.N_h_n
            }
        }
    }
    return px.IsMajority(sum), max_a
}

func (px *Paxos) Decide(seq int, n num, v interface{}) {
    px.mu.Lock()
    px.instances[seq].state = Decided
    px.instances[seq].n_a = n
    px.instances[seq].v_a = v
    px.instances[seq].n_h = n
    px.mu.Unlock()
    args := &Decideargs{}
    args.Instance = seq
    args.V_a = v
    args.N_a_n = n.n
    args.N_a_m = n.m
    args.Me = px.me
    args.Done = px.done[px.me]
    for index, peer := range px.peers{
        if index != px.me{
            var reply Decidereply
            call(peer, "Paxos.Decidereq", args, &reply)
        }
    }
}

func (px *Paxos) IsMajority(sum int) bool {
    return sum >= len(px.peers) / 2 + 1
}

func Ismax(n1 *num, n2 *num) bool {
    if n1.n > n2.n || (n1.n == n2.n && n1.m > n2.m){
        return true
    }else{
        return false
    }
}

func Ismaxoreq(n1 *num, n2 *num) bool {
    if n1.n > n2.n || (n1.n == n2.n && n1.m >= n2.m){
        return true
    }else{
        return false
    }
}

func (px *Paxos) Preparereq(args *Prepareargs, reply *Preparereply) error{
    px.mu.Lock()
    _, ok := px.instances[args.Instance]
    if !ok {
        px.instances[args.Instance] = &info{state:Pending}
    }
    //if Ismax(args.Pn, px.instances[args.Instance].n_h) {
    if args.Pn_n > px.instances[args.Instance].n_h.n || (args.Pn_n == px.instances[args.Instance].n_h.n && args.Pn_m > px.instances[args.Instance].n_h.m){
        px.instances[args.Instance].n_h.n = args.Pn_n
        px.instances[args.Instance].n_h.m = args.Pn_m
        reply.Err = OK
        reply.N_a_m = px.instances[args.Instance].n_a.m
        reply.N_a_n = px.instances[args.Instance].n_a.n
        reply.V_a = px.instances[args.Instance].v_a
    }else {
        reply.Err = Reject
        reply.N_h_m = px.instances[args.Instance].n_h.m
        reply.N_h_n = px.instances[args.Instance].n_h.n
    }
    px.mu.Unlock()
    return nil
}

func (px *Paxos) Acceptreq(args *Acceptargs, reply *Acceptreply) error {
    px.mu.Lock()
    _, ok := px.instances[args.Instance]
    if !ok {
        px.instances[args.Instance] = &info{state:Pending}
    }
    //if Ismaxoreq(args.N_a, px.instances[args.Instance].n_h) {
    if args.N_a_n > px.instances[args.Instance].n_h.n || (args.N_a_n == px.instances[args.Instance].n_h.n && args.N_a_m >= px.instances[args.Instance].n_h.m){
        reply.Err = OK
        px.instances[args.Instance].n_h.n = args.N_a_n
        px.instances[args.Instance].n_h.m = args.N_a_m
        px.instances[args.Instance].n_a.n = args.N_a_n
        px.instances[args.Instance].n_a.m = args.N_a_m
        px.instances[args.Instance].v_a = args.Value
    }else {
        reply.Err = Reject
        reply.N_h_m = px.instances[args.Instance].n_h.m
        reply.N_h_n = px.instances[args.Instance].n_h.n
    }
    px.mu.Unlock()
    return nil
}

func (px *Paxos) Decidereq(args *Decideargs, reply *Decidereply) error {
    //fmt.Print("Decidereq %d\n", px.me)
    px.mu.Lock()
    _, ok := px.instances[args.Instance]
    if !ok {
        px.instances[args.Instance] = &info{state:Decided}
    }
    px.instances[args.Instance].state = Decided
    px.instances[args.Instance].n_a.n = args.N_a_n
    px.instances[args.Instance].n_a.m = args.N_a_m
    px.instances[args.Instance].v_a = args.V_a
    px.instances[args.Instance].n_h.n = args.N_a_n
    px.instances[args.Instance].n_h.m = args.N_a_m
    px.done[args.Me] = args.Done
    px.mu.Unlock()
    return nil
}

func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
    //fmt.Printf("start a paxos\n")
    go func() {
        if seq < px.Min(){
            return
        }
        px.Proposer(seq, v)
    }()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
    px.mu.Lock()
    if seq > px.done[px.me]{
        px.done[px.me] = seq
    }
    px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
    px.mu.Lock()
    max := 0
    for k, _ := range px.instances {
        if k > max {
            max = k
        }
    }
    px.mu.Unlock()
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
    px.mu.Lock()
    min := px.done[px.me]
    for _,v := range px.done{
        if v < min{
            min = v
        }
    }
    for k,v := range px.instances{
        if k > min{
            continue
        }
        if v.state != Decided{
            continue
        }
        delete(px.instances, k)
        delete(px.instances, k)
    }
    px.mu.Unlock()
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
    if seq < px.Min(){
        return Forgotten, nil
    }
    px.mu.Lock()
    instance, ok := px.instances[seq]
    if !ok {
        //fmt.Println(333333)
        px.mu.Unlock()
        return Pending, nil
    }
    px.mu.Unlock()
    //fmt.Printf("Status %d  %d\n", px.me, instance.state)
	return instance.state, instance.v_a
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
    px.instances = map[int]*info{}
    px.nums = map[int]*num{}
    px.done = make([]int, len(px.peers))
    for i,_ := range px.done{
        px.done[i] = -1
    }

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
