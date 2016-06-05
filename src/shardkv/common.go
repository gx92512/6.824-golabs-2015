package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
    ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

    UID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
    UID int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ReceiveArgs struct {
    UID_CFG string  // UID of reconfig action
    ShardNum int    // Shard Id
    Num int         // configuration number s.t. reconfig Num --> Num+1
    Shard map[string]string     // key/value in this shard
    RequestCache map[int64]ReqR // request cache for this shard
}

type ReceiveReply struct {
    Err Err
}
