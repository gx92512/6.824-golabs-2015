package paxos

const (
    OK = "OK"
    Reject = "Reject"
)

type Prepareargs struct {
    Instance  int
    //Pn        num
    Pn_n int
    Pn_m int
}

type Preparereply struct {
    Err       string
    //N_a       num
    V_a       interface{}
    //N_h       num
    N_a_n int
    N_a_m int
    N_h_n int
    N_h_m int
}

type Acceptargs struct {
    Instance  int
    //N_a       num
    N_a_n int
    N_a_m int
    Value     interface{}
}

type Acceptreply struct {
    Err       string
    //N_h       num
    N_h_n int
    N_h_m int
}

type Decideargs struct {
    Instance  int
    V_a       interface{}
    //N_a       num
    N_a_n int
    N_a_m int
    Me int
    Done int
}

type Decidereply struct {
}
