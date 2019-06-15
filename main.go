package main

import (
	"github.com/jaqmol/approx/axenvs"
	"github.com/jaqmol/approx/axmsg"
)

func main() {
	envs := axenvs.NewEnvs("approx_fork", []string{"DISTRIBUTE"}, nil)
	errMsg := axmsg.Errors{Source: "approx_fork"}

	if len(envs.Outs) < 2 {
		errMsg.LogFatal(nil, "Fork expects more than 1 output, but got %v", len(envs.Outs))
	}
	if len(envs.Ins) != 1 {
		errMsg.LogFatal(nil, "Fork expects exactly 1 input, but got %v", len(envs.Ins))
	}

	if len(envs.Required["DISTRIBUTE"]) == 0 {
		errMsg.LogFatal(nil, "Fork expects value for env DISTRIBUTE")
	}

	af := NewApproxFork(envs)
	af.Start()
}
