package main

import (
	"github.com/jaqmol/approx/axmsg"
	"github.com/jaqmol/approx/processorconf"
)

func main() {
	conf := processorconf.NewProcessorConf("approx_fork", []string{"DISTRIBUTE"})
	errMsg := axmsg.Errors{Source: "approx_fork"}

	if len(conf.Outputs) < 2 {
		errMsg.LogFatal(nil, "Fork expects more than 1 output, but got %v", len(conf.Outputs))
	}
	if len(conf.Inputs) != 1 {
		errMsg.LogFatal(nil, "Fork expects exactly 1 input, but got %v", len(conf.Inputs))
	}

	if len(conf.Envs["DISTRIBUTE"]) == 0 {
		errMsg.LogFatal(nil, "Fork expects value for env DISTRIBUTE")
	}

	af := NewApproxFork(conf)
	af.Start()
}
