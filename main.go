package main

import (
	"github.com/jaqmol/approx/errormsg"
	"github.com/jaqmol/approx/processorconf"
)

func main() {
	conf := processorconf.NewProcessorConf("approx_fork", []string{"DISTRIBUTE"})

	if len(conf.Outputs) < 2 {
		errormsg.LogFatal("approx_fork", nil, -2001, "Fork expects more than 1 output, but got %v", len(conf.Outputs))
	}
	if len(conf.Inputs) != 1 {
		errormsg.LogFatal("approx_fork", nil, -2002, "Fork expects exactly 1 input, but got %v", len(conf.Inputs))
	}

	if len(conf.Envs["DISTRIBUTE"]) == 0 {
		errormsg.LogFatal("approx_fork", nil, -2003, "Fork expects value for env DISTRIBUTE")
	}

	af := NewApproxFork(conf)
	af.Start()
}
