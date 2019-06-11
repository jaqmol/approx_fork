package main

import (
	"bufio"
	"io"

	"github.com/jaqmol/approx/axmsg"
	"github.com/jaqmol/approx/processorconf"
)

// NewApproxFork ...
func NewApproxFork(conf *processorconf.ProcessorConf) *ApproxFork {
	errMsg := &axmsg.Errors{Source: "approx_fork"}
	distrEnv := conf.Envs["DISTRIBUTE"]
	var distr Distribute
	if "copy" == distrEnv {
		distr = DistributeCopy
	} else if "round_robin" == distrEnv {
		distr = DistributeRoundRobin
	} else {
		errMsg.LogFatal(nil, "Fork expects env DISTRIBUTE to be either copy or round_robin, but got %v", distrEnv)
	}
	return &ApproxFork{
		errMsg:      errMsg,
		conf:        conf,
		outputs:     conf.Outputs,
		input:       conf.Inputs[0],
		distribute:  distr,
		outputIndex: 0,
	}
}

// ApproxFork ...
type ApproxFork struct {
	errMsg      *axmsg.Errors
	conf        *processorconf.ProcessorConf
	outputs     []*bufio.Writer
	input       *bufio.Reader
	distribute  Distribute
	outputIndex int
}

// Distribute ...
type Distribute int

// Distribute Types
const (
	DistributeCopy Distribute = iota
	DistributeRoundRobin
)

// Start ...
func (a *ApproxFork) Start() {
	var hardErr error
	for hardErr == nil {
		var msgBytes []byte
		msgBytes, hardErr = a.input.ReadBytes('\n')
		if hardErr != nil {
			break
		}

		if a.distribute == DistributeCopy {
			a.distributeCopy(msgBytes)
		} else if a.distribute == DistributeRoundRobin {
			a.distributeRoundRobin(msgBytes)
		}
	}

	if hardErr == io.EOF {
		a.errMsg.LogFatal(nil, "Unexpected EOL listening for response input")
	} else {
		a.errMsg.LogFatal(nil, "Unexpected error listening for response input: %v", hardErr.Error())
	}
}

func (a *ApproxFork) distributeCopy(msgBytes []byte) {
	for i := 0; i < len(a.outputs); i++ {
		a.writeToOutput(i, msgBytes)
	}
}

func (a *ApproxFork) distributeRoundRobin(msgBytes []byte) {
	if a.outputIndex >= len(a.outputs) {
		a.outputIndex = 0
	}
	a.writeToOutput(a.outputIndex, msgBytes)
	a.outputIndex++
}

func (a *ApproxFork) writeToOutput(index int, msgBytes []byte) {
	output := a.outputs[index]
	_, err := output.Write(msgBytes)
	if err != nil {
		a.errMsg.Log(nil, "Error writing response to output: %v", err.Error())
		return
	}
	err = output.Flush()
	if err != nil {
		a.errMsg.Log(nil, "Error flushing response to output: %v", err.Error())
		return
	}
}
