package polite

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

func maybeBecomeSupervisor() {
	pid, _ := strconv.Atoi(os.Getenv("_PQX_SUP_PID"))
	if pid == 0 {
		return
	}
	log.SetFlags(0)
	awaitParentDeath()
	p, err := os.FindProcess(pid)
	if err != nil {
		// If the parent has already died, this will fail with "no
		// such process".
		//
		// This is fine, we'll just exit.
		_ = true // prevent empty branch error
	}
	if err := p.Signal(syscall.Signal(syscall.SIGQUIT)); err != nil {
		// If the parent has already died, this will fail with "no
		// such process" or "process already finished", both of
		// which is fine.
		//
		// Do nothing.
		_ = true // prevent empty branch error
	}
	os.Exit(0)
}

func awaitParentDeath() {
	_, _ = os.Stdin.Read(make([]byte, 1))
}

func shutThisDownAfterMyDeath(p *os.Process) {
	exe, err := os.Executable()
	if err != nil {
		panic(err)
	}
	sup := exec.Command(exe)
	sup.Env = append(os.Environ(), "_PQX_SUP_PID="+strconv.Itoa(p.Pid))
	sup.Stdout = os.Stdout
	sup.Stderr = os.Stderr

	// set a pipe we never write to as to block the supervisor until we die
	_, err = sup.StdinPipe()
	if err != nil {
		panic(err)
	}
	err = sup.Start()
	if err != nil {
		panic(err)
	}

	go func() {
		// Exiting this function without this reference to sup means
		// sup can become eligible for GC after this exists. This means
		// the stdin pipe will also be collected, ultimately causing
		// sup to think it's parent has died, and it will shutdown
		// postgres and exit.
		_ = sup.Wait()
	}()
}
