//go:build windows

package sq

import (
	"os"
	"syscall"
)

func init() {
	// https://stackoverflow.com/a/69542231
	const ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x4
	var stderrMode uint32
	stderr := syscall.Handle(os.Stderr.Fd())
	syscall.GetConsoleMode(stderr, &stderrMode)
	syscall.MustLoadDLL("kernel32").MustFindProc("SetConsoleMode").Call(uintptr(stderr), uintptr(stderrMode|ENABLE_VIRTUAL_TERMINAL_PROCESSING))
	var stdoutMode uint32
	stdout := syscall.Handle(os.Stdout.Fd())
	syscall.GetConsoleMode(stdout, &stdoutMode)
	syscall.MustLoadDLL("kernel32").MustFindProc("SetConsoleMode").Call(uintptr(stdout), uintptr(stdoutMode|ENABLE_VIRTUAL_TERMINAL_PROCESSING))
}
