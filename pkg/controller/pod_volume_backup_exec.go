package controller

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	velerov1api "github.com/heptio/velero/pkg/apis/velero/v1"
)

type Progress struct {
	PercentDone float64 `json:"percent_done"`
}

func (c *podVolumeBackupController) RunBackupCommand(cmd *exec.Cmd, req *velerov1api.PodVolumeBackup) (string, string, error) {

	var stdoutBuf, stderrBuf, stdoutStatBuf bytes.Buffer
	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()

	var errStdout error
	stdout := io.MultiWriter(os.Stdout, &stdoutBuf, &stdoutStatBuf)
	stderr := io.MultiWriter(os.Stderr, &stderrBuf)

	err := cmd.Start()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		_, errStdout = io.Copy(stdout, stdoutIn)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		var line string
		var progress Progress
		for {
			time.Sleep(10 * time.Second)
			scanner := bufio.NewScanner(&stdoutStatBuf)
			line = ""
			for scanner.Scan() {
				line = (scanner.Text())
			}
			if err := scanner.Err(); err != nil {
				fmt.Fprintln(os.Stderr, "reading standard input:", err)
				break
			}
			stdoutStatBuf.Reset()
			err = json.Unmarshal([]byte(line), &progress)
			if err != nil {
				break
			}
			_, err = c.patchPodVolumeBackup(req, func(r *velerov1api.PodVolumeBackup) {
				r.Status.Progress = fmt.Sprintf("%.2f%%", progress.PercentDone*100)
			})
		}

		wg.Done()
	}()

	_, _ = io.Copy(stderr, stderrIn)
	wg.Wait()

	err = cmd.Wait()

	outStr, errStr := string(stdoutBuf.Bytes()), string(stderrBuf.Bytes())

	return outStr, errStr, err
}
