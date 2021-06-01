package coward

import (
	"flag"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/go-ps"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	coward    = flag.Bool("c", false, "")
	slaves    = flag.Int("slaves", 0, "")
	Slave     = flag.Int("s", 0, "internal use")
	slaveMark = flag.Int("sm", 0, "internal use")
)

func Init(name string) bool {
	flag.Parse()

	log.SetReportCaller(true)
	log.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   "/var/log/" + name + ".log",
		MaxSize:    100, // megabytes
		MaxBackups: 16,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}))

	p, _ := ps.Processes()
	for _, p := range p {
		if strings.Contains(p.Executable(), name) && os.Getpid() != p.Pid() {
			if *coward {
				log.Info("coward mode, existing server: ", p.Pid(), ", exit quietly")
				return false
			}
			if *Slave == 0 {
				log.Info("terminate old server: ", p.Pid(), exec.Command("kill", "-9", strconv.Itoa(p.Pid())).Run())
				time.Sleep(time.Second)
			}
		}
	}

	if *Slave == 0 {
		rand.Seed(time.Now().Unix())
		mark := rand.Intn(1024)
		for i := 1; i <= *slaves; i++ {
			log.Info("start slave", i, " ==== ", exec.Command(os.Args[0], "-s", strconv.Itoa(i), "-sm", strconv.Itoa(mark)).Start())
		}
	}

	return true
}
