// Copyright (c) 2022 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/olekukonko/tablewriter"
)

const bufSize = 100

var (
	version                     = "1.0.0"
	globalContext, globalCancel = context.WithCancel(context.Background())
	globalJSON                  bool
)

const (
	envPrefix = "CONFESS_"
)

// node represents an endpoint to S3 object store
type node struct {
	endpointURL *url.URL
	client      *minio.Client
}

type nodeState struct {
	nodes  []*node
	hc     *healthChecker
	cliCtx *cli.Context
	buf    *objectsBuf
	logCh  chan testResult
}

type ErrLog struct {
}
type Object struct {
	Key       string
	VersionID string
	ETag      string
}
type objectsBuf struct {
	lock     sync.RWMutex
	objects  []Object
	prefixes []string
}

func newObjectsBuf() *objectsBuf {
	var pfxes []string
	for i := 0; i < 10; i++ {
		pfxes = append(pfxes, fmt.Sprintf("prefix%d", i))
	}
	return &objectsBuf{
		objects:  make([]Object, 0, bufSize),
		prefixes: pfxes,
	}
}
func main() {
	app := cli.NewApp()
	app.Name = os.Args[0]
	app.Author = "MinIO, Inc."
	app.Description = `Object store consistency checker`
	app.UsageText = "HOSTS [FLAGS]"
	app.Version = version
	app.Copyright = "(c) 2022 MinIO, Inc."
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "access-key",
			Usage:  "Specify access key",
			EnvVar: envPrefix + "ACCESS_KEY",
			Value:  "",
		},
		cli.StringFlag{
			Name:   "secret-key",
			Usage:  "Specify secret key",
			EnvVar: envPrefix + "SECRET_KEY",
			Value:  "",
		},
		cli.BoolFlag{
			Name:  "insecure",
			Usage: "disable SSL certificate verification",
		},
		cli.StringFlag{
			Name:   "region",
			Usage:  "Specify a custom region",
			EnvVar: envPrefix + "REGION",
		},
		cli.StringFlag{
			Name:   "signature",
			Usage:  "Specify a signature method. Available values are S3V2, S3V4",
			Value:  "S3V4",
			Hidden: true,
		},
		cli.StringFlag{
			Name:  "bucket",
			Usage: "Bucket to use for confess tests",
		},
		cli.StringFlag{
			Name:  "output, o",
			Usage: "Specify output path for confess log",
		},
	}
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Description}}
USAGE:
  {{.Name}} - {{.UsageText}}
HOSTS:
  HOSTS is a comma separated list or a range of hostnames/ip-addresses
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
  1. Run consistency across 4 MinIO Servers (http://minio1:9000 to http://minio4:9000)
     $ confess http://minio{1...4}:9000 --access-key minio --secret-key minio123
`
	app.Action = confessMain
	app.Run(os.Args)
}

func checkMain(ctx *cli.Context) {
	if !ctx.Args().Present() {
		console.Fatalln(fmt.Errorf("not arguments found, please check documentation '%s --help'", ctx.App.Name))
	}
	if ctx.String("bucket") == "" {
		console.Fatalln("--bucket flag needs to be set")
	}
	if !ctx.IsSet("access-key") || !ctx.IsSet("secret-key") {
		console.Fatalln("--access-key and --secret-key flags needs to be set")
	}
}

type resultMsg struct {
	Result testResult `json:"result"`
}

func (m resultMsg) JSON() string {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetIndent("", " ")
	// Disable escaping special chars to display XML tags correctly
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		console.Fatalln(fmt.Errorf("unable to marshal into JSON %w", err))
	}
	return buf.String()
}

type testResult struct {
	Method   string        `json:"method"`
	FuncName string        `json:"funcName"`
	Path     string        `json:"path"`
	Node     string        `json:"node"`
	Err      error         `json:"err,omitempty"`
	Latency  time.Duration `json:"duration"`
	Final    bool          `json:"final"`
	Cleanup  bool          `json:"cleanup"`
}

func (r *testResult) String() string {
	return fmt.Sprintf("%s: %s %s %s %s", r.Node, r.Path, r.Method, r.FuncName, r.Err.Error())
}

type resultsModel struct {
	spinner  spinner.Model
	metrics  *metrics
	quitting bool
	cleanup  bool
}

type opStats struct {
	total    int
	failures int
	lastNode string
	latency  time.Duration
}
type metrics struct {
	startTime time.Time
	mutex     sync.RWMutex
	ops       map[string]opStats
	numTests  int
	numFailed int
	Failures  []testResult
	lastOp    string
}

func (m *metrics) Clone() metrics {
	ops := make(map[string]opStats, len(m.ops))
	for k, v := range m.ops {
		ops[k] = v
	}
	var failures []testResult
	failures = append(failures, m.Failures...)
	return metrics{
		numTests:  m.numTests,
		numFailed: m.numFailed,
		ops:       ops,
		Failures:  failures,
		lastOp:    m.lastOp,
	}
}
func (m *metrics) Update(msg testResult) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.lastOp = msg.Method
	stats, ok := m.ops[msg.Method]
	if !ok {
		stats = opStats{}
	}
	stats.lastNode = msg.Node
	stats.latency = msg.Latency
	if msg.Err != nil {
		stats.failures++
		m.numFailed++
		m.Failures = append(m.Failures, msg)
	}
	stats.total++
	m.ops[msg.Method] = stats
	m.numTests++
}

func getHeader(ctx *cli.Context) string {
	var s strings.Builder
	s.WriteString("confess " + version + " ")
	flags := ctx.GlobalFlagNames()
	for idx, flag := range flags {
		if !ctx.IsSet(flag) {
			continue
		}
		switch {
		case ctx.Bool(flag):
			s.WriteString(fmt.Sprintf("%s=%t", flag, ctx.Bool(flag)))
		case ctx.String(flag) != "":
			val := ctx.String(flag)
			if flag == "secret-key" {
				val = "*REDACTED*"
			}
			s.WriteString(fmt.Sprintf("%s=%s", flag, val))
		}
		if idx != len(flags)-1 {
			s.WriteString(" ")
		}
	}
	s.WriteString("\n")
	return s.String()
}

var titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#008080")).Render

func initUI() *resultsModel {
	s := spinner.New()
	s.Spinner = spinner.Points
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	console.SetColor("duration", color.New(color.FgHiWhite))
	console.SetColor("path", color.New(color.FgGreen))
	console.SetColor("error", color.New(color.FgHiRed))
	console.SetColor("title", color.New(color.FgHiCyan))
	console.SetColor("cleanup", color.New(color.FgHiMagenta))

	console.SetColor("node", color.New(color.FgCyan))

	return &resultsModel{
		spinner: s,
		metrics: &metrics{
			ops:       make(map[string]opStats),
			Failures:  make([]testResult, 0),
			startTime: time.Now(),
		},
	}
}

func confessMain(ctx *cli.Context) {
	checkMain(ctx)
	rand.Seed(time.Now().UnixNano())

	nodeState := configureClients(ctx)
	ui := tea.NewProgram(initUI())
	go func() {
		e := nodeState.runTests(globalContext, func(res testResult) {
			if globalJSON {
				console.Println(resultMsg{Result: res})
				return
			}
			ui.Send(res)
			if res.Err != nil {
				select {
				case nodeState.logCh <- res:
				case <-globalContext.Done():
					return
				}
			}
		})
		if e != nil && !errors.Is(e, context.Canceled) {
			console.Fatalln(fmt.Errorf("unable to run confess: %w", e))
		}

	}()
	go func() {
		logFile := fmt.Sprintf("%s%s", "confess_log", time.Now().Format(".01-02-2006-15-04-05"))
		if ctx.IsSet("output") {
			logFile = fmt.Sprintf("%s/%s", ctx.String("output"), logFile)
		}
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			console.Fatalln("could not create + migration_log.txt", err)
			return
		}
		f.WriteString(getHeader(ctx))
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		for {
			select {
			case <-globalContext.Done():
				return
			case res, ok := <-nodeState.logCh:
				if !ok {
					return
				}
				if _, err := f.WriteString(res.String() + "\n"); err != nil {
					console.Errorln(fmt.Sprintf("Error writing to migration_log.txt for "+res.String(), err))
					os.Exit(1)
				}

			}
		}
	}()
	if e := ui.Start(); e != nil {
		globalCancel()
		console.Fatalln("Unable to start confess")
	}
}

func (m *resultsModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m *resultsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.quitting {
		return m, tea.Quit
	}
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		default:
			return m, nil
		}
	case testResult:
		m.metrics.Update(msg)
		if msg.Cleanup {
			m.cleanup = true
		}
		if msg.Final {
			m.quitting = true
			return m, tea.Quit
		}
		return m, nil
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	var cmd tea.Cmd
	m.spinner, cmd = m.spinner.Update(msg)
	return m, cmd
}

var whiteStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(lipgloss.Color("#ffffff"))

func title(s string) string {
	return titleStyle(s) //console.Colorize("title", s)
}
func opTitle(s string) string {
	return titleStyle(s)
}

var style = lipgloss.NewStyle().
	Bold(true).
	Foreground(lipgloss.Color("#FAFAFA")).Align(lipgloss.Left)

//Background(lipgloss.Color("#7D56F4")).
//	PaddingTop(2).
//	PaddingLeft(4).
//Width(22)

func (m *resultsModel) View() string {
	var s strings.Builder
	s.WriteString(whiteStyle.Render("confess "))
	s.WriteString(whiteStyle.Render(version + "\n"))

	s.WriteString(whiteStyle.Render("Copyright MinIO\n"))
	s.WriteString(whiteStyle.Render("GNU AGPL V3\n\n"))

	if !m.quitting {
		if m.cleanup {
			s.WriteString(fmt.Sprintf("%s %s\n", console.Colorize("title", "confess last operation:"), console.Colorize("cleanup", "cleaning up bucket..")))
		}
		s.WriteString(fmt.Sprintf("%s %s at %s\n", console.Colorize("title", "confess last operation:"), m.metrics.lastOp, m.metrics.ops[m.metrics.lastOp].lastNode))
	}

	// Set table header
	table := tablewriter.NewWriter(&s)
	table.SetAutoWrapText(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(true)
	table.SetRowLine(false)
	var data [][]string

	addLine := func(prefix string, value interface{}) {
		data = append(data, []string{
			prefix,
			fmt.Sprint(value),
		})
	}

	m.metrics.mutex.RLock()
	metrics := m.metrics.Clone()
	m.metrics.mutex.RUnlock()

	if metrics.numTests == 0 {
		s.WriteString("(waiting for data)")
		return s.String()
	}
	addLine(title("Total Operations:"), fmt.Sprintf("%7d ; %s: %7d %s: %7d %s: %7d %s: %7d %s: %7d", metrics.numTests, opTitle("PUT"), metrics.ops[http.MethodPut].total, opTitle("HEAD"), metrics.ops[http.MethodHead].total, opTitle("GET"), metrics.ops[http.MethodGet].total, opTitle("LIST"), metrics.ops["LIST"].total, opTitle("DEL"), metrics.ops[http.MethodDelete].total))
	addLine(title("Total Failures:"), fmt.Sprintf("%7d ; %s: %7d %s: %7d %s: %7d %s: %7d %s: %7d", metrics.numFailed, opTitle("PUT"), metrics.ops[http.MethodPut].failures, opTitle("HEAD"), metrics.ops[http.MethodHead].failures, opTitle("GET"), metrics.ops[http.MethodGet].failures, opTitle("LIST"), metrics.ops["LIST"].failures, opTitle("DEL"), metrics.ops[http.MethodDelete].failures))

	if len(metrics.Failures) > 0 {
		lim := 10
		if len(metrics.Failures) < lim {
			lim = len(metrics.Failures)
		}
		addLine("", "-----------------------------------------Errors------------------------------------------------------")
		for _, s := range metrics.Failures[:lim] {
			addLine(s.Node, console.Colorize("metrics-error", fmt.Sprintf("%s %s %d :%s", s.Method, s.Path, s.Latency, s.Err.Error())))
		}
	}
	table.AppendBulk(data)
	table.Render()

	return s.String()
}
