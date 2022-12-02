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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
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
	Prefixes []string
	nodes    []*node
	hc       *healthChecker
	cliCtx   *cli.Context
	logCh    chan testResult
	testCh   chan OpSequence
	wg       sync.WaitGroup
}

func (n *nodeState) queueTest(op OpSequence) {
	n.testCh <- op
}

var (
	concurrency = 100
)

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

type testResult struct {
	Method   string           `json:"method"`
	FuncName string           `json:"funcName"`
	Path     string           `json:"path"`
	Node     string           `json:"node"`
	Err      error            `json:"err,omitempty"`
	Latency  time.Duration    `json:"duration"`
	Final    bool             `json:"final"`
	Cleanup  bool             `json:"cleanup"`
	Offline  bool             `json:"offline"`
	data     minio.ObjectInfo `json:"-"`
}

func (r *testResult) ToItem() item {
	var errMsg string
	if r.Err != nil {
		errMsg = r.Err.Error()
	}
	return item{
		title: r.Node,
		desc:  fmt.Sprintf("%s %s %s %s", r.Path, r.Method, r.FuncName, errMsg),
	}
}
func (r *testResult) String() string {
	var errMsg string
	if r.Err != nil {
		errMsg = r.Err.Error()
	}
	return fmt.Sprintf("%s: %s %s %s %s", r.Node, r.Path, r.Method, r.FuncName, errMsg)
}

type resultsModel struct {
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
	numTests  int32
	numFailed int32
	Failures  list.Model
	lastOp    string
}

func (m *metrics) Clone() metrics {
	ops := make(map[string]opStats, len(m.ops))
	for k, v := range m.ops {
		ops[k] = v
	}
	// var failures []testResult
	// failures = append(failures, m.Failures...)
	return metrics{
		numTests:  m.numTests,
		numFailed: m.numFailed,
		ops:       ops,
		Failures:  m.Failures,
		lastOp:    m.lastOp,
		startTime: m.startTime,
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
		m.Failures.InsertItem(len(m.Failures.Items()), msg.ToItem())
		// m.Failures = append(m.Failures, msg)
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

const listHeight = 14
const defaultWidth = 20

var (
	titleStyle        = lipgloss.NewStyle().MarginLeft(2)
	itemStyle         = lipgloss.NewStyle().PaddingLeft(4)
	selectedItemStyle = lipgloss.NewStyle().PaddingLeft(2).Foreground(lipgloss.Color("170"))
	paginationStyle   = list.DefaultStyles().PaginationStyle.PaddingLeft(4)
	helpStyle         = list.DefaultStyles().HelpStyle.PaddingLeft(4).PaddingBottom(1)
	quitTextStyle     = lipgloss.NewStyle().Margin(1, 0, 2, 4)
)

// var titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#008080")).Render

func initUI() *resultsModel {
	items := make([]list.Item, 0)
	m := resultsModel{
		metrics: &metrics{
			ops:       make(map[string]opStats),
			Failures:  list.New(items, list.NewDefaultDelegate(), defaultWidth, listHeight),
			startTime: time.Now(),
		},
	}
	m.metrics.Failures.Title = "----Errors----"
	m.metrics.Failures.SetShowStatusBar(false)
	m.metrics.Failures.SetFilteringEnabled(false)
	m.metrics.Failures.Styles.Title = titleStyle
	m.metrics.Failures.Styles.PaginationStyle = paginationStyle
	m.metrics.Failures.Styles.HelpStyle = helpStyle

	return &m
}

func confessMain(ctx *cli.Context) {
	f, err := tea.LogToFile("confess_debug.log", "debug")
	if err != nil {
		fmt.Println("fatal:", err)
		os.Exit(1)
	}
	defer f.Close()
	checkMain(ctx)
	rand.Seed(time.Now().UnixNano())
	nodeState := configureClients(ctx)
	ui := tea.NewProgram(initUI(), tea.WithAltScreen())
	sendFn := ui.Send
	nodeState.init(globalContext, sendFn)

	go func() {
		e := nodeState.runTests(globalContext)
		if e != nil && !errors.Is(e, context.Canceled) {
			console.Fatalln(fmt.Errorf("unable to run confess: %w", e))
		}
	}()
	if e := ui.Start(); e != nil {
		globalCancel()
		console.Fatalln("Unable to start confess")
	}
}

func (m *resultsModel) Init() tea.Cmd {
	return nil
}

var docStyle = lipgloss.NewStyle().Margin(1, 2)

type item struct {
	title, desc string
}

func (i item) Title() string       { return i.title }
func (i item) Description() string { return i.desc }
func (i item) FilterValue() string { return i.title }

func (m *resultsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
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
	case tea.WindowSizeMsg:
		h, v := docStyle.GetFrameSize()
		m.metrics.Failures.SetSize(msg.Width-h, msg.Height-v)
	case testResult:
		m.metrics.Update(msg)
		if msg.Cleanup {
			m.cleanup = true
		}
		if msg.Final {
			m.quitting = true
			return m, tea.Quit
		}
		m.metrics.Failures.InsertItem(len(m.metrics.Failures.Items()), msg.ToItem())
		return m, nil
	}

	m.metrics.Failures, cmd = m.metrics.Failures.Update(msg)
	return m, cmd

}

var whiteStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(lipgloss.Color("#ffffff"))

func (m *resultsModel) View() string {
	var s strings.Builder
	s.WriteString(whiteStyle.Render("confess " + version + "\nCopyright MinIO\nGNU AGPL V3\n\n"))
	if atomic.LoadInt32(&m.metrics.numTests) == 0 {
		s.WriteString("(waiting for data)")
		return s.String()
	}

	if len(m.metrics.Failures.Items()) > 0 {
		s.WriteString("-----------------------------------------Errors------------------------------------------------------\n")

		s.WriteString(docStyle.Render(m.metrics.Failures.View()))
	}
	s.WriteString(fmt.Sprintf("\n\n%d operations succeeded, %d failed in %s\n", atomic.LoadInt32(&m.metrics.numTests), atomic.LoadInt32(&m.metrics.numFailed), humanize.RelTime(m.metrics.startTime, time.Now(), "", "")))

	return s.String()
}
