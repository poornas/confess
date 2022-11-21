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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/progress"
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
	version                     = "0.0.0-dev"
	globalContext, globalCancel = context.WithCancel(context.Background())
	globalJSON                  bool
)

const (
	envPrefix = "MONKEYCON_"
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
	bIdx   uint32
}

type Object struct {
	Key       string
	VersionID string
}
type objectsBuf struct {
	objects []Object
}

func newObjectsBuf() *objectsBuf {
	return &objectsBuf{
		objects: make([]Object, bufSize),
	}
}
func main() {
	app := cli.NewApp()
	app.Name = os.Args[0]
	app.Author = "MinIO, Inc."
	app.Description = `Object store consistency checker`
	app.UsageText = "[SITE]"
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
			Name:   "tls",
			Usage:  "Use TLS (HTTPS) for transport",
			EnvVar: envPrefix + "TLS",
		},
		cli.StringFlag{
			Name:   "region",
			Usage:  "Specify a custom region",
			EnvVar: envPrefix + "_REGION",
		},
		cli.StringFlag{
			Name:   "signature",
			Usage:  "Specify a signature method. Available values are S3V2, S3V4",
			Value:  "S3V4",
			Hidden: true,
		},
		cli.StringFlag{
			Name:  "bucket",
			Usage: "Bucket to use for monkeycon tests. ALL DATA WILL BE DELETED IN BUCKET!",
		},
		cli.DurationFlag{
			Name:  "duration",
			Usage: "Duration to run the tests. Use 's' and 'm' to specify seconds and minutes.",
			Value: 30 * time.Second,
		},
	}
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Description}}
USAGE:
  {{.Name}} - {{.UsageText}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
SITE:
  SITE is a comma separated list of pools of that site: http://172.17.0.{2...5},http://172.17.0.{6...9} or one or more nodes.
EXAMPLES:
  1. Run monkeycon consistency across 4 MinIO Servers (http://minio1:9000 to http://minio4:9000)
     $ monkeycon http://minio{1...4}:9000
`
	app.Action = monkeyconMain
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
		console.Fatalln(fmt.Errorf("Unable to marshal into JSON %w", err))
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
}
type resultsModel struct {
	spinner  spinner.Model
	progress progress.Model
	metrics  *metrics
	duration time.Duration
	quitting bool
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

type tickMsg time.Time

const (
	padding  = 0 // was 2
	maxWidth = 80
)

var helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#626262")).Render
var titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#008080")).Render

func initUI(duration time.Duration) *resultsModel {
	s := spinner.New()
	s.Spinner = spinner.Points
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	console.SetColor("duration", color.New(color.FgHiWhite))
	console.SetColor("path", color.New(color.FgGreen))
	console.SetColor("error", color.New(color.FgHiRed))
	console.SetColor("title", color.New(color.FgHiCyan))
	console.SetColor("node", color.New(color.FgCyan))

	return &resultsModel{
		spinner:  s,
		progress: progress.New(progress.WithDefaultGradient()),
		duration: duration,
		metrics: &metrics{
			ops:       make(map[string]opStats),
			Failures:  make([]testResult, 0),
			startTime: time.Now(),
		},
	}
}
func tickCmd() tea.Cmd {
	return tea.Tick(time.Second*1, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func monkeyconMain(ctx *cli.Context) {
	checkMain(ctx)
	nodeState := configureClients(ctx)
	ui := tea.NewProgram(initUI(ctx.Duration("duration")))
	go func() {
		e := nodeState.runTests(globalContext, func(res testResult) {
			if globalJSON {
				console.Println(resultMsg{Result: res})
				return
			}
			ui.Send(res)
		})
		if e != nil && !errors.Is(e, context.Canceled) {
			console.Fatalln(fmt.Errorf("unable to run monkeycon: %w", e))
		}
	}()
	if e := ui.Start(); e != nil {
		globalCancel()
		console.Fatalln("Unable to start monkeycon")
	}
	// this is just to test the healthcheck ping
	//todo: remove this code:begin
	/*
		hcTimer := time.NewTimer(15 * time.Second)
		defer hcTimer.Stop()
		for {
			select {
			case <-hcTimer.C:
				for _, n := range nodeState.nodes {
					st := "online"
					if nodeState.hc.isOffline(n.endpointURL) {
						st = "offline"
					}
					fmt.Println(n.endpointURL, " is :", st)
				}
				hcTimer.Reset(15 * time.Second)
			case <-globalContext.Done():
				return
			}
		}
	*/
	//todo: remove this code:end

}

func (m *resultsModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, tickCmd())
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
		if msg.Final {
			m.quitting = true
			return m, tea.Quit
		}
		return m, nil
	case tea.WindowSizeMsg:
		m.progress.Width = msg.Width - padding*2 - 4
		if m.progress.Width > maxWidth {
			m.progress.Width = maxWidth
		}
		return m, nil

	case tickMsg:
		if m.progress.Percent() == 1.0 {
			return m, tea.Quit
		}

		// Note that you can also use progress.Model.SetPercent to set the
		// percentage value explicitly, too.
		cmd := m.progress.IncrPercent(float64(time.Since(m.metrics.startTime) / m.duration))
		return m, tea.Batch(tickCmd(), cmd)

	// FrameMsg is sent when the progress bar wants to animate itself
	case progress.FrameMsg:
		progressModel, cmd := m.progress.Update(msg)
		m.progress = progressModel.(progress.Model)
		return m, cmd
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
func (m *resultsModel) View() string {
	var s strings.Builder
	if !m.quitting {
		s.WriteString(fmt.Sprintf("%s %s\n", console.Colorize("title", "Monkeycon activity:"), m.spinner.View()))
		pad := strings.Repeat(" ", padding)
		s.WriteString("\n" +
			pad + m.progress.View() + "\n\n")
	}
	// Set table header
	table := tablewriter.NewWriter(&s)
	table.SetAutoWrapText(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(true)
	table.SetRowLine(false)
	addRow := func(s string) {
		table.Append([]string{s})
	}

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
	addLine(title("Total Operations:"), fmt.Sprintf("%7d ; %s: %7d %s: %7d %s: %7d %s: %7d", metrics.numTests, opTitle("PUT"), metrics.ops[http.MethodPut].total, opTitle("HEAD"), metrics.ops[http.MethodHead].total, opTitle("GET"), metrics.ops[http.MethodGet].total, opTitle("LIST"), metrics.ops["LIST"].total))
	addLine(title("Total Failures:"), fmt.Sprintf("%7d ; %s: %7d %s: %7d %s: %7d %s: %7d", metrics.numFailed, opTitle("PUT"), metrics.ops[http.MethodPut].failures, opTitle("HEAD"), metrics.ops[http.MethodHead].failures, opTitle("GET"), metrics.ops[http.MethodGet].failures, opTitle("LIST"), metrics.ops["LIST"].failures))

	for _, s := range metrics.Failures {
		addRow(console.Colorize("metrics-error", s))
	}
	if len(metrics.Failures) > 0 {
		addLine("", "------------------------------------------- Errors --------------------------------------------------")
		for _, s := range metrics.Failures {
			addLine("", console.Colorize("metrics-error", s))
		}
	}
	table.AppendBulk(data)
	table.Render()

	return s.String()
}
