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

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/table"
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
	Offline  bool             `json:"offline"`
	data     minio.ObjectInfo `json:"-"`
}

func (r *testResult) toRow() table.Row {
	var errMsg string
	if r.Err != nil {
		errMsg = r.Err.Error()
	}

	return table.Row{
		r.Node,
		r.Path,
		r.Method,
		errMsg,
	}
}

func (r *testResult) String() string {
	var errMsg string
	if r.Err != nil {
		errMsg = r.Err.Error()
	}
	return fmt.Sprintf("%s: %s %s %s %s", r.Node, r.Path, r.Method, r.FuncName, errMsg)
}

type keyMap struct {
	quit  key.Binding
	up    key.Binding
	down  key.Binding
	enter key.Binding
}

func newKeyMap() keyMap {
	return keyMap{
		up: key.NewBinding(
			key.WithKeys("k", "up", "left", "shift+tab"),
			key.WithHelp("↑/k", "Move up"),
		),
		down: key.NewBinding(
			key.WithKeys("j", "down", "right", "tab"),
			key.WithHelp("↓/j", "Move down"),
		),
		enter: key.NewBinding(
			key.WithKeys("enter", " "),
			key.WithHelp("enter/spacebar", ""),
		),
		quit: key.NewBinding(
			key.WithKeys("ctrl+c", "q"),
			key.WithHelp("q", "quit"),
		),
	}
}

type resultsModel struct {
	// metrics *metrics
	rows    []table.Row
	table   table.Model
	help    help.Model
	keymap  keyMap
	nlock   sync.RWMutex
	nodeMap map[string]bool

	startTime time.Time
	numTests  int32
	numFailed int32
	lastOp    string

	quitting bool
}

// type opStats struct {
// 	total    int
// 	failures int
// 	lastNode string
// 	latency  time.Duration
// }

// type metrics struct {
// 	mutex sync.RWMutex
// 	ops   map[string]opStats
// }

// func (m *metrics) Clone() metrics {
// 	ops := make(map[string]opStats, len(m.ops))
// 	for k, v := range m.ops {
// 		ops[k] = v
// 	}
// 	// var failures []testResult
// 	// failures = append(failures, m.Failures...)
// 	return metrics{
// 		numTests:  m.numTests,
// 		numFailed: m.numFailed,
// 		ops:       ops,
// 		lastOp:    m.lastOp,
// 		startTime: m.startTime,
// 	}
// }
func (m *resultsModel) updateModel(msg testResult) {
	m.lastOp = msg.Method
	atomic.AddInt32(&m.numTests, 1)
	if msg.Err != nil {
		atomic.AddInt32(&m.numFailed, 1)
		if len(m.rows) < 1000 {
			m.rows = append(m.rows, msg.toRow())
		}
	}
	m.nlock.Lock() // toggle node online/offline status
	status, ok := m.nodeMap[msg.Node]
	if !ok || status != msg.Offline {
		m.nodeMap[msg.Node] = msg.Offline
	}
	m.nlock.Unlock()
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

func initUI() *resultsModel {
	m := resultsModel{
		keymap: newKeyMap(),
		help:   help.New(),
		// metrics: &metrics{
		// 	startTime: time.Now(),
		// },
		startTime: time.Now(),
		nodeMap:   make(map[string]bool),
	}
	t := table.New(
		table.WithColumns(m.getColumns()),
		table.WithFocused(true),
		table.WithHeight(0),
	)
	t.SetStyles(getTableStyles())
	m.table = t
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

func (m *resultsModel) helpView() string {
	return "\n" + m.help.ShortHelpView([]key.Binding{
		m.keymap.enter,
		m.keymap.down,
		m.keymap.up,
		m.keymap.quit,
	})
}

func (m *resultsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	if m.quitting {
		return m, tea.Quit
	}
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			if m.table.Focused() {
				m.table.Blur()
			} else {
				m.table.Focus()
			}
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		case "enter":
			m.table = table.New(
				table.WithColumns(m.getColumns()),
				table.WithRows(m.rows),
				table.WithFocused(true),
				table.WithHeight(5),
			)
			m.table.SetStyles(getTableStyles())
		default:
		}
	case testResult:
		m.updateModel(msg)
		if msg.Final {
			m.quitting = true
			return m, nil
		}

		m.table.SetRows(m.rows)
		if len(m.rows) > 0 {
			m.table.SetHeight(5)
		}
		m.table.UpdateViewport()
		m.table, cmd = m.table.Update(msg)
		return m, cmd
	}
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func getTableStyles() table.Styles {
	ts := table.DefaultStyles()
	ts.Header = ts.Header.
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	ts.Selected = ts.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("300")).
		Bold(false)
	return ts
}

func (m *resultsModel) getColumns() []table.Column {
	return []table.Column{
		{Title: "Node", Width: 20},
		{Title: "Path", Width: 30},
		{Title: "Op", Width: 4},
		{Title: "Error", Width: 40}}
}

var baseStyle = lipgloss.NewStyle().
	Align(lipgloss.Left).
	BorderForeground(lipgloss.Color("240"))

var whiteStyle = lipgloss.NewStyle().
	Bold(true).
	AlignHorizontal(lipgloss.Left).
	Foreground(lipgloss.Color("#ffffff"))
var subtle = lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#383838"}
var special = lipgloss.AdaptiveColor{Light: "#43BF6D", Dark: "#73F59F"}

var divider = lipgloss.NewStyle().
	SetString("•").
	Padding(0, 1).
	Foreground(subtle).
	String()
var (
	advisory  = lipgloss.NewStyle().Foreground(special).Render
	infoStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.NormalBorder()).
			BorderTop(true).
			BorderForeground(subtle)
)

func (m *resultsModel) View() string {
	var s strings.Builder
	s.WriteString(whiteStyle.Render("confess " + version + "\nCopyright MinIO\nGNU AGPL V3\n"))
	if atomic.LoadInt32(&m.numTests) == 0 {
		s.WriteString("(waiting for data)")
		return s.String()
	}

	if len(m.rows) > 0 {
		block := lipgloss.PlaceHorizontal(80, lipgloss.Center, "Consistency Errors"+divider)
		row := lipgloss.JoinHorizontal(lipgloss.Left, block)

		s.WriteString(row + "\n")
		s.WriteString(baseStyle.Render(m.table.View()))
		s.WriteString(m.helpView())
	}

	s.WriteString(fmt.Sprintf("\n\n%d operations succeeded, %d failed in %s\n", atomic.LoadInt32(&m.numTests), atomic.LoadInt32(&m.numFailed), humanize.RelTime(m.startTime, time.Now(), "", "")))

	return s.String()
}
