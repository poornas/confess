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
	"sort"
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

const bufSize = 1000

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
	m        *Model
	wg       sync.WaitGroup
	lwg      sync.WaitGroup
}

func (n *nodeState) queueTest(op OpSequence) {
	n.testCh <- op
}

var (
	concurrency = 100
)

type Object struct {
	Key       string
	VersionID string
	ETag      string
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

type Model struct {
	rows   []table.Row
	table  table.Model
	help   help.Model
	keymap keyMap

	// track offline nodes
	nlock      sync.RWMutex
	offlineMap map[string]bool

	// stats
	startTime time.Time
	numTests  int32
	numFailed int32
	lastOp    string

	quitting bool
}

func (m *Model) updateModel(msg testResult) {
	m.lastOp = msg.Method
	if !errors.Is(msg.Err, errNodeOffline) {
		atomic.AddInt32(&m.numTests, 1)
		if msg.Err != nil {
			atomic.AddInt32(&m.numFailed, 1)
			if len(m.rows) < bufSize {
				m.rows = append(m.rows, msg.toRow())
			}
		}
	}

	m.nlock.Lock()
	_, ok := m.offlineMap[msg.Node]

	if !ok && msg.Offline {
		m.offlineMap[msg.Node] = msg.Offline
	}
	if !msg.Offline && ok {
		delete(m.offlineMap, msg.Node)
	}
	m.nlock.Unlock()
}

func (m *Model) helpView() string {
	return "\n" + m.help.ShortHelpView([]key.Binding{
		m.keymap.enter,
		m.keymap.down,
		m.keymap.up,
		m.keymap.quit,
	})
}

func (m *Model) summaryMsg() string {
	success := atomic.LoadInt32(&m.numTests) - atomic.LoadInt32(&m.numFailed)
	return fmt.Sprintf("Operations succeeded=%d Operations Failed=%d Duration=%s", success, atomic.LoadInt32(&m.numFailed), humanize.RelTime(m.startTime, time.Now(), "", ""))
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

func initUI() *Model {
	m := Model{
		keymap:     newKeyMap(),
		help:       help.New(),
		startTime:  time.Now(),
		offlineMap: make(map[string]bool),
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
	model := initUI()
	nodeState.m = model
	ui := tea.NewProgram(model, tea.WithAltScreen())
	sendFn := ui.Send
	nodeState.init(globalContext, sendFn)

	go func() {
		e := nodeState.runTests(globalContext)
		fmt.Println("------e----------", e)
		if e != nil && !errors.Is(e, context.Canceled) {
			console.Fatalln(fmt.Errorf("unable to run confess: %w", e))
		}
	}()
	if e := ui.Start(); e != nil {
		globalCancel()
		console.Fatalln("Unable to start confess")
	}
}

func (m *Model) Init() tea.Cmd {
	return nil
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
		Foreground(lipgloss.Color("229")). //cacfca 229,300
		Background(lipgloss.Color("245")).
		Bold(false)
	return ts
}

func (m *Model) getColumns() []table.Column {
	return []table.Column{
		{Title: "Node", Width: 20},
		{Title: "Path", Width: 30},
		{Title: "Op", Width: 4},
		{Title: "Error", Width: 40}}
}

var (
	baseStyle = lipgloss.NewStyle().
			Align(lipgloss.Left).
			BorderForeground(lipgloss.Color("240"))

	whiteStyle = lipgloss.NewStyle().
			Bold(true).
			AlignHorizontal(lipgloss.Left).
			Foreground(lipgloss.Color("#ffffff"))
	subtle    = lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#383838"}
	warnColor = lipgloss.AdaptiveColor{Light: "#bf4364", Dark: "#e31441"}
	//special   = lipgloss.AdaptiveColor{Light: "#43BF6D", Dark: "#73F59F"}
	special = lipgloss.AdaptiveColor{Light: "#22E32F", Dark: "#07E316"}

	divider = lipgloss.NewStyle().
		SetString("•").
		Padding(0, 1).
		Foreground(subtle).
		String()

	advisory = lipgloss.NewStyle().Foreground(special).Render
	warn     = lipgloss.NewStyle().Foreground(warnColor).Render
)

func (m *Model) View() string {
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
	success := atomic.LoadInt32(&m.numTests) - atomic.LoadInt32(&m.numFailed)
	s.WriteString(fmt.Sprintf("\n\n%d %s %d %s %s\n\n", success, advisory("operations succeeded"), atomic.LoadInt32(&m.numFailed), warn("failed")+" in", humanize.RelTime(m.startTime, time.Now(), "", "")))
	m.nlock.RLock()
	nodes := []string{}
	for node := range m.offlineMap {
		nodes = append(nodes, node)
	}
	m.nlock.RUnlock()
	sort.Strings(nodes)
	for _, node := range nodes {
		s.WriteString(divider + whiteStyle.Render(node) + " is " + warn("offline") + "\n")

	}
	return s.String()
}
