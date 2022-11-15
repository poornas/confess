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
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
)

var (
	version                     = "0.0.0-dev"
	globalContext, globalCancel = context.WithCancel(context.Background())
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
	nodes []*node
	hc    *healthChecker
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
			EnvVar: envPrefix + "_ACCESS_KEY",
			Value:  "",
		},
		cli.StringFlag{
			Name:   "secret-key",
			Usage:  "Specify secret key",
			EnvVar: envPrefix + "_SECRET_KEY",
			Value:  "",
		},
		cli.BoolFlag{
			Name:   "tls",
			Usage:  "Use TLS (HTTPS) for transport",
			EnvVar: envPrefix + "_TLS",
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
}

func monkeyconMain(ctx *cli.Context) {
	checkMain(ctx)
	ctxt, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	// initUI()
	nodeState := configureClients(ctx)
	// this is just to test the healthcheck ping
	//todo: remove this code:begin
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
		case <-ctxt.Done():
			return
		}
	}
	//todo: remove this code:end

}
