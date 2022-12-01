package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/minio/console/pkg"
	"github.com/minio/mc/pkg/probe"
	md5simd "github.com/minio/md5-simd"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/ellipses"
	"golang.org/x/net/http2"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var slashSeparator = "/"

// clientTransport returns a new http configuration
// used while communicating with the host.
func clientTransport(ctx *cli.Context, enableTLS bool) *http.Transport {
	// For more details about various values used here refer
	// https://golang.org/pkg/net/http/#Transport documentation
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   1024,
		WriteBufferSize:       32 << 10, // 32KiB moving up from 4KiB default
		ReadBufferSize:        32 << 10, // 32KiB moving up from 4KiB default
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		// Go net/http automatically unzip if content-type is
		// gzip disable this feature, as we are always interested
		// in raw stream.
		DisableCompression: true,
	}
	if enableTLS {
		// Keep TLS config.
		tr.TLSClientConfig = &tls.Config{
			RootCAs:            mustGetSystemCertPool(),
			InsecureSkipVerify: ctx.GlobalBool("insecure"),
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		}
		// Because we create a custom TLSClientConfig, we have to opt-in to HTTP/2.
		// See https://github.com/golang/go/issues/14275
		http2.ConfigureTransport(tr)
	}

	return tr
}

// getClient creates a client with the specified host and the options set in the context.
func getClient(ctx *cli.Context, hostURL *url.URL) (*minio.Client, error) {
	var creds *credentials.Credentials
	switch strings.ToUpper(ctx.String("signature")) {
	case "S3V4":
		// if Signature version '4' use NewV4 directly.
		creds = credentials.NewStaticV4(ctx.String("access-key"), ctx.String("secret-key"), "")
	case "S3V2":
		// if Signature version '2' use NewV2 directly.
		creds = credentials.NewStaticV2(ctx.String("access-key"), ctx.String("secret-key"), "")
	default:
		console.Fatalln(probe.NewError(errors.New("unknown signature method. S3V2 and S3V4 is available")), strings.ToUpper(ctx.String("signature")))
	}
	cl, err := minio.New(hostURL.Host, &minio.Options{
		Creds:        creds,
		Secure:       hostURL.Scheme == "https",
		Region:       ctx.String("region"),
		BucketLookup: minio.BucketLookupAuto,
		CustomMD5:    md5simd.NewServer().NewHash,
		Transport:    clientTransport(ctx, hostURL.Scheme == "https"),
	})
	if err != nil {
		return nil, err
	}
	cl.SetAppInfo("confess", pkg.Version)
	return cl, nil
}

func configureClients(ctx *cli.Context) *nodeState {
	var endpoints []string
	var nodes []*node

	for _, hostStr := range ctx.Args() {
		hosts := strings.Split(hostStr, ",")
		for _, host := range hosts {
			if len(host) == 0 {
				continue
			}
			if !ellipses.HasEllipses(host) {
				endpoints = append(endpoints, host)
				continue
			}
			patterns, perr := ellipses.FindEllipsesPatterns(host)
			if perr != nil {
				console.Fatalln(fmt.Errorf("unable to parse input arg %s: %s", patterns, perr))
			}
			for _, lbls := range patterns.Expand() {
				endpoints = append(endpoints, strings.Join(lbls, ""))
			}
		}
	}

	hcMap := make(map[string]epHealth)
	for _, endpoint := range endpoints {
		endpoint = strings.TrimSuffix(endpoint, slashSeparator)
		target, err := url.Parse(endpoint)
		if err != nil {
			console.Fatalln(fmt.Errorf("unable to parse input arg %s: %s", endpoint, err))
		}
		if target.Scheme == "" {
			target.Scheme = "http"
		}
		if target.Scheme != "http" && target.Scheme != "https" {
			console.Fatalln("unexpected scheme %s, should be http or https, please use '%s --help'",
				endpoint, ctx.App.Name)
		}
		if target.Host == "" {
			console.Fatalln(fmt.Errorf("missing host address %s, please use '%s --help'",
				endpoint, ctx.App.Name))
		}
		clnt, err := getClient(ctx, target)
		if err != nil {
			console.Fatalln(fmt.Errorf("could not initialize client for %s",
				endpoint))
		}
		n := &node{
			endpointURL: target,
			client:      clnt,
		}
		nodes = append(nodes, n)
		hcMap[target.Host] = epHealth{
			Endpoint: target.Host,
			Scheme:   target.Scheme,
			Online:   true,
		}
	}
	return &nodeState{
		nodes:  nodes,
		hc:     newHealthChecker(ctx, hcMap),
		buf:    newObjectsBuf(),
		cliCtx: ctx,
		logCh:  make(chan testResult, 100),
		testCh: make(chan Op, 1000),
	}
}

// mustGetSystemCertPool - return system CAs or empty pool in case of error (or windows)
func mustGetSystemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return x509.NewCertPool()
	}
	return pool
}

// getCertPool - return system CAs or load CA from file if flag specified
func getCertPool(cacert string) *x509.CertPool {
	if cacert == "" {
		return mustGetSystemCertPool()
	}

	pool := x509.NewCertPool()
	caPEM, err := ioutil.ReadFile(cacert)
	if err != nil {
		console.Fatalln(fmt.Errorf("unable to load CA certificate: %s", err))
	}
	ok := pool.AppendCertsFromPEM([]byte(caPEM))
	if !ok {
		console.Fatalln(fmt.Errorf("unable to load CA certificate: %s is not valid certificate", cacert))
	}
	return pool
}

// getCertKeyPair - load client certificate and key pair from file if specified
func getCertKeyPair(cert, key string) []tls.Certificate {
	if cert == "" && key == "" {
		return nil
	}
	if cert == "" || key == "" {
		console.Fatalln(fmt.Errorf("both --cert and --key flags must be specified"))
	}
	certPEM, err := ioutil.ReadFile(cert)
	if err != nil {
		console.Fatalln(fmt.Errorf("unable to load certificate: %s", err))
	}
	keyPEM, err := ioutil.ReadFile(key)
	if err != nil {
		console.Fatalln(fmt.Errorf("unable to load key: %s", err))
	}
	keyPair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		console.Fatalln(fmt.Errorf("%s", err))
	}
	return []tls.Certificate{keyPair}
}
