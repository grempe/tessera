// reopen measures what Tessera's AWS driver leaves behind each time a log is
// opened, used and shut down in the same process.
//
// It needs a MySQL server, on which it creates two databases, and an S3 or
// S3-compatible bucket. AWS credentials come from the usual environment
// variables or config files. For example, from the repository's root:
//
//	go run ./repro/reopen -mysql_dsn='root:root@tcp(localhost:3306)/' \
//	  -bucket=tessera-repro -s3_endpoint=http://localhost:9000 -idle=64 -antispam
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-sql-driver/mysql"
	"github.com/transparency-dev/tessera"
	tessaws "github.com/transparency-dev/tessera/storage/aws"
	awsantispam "github.com/transparency-dev/tessera/storage/aws/antispam"
	"golang.org/x/mod/sumdb/note"
)

var (
	mysqlDSN   = flag.String("mysql_dsn", "", "DSN of a MySQL user that can create databases, e.g. root:root@tcp(localhost:3306)/")
	bucket     = flag.String("bucket", "", "S3 bucket to write the log to, under a random prefix")
	s3Endpoint = flag.String("s3_endpoint", "", "Endpoint of an S3-compatible store, with path-style addressing; empty for AWS")
	idle       = flag.Int("idle", 0, "MaxIdleConns for the sequencer's pool, and the antispam's")
	antispam   = flag.Bool("antispam", false, "Also use the persistent AWS antispam")
	rounds     = flag.Int("rounds", 5, "How many times to open, use and shut down the log")
	wait       = flag.Duration("wait", 100*time.Second, "How long to wait after the last round, so idle HTTP connections close, before counting again")
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	ctx := context.Background()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	suffix := hex.EncodeToString(b)
	admin, err := sql.Open("mysql", *mysqlDSN)
	must(err)
	admin.SetMaxOpenConns(1)
	logDSN, asDSN := createDB(admin, "reopen_log_"+suffix), createDB(admin, "reopen_as_"+suffix)
	skey, _, err := note.GenerateKey(rand.Reader, "example.com/reopen")
	must(err)
	signer, err := note.NewSigner(skey)
	must(err)
	sdkCfg, err := config.LoadDefaultConfig(ctx)
	must(err)
	s3Opts := func(o *s3.Options) {
		if *s3Endpoint != "" {
			o.BaseEndpoint, o.UsePathStyle = aws.String(*s3Endpoint), true
		}
	}

	baseG, baseT := runtime.NumGoroutine(), threads(admin)
	fmt.Printf("MaxIdleConns=%d antispam=%v\n", *idle, *antispam)
	for r := 1; r <= *rounds; r++ {
		rctx, cancel := context.WithCancel(ctx)
		d, err := tessaws.New(rctx, tessaws.Config{SDKConfig: &sdkCfg, S3Options: s3Opts, Bucket: *bucket, BucketPrefix: "reopen-" + suffix, DSN: logDSN, MaxIdleConns: *idle})
		must(err)
		opts := tessera.NewAppendOptions().WithCheckpointSigner(signer).WithCheckpointInterval(time.Second).WithBatching(1, 10*time.Millisecond)
		if *antispam {
			as, err := awsantispam.NewAntispam(rctx, asDSN, awsantispam.AntispamOpts{MaxIdleConns: *idle})
			must(err)
			opts = opts.WithAntispam(tessera.DefaultAntispamInMemorySize, as)
		}
		a, shutdown, lr, err := tessera.NewAppender(rctx, d, opts)
		must(err)
		f := a.Add(rctx, tessera.NewEntry(fmt.Appendf(nil, "round %d", r)))
		_, _, err = tessera.NewPublicationAwaiter(rctx, lr.ReadCheckpoint, 50*time.Millisecond).Await(rctx, f)
		must(err)
		sctx, scancel := context.WithTimeout(ctx, time.Minute)
		must(shutdown(sctx))
		scancel()
		cancel()
		time.Sleep(2 * time.Second) // let background work see the cancellation
		fmt.Printf("  after round %2d: +%d goroutines, +%d MySQL connections\n", r, runtime.NumGoroutine()-baseG, threads(admin)-baseT)
	}
	time.Sleep(*wait)
	fmt.Printf("  %s after the last round: +%d goroutines, +%d MySQL connections\n", *wait, runtime.NumGoroutine()-baseG, threads(admin)-baseT)
	fmt.Println("  goroutines, by function:")
	printGoroutines()
}

// printGoroutines prints how many goroutines are in each function, naming each
// stack by its first frame outside the runtime and the standard library's
// synchronisation and polling.
func printGoroutines() {
	var sb strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&sb, 1)
	counts := map[string]int{}
	for _, block := range strings.Split(sb.String(), "\n\n") {
		lines := strings.Split(block, "\n")
		n := 0
		for _, l := range lines {
			if _, err := fmt.Sscanf(l, "%d @", &n); err == nil {
				break
			}
		}
		for _, l := range lines {
			f := strings.Fields(l)
			if len(f) < 3 || f[0] != "#" {
				continue
			}
			if fn := f[2]; !strings.HasPrefix(fn, "runtime.") && !strings.HasPrefix(fn, "internal/poll.") && !strings.HasPrefix(fn, "sync.") && !strings.HasPrefix(fn, "time.") {
				counts[strings.SplitN(fn, "+", 2)[0]] += n
				break
			}
		}
	}
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return counts[keys[i]] > counts[keys[j]] })
	for _, k := range keys {
		if counts[k] > 1 {
			fmt.Printf("    %3d %s\n", counts[k], k)
		}
	}
}

func threads(db *sql.DB) int {
	var name string
	var n int
	must(db.QueryRow("SHOW GLOBAL STATUS LIKE 'Threads_connected'").Scan(&name, &n))
	return n
}

func createDB(admin *sql.DB, name string) string {
	_, err := admin.Exec("CREATE DATABASE " + name)
	must(err)
	cfg, err := mysql.ParseDSN(*mysqlDSN)
	must(err)
	cfg.DBName = name
	return cfg.FormatDSN()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
