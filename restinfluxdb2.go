package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

type ResticMessage struct {
	MessageType string `json:"message_type"`
}

type ResticStatus struct {
	MessageType      string   `json:"message_type"`
	SecondsElapsed   int      `json:"seconds_elapsed"`
	SecondsRemaining int      `json:"seconds_remaining"`
	PercentDone      float64  `json:"percent_done"`
	TotalFiles       int      `json:"total_files"`
	FilesDone        int      `json:"files_done"`
	TotalBytes       int      `json:"total_bytes"`
	BytesDone        int      `json:"bytes_done"`
	ErrorCount       int      `json:"error_count"`
	CurrentFiles     []string `json:"current_files"`
}

func (r *ResticStatus) ToInfluxDBPoint(repo string) (p *write.Point) {
	p = influxdb2.NewPointWithMeasurement("restic").
		AddTag("repository", repo).
		AddTag("type", r.MessageType).
		AddField("seconds_elapsed", r.SecondsElapsed).
		AddField("seconds_remaining", r.SecondsRemaining).
		AddField("percent_done", math.Round(r.PercentDone*100)).
		AddField("total_files", r.TotalFiles).
		AddField("files_done", r.FilesDone).
		AddField("total_bytes", r.TotalBytes).
		AddField("bytes_done", r.BytesDone).
		AddField("error_count", r.ErrorCount).
		SetTime(time.Now())
	return
}

type ResticSummary struct {
	MessageType           string  `json:"message_type"`
	FilesNew              int     `json:"files_new"`
	FilesChanged          int     `json:"files_changed"`
	FilesUnmodified       int     `json:"files_unmodified"`
	DirectoriesNew        int     `json:"dirs_new"`
	DirectoriesChanged    int     `json:"dirs_changed"`
	DirectoriesUnmodified int     `json:"dirs_unmodified"`
	DataBlobs             int     `json:"data_blobs"`
	TreeBlobs             int     `json:"tree_blobs"`
	DataAdded             int     `json:"data_added"`
	TotalFilesProcessed   int     `json:"total_files_processed"`
	TotalBytesProcessed   int     `json:"total_bytes_processed"`
	TotalDuration         float64 `json:"total_duration"`
	SnapshotID            string  `json:"snapshot_id"`
}

func (s *ResticSummary) ToInfluxDBPoint(repo string) (p *write.Point) {
	p = influxdb2.NewPointWithMeasurement("restic").
		AddTag("repository", repo).
		AddTag("type", s.MessageType).
		AddTag("snapshot_id", s.SnapshotID).
		AddField("files_new", s.FilesNew).
		AddField("files_changed", s.FilesChanged).
		AddField("files_unmodified", s.FilesUnmodified).
		AddField("directories_new", s.DirectoriesNew).
		AddField("directories_changed", s.DirectoriesChanged).
		AddField("directories_unmodified", s.DirectoriesUnmodified).
		AddField("data_blobs", s.DataBlobs).
		AddField("tree_blobs", s.TreeBlobs).
		AddField("data_added", s.DataAdded).
		AddField("total_files_processed", s.TotalFilesProcessed).
		AddField("total_bytes_processed", s.TotalBytesProcessed).
		AddField("total_duration", s.TotalDuration).
		SetTime(time.Now())
	return
}

func main() {

	// Get Restic repository name from arguments...
	if len(os.Args) < 2 {
		log.Fatalln("restic repository not specified (as a command line argument)")
	}
	restic_repo := os.Args[1]

	// Get InfluxDB settings from environment...
	influxdb2_url, ok := os.LookupEnv("RESTINFLUXDB2_URL")
	if !ok {
		log.Fatalln("influxdb2 url not specified (RESTINFLUXDB2_URL)")
	}
	influxdb2_token, ok := os.LookupEnv("RESTINFLUXDB2_TOKEN")
	if !ok {
		log.Fatalln("influxdb2 token not specified (RESTINFLUXDB2_TOKEN)")
	}
	influxdb2_org, ok := os.LookupEnv("RESTINFLUXDB2_ORG")
	if !ok {
		log.Fatalln("influxdb2 org not specified (RESTINFLUXDB2_ORG)")
	}
	influxdb2_bucket, ok := os.LookupEnv("RESTINFLUXDB2_BUCKET")
	if !ok {
		log.Fatalln("influxdb2 bucket not specified (RESTINFLUXDB2_BUCKET)")
	}

	// InfluxDB client
	client := influxdb2.NewClient(influxdb2_url, influxdb2_token)
	defer client.Close()

	// InfluxDB writer
	writeAPI := client.WriteAPI(influxdb2_org, influxdb2_bucket)
	defer writeAPI.Flush()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		// Load the JSON from Restic via STDIN...
		var text = scanner.Text()

		// Parse into a ResticMessage, so we can determine the message type...
		var rm ResticMessage
		json.Unmarshal([]byte(text), &rm)

		// Finally, parse the message into the correct struct depending on the type...
		switch strings.ToLower(rm.MessageType) {
		case "status":
			var rs ResticStatus
			json.Unmarshal([]byte(text), &rs)
			writeAPI.WritePoint(rs.ToInfluxDBPoint(restic_repo))
		case "summary":
			var ru ResticSummary
			json.Unmarshal([]byte(text), &ru)
			fmt.Print(ru)
		default:
			fmt.Fprintln(os.Stderr, "unknown restic message type:", rm.MessageType)
		}

		writeAPI.Flush()

	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading data from restic:", err)
	}
}
