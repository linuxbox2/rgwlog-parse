// -*- mode:go; tab-width:4; indent-tabs-mode:t -*-

package main

import (
	"bufio"
    "flag"
    "fmt"
    "log"
    "os"
	"regexp"
	"strconv"
	"time"
	"sort"
)

type rgwReq struct {
	id int64
	start_ts string
	end_ts string
	start_line int64
	end_line int64
	uri string
	op string
	trans_id string
	start_req string
	end_req string
	duration time.Duration
}

func parseCephTime(ct string) time.Time {
	layout := "2006-02-06 15:04:05.000000"
	tc, _ := time.Parse(layout, ct)
	return tc
}

func toMillis(d time.Duration) int64 {
	return d.Nanoseconds() / 1000000
}

type rgwReqArr []rgwReq

func (s rgwReqArr) Len() int {
    return len(s)
}

func (s rgwReqArr) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}

func (s rgwReqArr) Less(i, j int) bool {
    return toMillis(s[i].duration) < toMillis(s[j].duration)
}

func printReq(req_p rgwReq) {
	duration_ms := toMillis(req_p.duration)
	fmt.Printf("req id=%d op=%s start_ts=%s end_ts=%s uri=%s duration=%dms\n",
		req_p.id, req_p.op, req_p.start_ts, req_p.end_ts,
		req_p.uri, duration_ms)
}

func main() {
	var logpath string
	flag.StringVar(&logpath, "path", "/tmp/rgw.log",
		"path to rgw log at debug-rgw=20, debug-ms=1")
	var flag_ms int
	flag.IntVar(&flag_ms, "slow_ms", 40,
		"note requests taking longer than <slow_ms>ms")
	flag.Parse()
	var slow_ms int64
	slow_ms = int64(flag_ms)

	file, err := os.Open(logpath)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

	reqs_map := make(map[int64]rgwReq)
	slow_reqs := make([]rgwReq, 0)
	
	//2017-03-02 17:53:00.121512 7fe3f0e29700  2 req 1545628:0.000011::GET /devbcos/bcs/dajirarddev/GDDataScienceNews_OM77ZBBE07IK_body.html/1/1::initializing for trans_id = tx00000000000000017959c-0058b85bfc-228bde-cnj1-dev

	req_start := "(\\d+-\\d+-\\d+ \\d+:\\d+:\\d+.\\d+) \\w+\\s+\\d+ req (\\d+):(\\d+.\\d+)::(\\w+) ([\\w/\\?\\-\\._]+)::initializing for trans_id = ([\\w\\d\\-]+)"
	re_req_start := regexp.MustCompile(req_start)

	//2017-03-02 17:53:00.009071 7fe3a6594700  2 req 1545623:0.009503:s3:GET /devbcos/bcs/dajirastg/58B8596F029C07BA0119AE820000_message_body.html/2/1:get_obj:http status=200
	req_done := "(\\d+-\\d+-\\d+ \\d+:\\d+:\\d+.\\d+) \\w+\\s+\\d+ req (\\d+):(\\d+.\\d+):(\\w+):(\\w+) ([\\w/\\?\\-\\._]+):[\\w\\_]+:http status=(\\d+)"

	re_req_done := regexp.MustCompile(req_done)

	var i, j, k int64
	i = 0 // line#
	j = 0 // match#
	k = 0 // matching start and end
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
		line := scanner.Text()
        //fmt.Println(line)
		m := re_req_start.FindStringSubmatch(line)
		if (m != nil) {
			if (false) {
				for k, v := range m {
					fmt.Printf("%d ::: %s\n", k, v)
				}
			}
			req_id, _ := strconv.ParseInt(m[2], 10, 0)
			req_ts := m[1]
			req_op := m[4]
			req_uri := m[5]
			req_trans := m[6]
			req_s := rgwReq{
				req_id, req_ts, "", i, -1, req_uri, req_op, req_trans, line,
				"", 0}
			if (false) {
				printReq(req_s)
			}
			reqs_map[req_id] = req_s
			j++
		} else {
			// try to find req finish
  			m := re_req_done.FindStringSubmatch(line)
			if (m != nil) {
				if (false) {
					for k, v := range m {
						fmt.Printf("DONE %d ::: %s\n", k, v)
					}
				}
				req_id, _ := strconv.ParseInt(m[2], 10, 0)
				req_p, has_k := reqs_map[req_id]
				if (has_k) {
					req_p.end_ts = m[1]
					start := parseCephTime(req_p.start_ts)
					end := parseCephTime(req_p.end_ts)
					req_p.duration = end.Sub(start)
					duration_ms := toMillis(req_p.duration)
					if (false) {
						printReq(req_p)
						fmt.Println("start time " , start, " end time ", end)
					}
					reqs_map[req_id] = req_p
					if (duration_ms > slow_ms) {
						slow_reqs = append(slow_reqs, req_p)
					}
					k++
				}
			}
		}
		i++
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

	fmt.Printf("Finished, parsed %d lines (%d matches, %d braced)\n",
		i, j, k)

	if (len(slow_reqs) > 0) {
		sort.Sort(rgwReqArr(slow_reqs))
		slowest := slow_reqs[len(slow_reqs)-1]
		fmt.Printf("Saw %d requests longer than %dms:\n",
			len(slow_reqs), slow_ms)
		fmt.Printf("Slowest was:")
		printReq(slowest)
		if (false) {
			fmt.Println("dump reqs:")
			for _, elt := range slow_reqs {
				printReq(elt)
			}
		}
	}

	for i, req_p := range reqs_map {
		fmt.Printf("req %d:: ", i )
		printReq(req_p)
	}

}
