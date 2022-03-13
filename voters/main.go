package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/tendermint/tendermint/libs/json"
	rpchttp "github.com/tendermint/tendermint/rpc/client/http"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	rpcEndpoint string
	lcdEndpoint string
	outFile     string
	blockHeight int64
	maxHeight   int64
	showAll     bool
	dedup       = make(map[string]bool) // large and inefficient, but good enough
	f           *os.File
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.StringVar(&rpcEndpoint, "rpc", "http://127.0.0.1:26657", "rpc endpoint")
	flag.StringVar(&lcdEndpoint, "lcd", "http://127.0.0.1:1317", "lcd endpoint")
	flag.StringVar(&outFile, "out", "out.csv", "csv file to write")
	flag.Int64Var(&blockHeight, "start", 2224300, "starting height")
	flag.Int64Var(&maxHeight, "end", 0, "ending height, if 0 will get current height")
	flag.BoolVar(&showAll, "all", false, "show all voters, not just odd ones")
	flag.Parse()

	var err error
	f, err = os.OpenFile(outFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}

	client, _ := rpchttp.New(rpcEndpoint, "/websocket")
	err = client.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status, err := client.Status(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if maxHeight == 0 {
		maxHeight = status.SyncInfo.LatestBlockHeight
	}

	voters := make(chan string)
	go getDetails(ctx, voters, f)

	for ; blockHeight <= maxHeight; blockHeight += 1 {
		if blockHeight % 10 == 0 {
			log.Println("block ", blockHeight)
		}
		resp, e := client.BlockResults(ctx, &blockHeight)
		if e != nil {
			log.Fatalf("could not query blocks at height %d: %v", blockHeight, e)
		}
		for _, txr := range resp.TxsResults {
			for event := range txr.Events {
				if txr.Events[event].Type == "tx" {
					if ok, who := wasVote(txr.Log); ok && !dedup[who] {
						dedup[who] = true
						voters <- who
					}
				}
			}
		}
	}
}

func getDetails(ctx context.Context, whom chan string, f *os.File) {
	tick := time.NewTicker(15*time.Second)
	buf := bytes.NewBufferString("address,validators staked,ujuno balance\n")
	wMux := sync.Mutex{}
	var counter uint32
	defer func() {
		wMux.Lock()
		f.Write(buf.Bytes())
		f.Close()
		wMux.Unlock()
	}()
	for {
		select {
		case who := <-whom:
			go func(who string) {
				bal, err := getBalance(who)
				if err != nil {
					log.Println(err)
					return
				}
				count, staked, err := getStakeCount(who)
				if err != nil {
					log.Println(err)
					return
				}
				bal += staked
				if showAll || (count == 0 || bal < 100_000) {
					wMux.Lock()
					buf.WriteString(fmt.Sprintf("%s,%d,%d\n", who, count, bal))
					wMux.Unlock()
					counter += 1
				}
			}(who)
		case <-ctx.Done():
			log.Println("account lookup worker exiting")
			return
		case <-tick.C:
			log.Printf("indexed %d voters, logged %d\n", len(dedup), counter)
			wMux.Lock()
			f.Write(buf.Bytes())
			buf.Reset()
			wMux.Unlock()
		}
	}
}

type StakedTo struct {
	Dels []struct {
		Balance struct {
			Denom  string `json:"denom"`
			Amount string `json:"amount"`
		} `json:"balance"`
	} `json:"delegation_responses"` // don't care bout who, just count
}

func getStakeCount(who string) (count int, amount int64, err error) {
	vals := &StakedTo{}
	resp, err := http.Get(lcdEndpoint + "/cosmos/staking/v1beta1/delegations/" + who)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, vals)
	if err != nil {
		return
	}
	for i := range vals.Dels {
		if stake, e := strconv.ParseInt(vals.Dels[i].Balance.Amount, 10, 64); e == nil && vals.Dels[i].Balance.Denom == "ujuno" {
			amount += stake
		}
	}
	count = len(vals.Dels)
	return
}

type Balance struct {
	Balances []struct {
		Denom  string `json:"denom"`
		Amount string `json:"amount"`
	} `json:"balances"`
}

func getBalance(who string) (ujuno int64, err error) {
	bal := &Balance{}
	resp, err := http.Get(lcdEndpoint + "/cosmos/bank/v1beta1/balances/" + who)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, bal)
	if err != nil {
		return
	}
	for i := range bal.Balances {
		if bal.Balances[i].Denom == "ujuno" {
			ujuno, err = strconv.ParseInt(strings.Replace(bal.Balances[i].Amount, "ujuno", "", 1), 10, 64)
			return
		}
	}
	return
}

type logMsg struct {
	Events []struct {
		Type       string `json:"type"`
		Attributes []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"attributes"`
	} `json:"events"`
}

func wasVote(s string) (voted bool, addr string) {
	if !strings.HasPrefix(s, "[") {
		return
	}
	events := make([]logMsg, 0)
	err := json.Unmarshal([]byte(s), &events)
	if err != nil {
		log.Println(err)
		return
	}
	for _, evt := range events {
		for o := range evt.Events {
			if voted {
				break
			}
			for i := range evt.Events[o].Attributes {
				if evt.Events[o].Attributes[i].Key == "action" && evt.Events[o].Attributes[i].Value == "/cosmos.gov.v1beta1.MsgVote" {
					voted = true
					break
				}
			}
		}
		if !voted {
			continue
		}
		for o := range evt.Events {
			for i := range evt.Events[o].Attributes {
				if evt.Events[o].Attributes[i].Key == "sender" {
					addr = evt.Events[o].Attributes[i].Value
					return
				}
			}
		}
	}
	return
}
