package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcd struct {
	cli       *clientv3.Client
	watcher   clientv3.Watcher
	watchChan clientv3.WatchChan
}

func new_etcd(url string, user string, password string) *etcd {
	cfg := clientv3.Config{
		Endpoints: []string{url},
	}
	if user != "" {
		cfg.Username = user
		cfg.Password = password
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		panic(err)
	}
	return &etcd{cli: cli}
}

func (db *etcd) put(k, v string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	resp, err := db.cli.Put(ctx, k, v)
	cancel()
	if err != nil {
		panic(err)
	}
	return resp.Header.Revision
}

func (db *etcd) get(k string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	resp, err := db.cli.Get(ctx, k)
	cancel()
	if err != nil {
		panic(err)
	}
	return string(resp.Kvs[0].Value)
}

func (db *etcd) init_watch() {
	db.watcher = clientv3.NewWatcher(db.cli)
	db.watchChan = db.watcher.Watch(context.Background(), "foo")
}

func (db *etcd) watch(cnt int) {
	i := 0
	for resp := range db.watchChan {
		for _, _ = range resp.Events {
			//log.Printf("%+v\n", ev)
			i += 1
			if i == cnt {
				return
			}
		}
	}
}

// https://github.com/golang/go/issues/47840

var concurrent = flag.Int("c", 1, "concurrent req")
var nreqs = flag.Int("n", 800, "total reqs")
var host = flag.String("host", "http://localhost:2379", "etcd host")
var watch = flag.Bool("watch", false, "do watch")
var put = flag.Bool("put", false, "do put")
var user = flag.String("user", "", "user")
var password = flag.String("password", "", "password")

func main() {
	log.Println("etcd grpc benchmark")
	flag.Parse()
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("%-30s: %s\n", f.Usage, f.Value)
	})
	url := fmt.Sprintf("%s/v3/kv/put", *host)

	cli := new_etcd(url, *user, *password)
	cli.init_watch()

	var wg sync.WaitGroup
	if *watch {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cnt := (*nreqs) * (*concurrent)
			cli.watch(cnt)
		}()
	}

	if *put {
		for n := 0; n < *concurrent; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < *nreqs; i++ {
					//key := base64.StdEncoding.EncodeToString([]byte("foo"))
					//value := base64.StdEncoding.EncodeToString([]byte("bar"))
					key := "foo"
					value := "bar"
					//log.Println(key, value)
					cli.put(key, value)
				}
			}()
		}
	}

	start := time.Now()
	wg.Wait()
	end := time.Now()
	sum := end.Sub(start).Seconds()
	log.Printf("total: %f s\n", sum)
}
