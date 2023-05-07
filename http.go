package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

var concurrent = flag.Int("c", 1, "concurrent req")
var nreqs = flag.Int("n", 800, "total reqs")
var host = flag.String("h", "localhost:2379", "etcd host")
var watch = flag.Bool("w", false, "do watch")

func main() {
	log.Println("etcd http benchmark")
	flag.Parse()
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("%-30s: %s\n", f.Usage, f.Value)
	})
	put_url := fmt.Sprintf("%s/v3/kv/put", *host)
	watch_url := fmt.Sprintf("%s/v3/watch", *host)

	var wg sync.WaitGroup
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}

	if *watch {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf2 := bytes.NewBufferString(`{"create_request": {"key":"Zm9v"} }`)
			res, err := client.Post(watch_url, "application/json", buf2)
			if err != nil {
				log.Fatal(err)
			}
			if res.StatusCode != 200 {
				log.Fatalf("failed: %+v\n", res)
			}
			cnt := (*nreqs)*(*concurrent) + 1
			scanner := bufio.NewScanner(res.Body)
			for scanner.Scan() {
				evt := scanner.Bytes()
				var parsed map[string]interface{}
				err = json.Unmarshal(evt, &parsed)
				if err != nil {
					log.Fatal(err)
				}
				//log.Printf("%+v\n", parsed)
				cnt -= 1
				if cnt == 0 {
					break
				}
			}
		}()
	}

	for n := 0; n < *concurrent; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//data := map[string]interface{}{"key":"Zm9v", "value":"YmFy"}
			data := map[string]interface{}{}
			for i := 0; i < *nreqs; i++ {
				data["key"] = base64.StdEncoding.EncodeToString([]byte("foo"))
				data["value"] = base64.StdEncoding.EncodeToString([]byte("bar"))
				buf, err := json.Marshal(data)
				if err != nil {
					log.Fatal(err)
				}
				buf2 := bytes.NewBuffer(buf)
				res, err := client.Post(put_url, "application/json", buf2)
				if err != nil {
					log.Fatal(err)
				}
				body, err := io.ReadAll(res.Body)
				res.Body.Close()
				if err != nil {
					log.Fatal(err)
				}
				if res.StatusCode != 200 {
					log.Fatalf("failed: %+v, body: %+v\n", res, string(body))
				}
				var parsed map[string]interface{}
				err = json.Unmarshal(body, &parsed)
				if err != nil {
					log.Fatal(err)
				}
				//log.Println(parsed)
			}
		}()
	}
	start := time.Now()
	wg.Wait()
	end := time.Now()
	sum := end.Sub(start).Seconds()
	log.Printf("total: %f s\n", sum)
}
