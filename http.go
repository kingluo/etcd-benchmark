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
var host = flag.String("host", "localhost:2379", "etcd host")
var watch = flag.Bool("watch", false, "do watch")
var put = flag.Bool("put", false, "do put")
var user = flag.String("user", "", "user")
var password = flag.String("password", "", "password")

func main() {
	log.Println("etcd http benchmark")
	flag.Parse()
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("%-30s: %s\n", f.Usage, f.Value)
	})
	put_url := fmt.Sprintf("%s/v3/kv/put", *host)
	watch_url := fmt.Sprintf("%s/v3/watch", *host)
	auth_url := fmt.Sprintf("%s/v3/auth/authenticate", *host)

	var wg sync.WaitGroup
	client := &http.Client{}

	var do_auth bool = (*user != "")
	var token string
	if do_auth {
		auth := fmt.Sprintf(`{"name": "%s", "password": "%s"}`, *user, *password)
		buf := bytes.NewBufferString(auth)
		res, err := client.Post(auth_url, "application/json", buf)
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
		token = parsed["token"].(string)
		log.Printf("%-30s: %s\n", "token", token)
	}

	if *watch {
		watch_started := make(chan bool)
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf2 := bytes.NewBufferString(`{"create_request": {"key":"Zm9v"} }`)
			req, err := http.NewRequest("POST", watch_url, buf2)
			if err != nil {
				log.Fatal(err)
			}
			if do_auth {
				req.Header.Set("Authorization", token)
			}
			res, err := client.Do(req)
			if res.StatusCode != 200 {
				log.Fatalf("failed: %+v\n", res)
			}
			close(watch_started)
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
		<-watch_started
	}

	if *put {
		for n := 0; n < *concurrent; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data := map[string]interface{}{}
				for i := 0; i < *nreqs; i++ {
					data["key"] = base64.StdEncoding.EncodeToString([]byte("foo"))
					data["value"] = base64.StdEncoding.EncodeToString([]byte("bar"))
					buf, err := json.Marshal(data)
					if err != nil {
						log.Fatal(err)
					}
					buf2 := bytes.NewBuffer(buf)
					req, err := http.NewRequest("POST", put_url, buf2)
					if err != nil {
						log.Fatal(err)
					}
					if do_auth {
						req.Header.Set("Authorization", token)
					}
					res, err := client.Do(req)
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
	}
	start := time.Now()
	wg.Wait()
	end := time.Now()
	sum := end.Sub(start).Seconds()
	log.Printf("total: %f s\n", sum)
}
