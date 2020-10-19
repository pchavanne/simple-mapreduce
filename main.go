package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/labstack/echo"
)

func main() {
	t := os.Getenv("TYPE")
	switch t {
	case "MASTER":
		master()
	case "MAP":
		mapper()
	case "REDUCE":
		reducer()
	}
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func master() {
	// create an instance of the http server
	e := echo.New()

	var client = &http.Client{}
	// add the request hnadler for the route
	e.GET("/compute", func(c echo.Context) error {
		text := c.QueryParam("text")
		// 1. Splitting
		words := strings.Split(text, " ")

		mapperHost := os.Getenv("MAPPER_HOST")
		var mapperIps []string
		ips, _ := net.LookupIP(mapperHost)
		for _, ip := range ips {
			mapperIps = append(mapperIps, ip.String())
		}
		mapSplitCount := int(math.Ceil(float64(len(words)) / float64(len(mapperIps))))
		var mapSplits = map[string][]string{}
		for idx, mapperIp := range mapperIps {
			if idx*mapSplitCount >= len(words) {
				break
			}
			mapSplits[mapperIp] = words[idx*mapSplitCount : min(idx*mapSplitCount+mapSplitCount, len(words))]
		}
		// 2. Mapping
		var mapping = map[string]map[string]int{}
		var wgm sync.WaitGroup
		wgm.Add(len(mapSplits))
		for host, split := range mapSplits {
			go func(host string, split []string) {
				defer wgm.Done()
				req, _ := http.NewRequest("GET", fmt.Sprintf("http;//%s:%s/map", host, os.Getenv("MAPPER_PORT")), nil)
				q := req.URL.Query()
				q.Add("str", strings.Join(split, " "))
				req.URL.RawQuery = q.Encode()
				res, _ := client.Do(req)
				body, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				buf := bytes.NewBuffer(body)
				var decodeMap map[string]int
				decoder := gob.NewDecoder(buf)
				_ = decoder.Decode(&decodeMap)
				mapping[host] = decodeMap
			}(host, split)
		}
		wgm.Wait()
		// 3. Shuffling
		var shuffling = map[string][]int{}
		for _, host := range mapping {
			for word, count := range host {
				shuffling[word] = append(shuffling[word], count)
			}
		}
		reducerHost := os.Getenv("REDUCER_HOST")
		var reducerIps []string
		ips, _ = net.LookupIP(reducerHost)
		for _, ip := range ips {
			reducerIps = append(reducerIps, ip.String())
		}
		var shuffleWords []string
		for word := range shuffling {
			shuffleWords = append(shuffleWords, word)
		}
		reduceSplitCount := int(math.Ceil(float64(len(shuffleWords)) / float64(len(reducerIps))))
		var reduceSplits = map[string]map[string][]int{}
		for idx, reducerIp := range reducerIps {
			if idx*reduceSplitCount >= len(shuffleWords) {
				break
			}
			reduceWords := shuffleWords[idx*reduceSplitCount : min(idx*reduceSplitCount+reduceSplitCount, len(shuffleWords))]
			reduceSplits[reducerIp] = map[string][]int{}
			for _, reducekey := range reduceWords {
				reduceSplits[reducerIp][reducekey] = shuffling[reducekey]
			}
		}
		// 4. Reducing
		var reducing = map[string]map[string]int{}
		var wgr sync.WaitGroup
		wgr.Add(len(reduceSplits))
		for host, split := range reduceSplits {
			go func(host string, slpit map[string][]int) {
				defer wgr.Done()
				req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s:%s/reduce", host, os.Getenv("REDUCER_PORT")), nil)
				buf := new(bytes.Buffer)
				encoder := gob.NewEncoder(buf)
				_ = encoder.Encode(split)
				q := req.URL.Query()
				q.Add("body", string(buf.Bytes()))
				req.URL.RawQuery = q.Encode()
				res, _ := client.Do(req)
				body, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()
				buf = bytes.NewBuffer(body)
				var decodeReduce = map[string]int{}
				decoder := gob.NewDecoder(buf)
				_ = decoder.Decode(&decodeReduce)
				reducing[host] = decodeReduce
			}(host, split)
		}
		wgr.Wait()
		return json.NewEncoder(c.Response()).Encode(&reducing)
	})
	e.Logger.Fatal(e.Start(":8080"))
}

func mapper() {
	e := echo.New()

	e.GET("/map", func(c echo.Context) error {
		str := c.QueryParam("str")
		words := strings.Split(str, " ")
		mapping := map[string]int{}
		for _, w := range words {
			if _, prs := mapping[w]; prs {
				mapping[w]++
			} else {
				mapping[w] = 1
			}
		}
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		_ = encoder.Encode(mapping)

		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})
	e.Logger.Fatal(e.Start(":8080"))
}

func reducer() {
	e := echo.New()

	e.GET("/reduce", func(c echo.Context) error {
		body := c.QueryParam("body")
		buf := bytes.NewBuffer([]byte(body))
		var reduceData = map[string][]int{}
		decoder := gob.NewDecoder(buf)
		_ = decoder.Decode(&reduceData)
		var reducing = map[string]int{}
		for key, value := range reduceData {
			reducing[key] = 0
			for _, count := range value {
				reducing[key] += count
			}
		}
		buf = new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)

		_ = encoder.Encode(reducing)
		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})
	e.Logger.Fatal(e.Start(":8080"))
}
