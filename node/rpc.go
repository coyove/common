package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	rpcClient = &http.Client{
		Timeout: heartbeat / 2,
		Transport: &http.Transport{
			MaxConnsPerHost: 2,
		},
	}

	debug_stop_traffic_between string
)

func (n *Node) newRequest(id string, method string, args ...interface{}) *http.Request {
	req, _ := http.NewRequest("GET", "http://"+id+"/"+method, nil)
	query := url.Values{}
	for i := 0; i < len(args); i += 2 {
		query.Add(args[i].(string), fmt.Sprintf("%v", args[i+1]))
	}
	query.Add("from", n.ID)
	req.URL.RawQuery = query.Encode()
	return req
}

func (n *Node) rpc(id string, method string, args ...interface{}) (map[string]string, error) {
	if debug_stop_traffic_between != "" && strings.Contains(debug_stop_traffic_between, n.ID) && strings.Contains(debug_stop_traffic_between, id) {
		return nil, fmt.Errorf("")
	}

	resp, err := rpcClient.Do(n.newRequest(id, method, args...))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	p, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	m := map[string]string{}
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, err
	}
	return m, err
}

func simpleJSON(kvs ...interface{}) []byte {
	_, fn, line, _ := runtime.Caller(1)
	fn = filepath.Base(fn)
	p := bytes.Buffer{}
	p.WriteString("{")
	for i := 0; i < len(kvs); i += 2 {
		p.WriteString(fmt.Sprintf("\"%v\"", kvs[i]))
		p.WriteString(":")
		p.WriteString(strconv.Quote(fmt.Sprint(kvs[i+1])))
		p.WriteString(",")
	}
	p.WriteString(fmt.Sprintf("\"source\":\"%s:%d\"}", fn, line))
	return p.Bytes()
}

func (n *Node) wrapRPCHandler(h func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		fromid := r.FormValue("from")
		for _, id := range n.allNodes {
			if id == fromid {
				h(w, r)
				return
			}
		}
		w.Write(simpleJSON("status", "dont-know-you"))
	}
}
