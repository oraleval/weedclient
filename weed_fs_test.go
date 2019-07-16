package weedclient

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

//测试所用构造函数
func newWeedClient() *weedclient {

	return &weedclient{
		root:       "U1",
		rootLock:   &sync.Mutex{},
		masters:    []string{"U1", "U2", "U3"},
		failStatus: []bool{true, true, true},
	}
}

func TestSelectNewMasterPeer(t *testing.T) {

	w := newWeedClient()

	w.selectNewMasterPeer()
	if w.failStatus[0] {
		t.Errorf("select fail 1 %v\n", w.failStatus)
		return
	}

	w.selectNewMasterPeer()
	if w.failStatus[1] && w.failStatus[2] {
		t.Errorf("select fail 2 %v\n", w.failStatus)
		return
	}

	w.selectNewMasterPeer()
	if w.failStatus[0] || w.failStatus[1] || w.failStatus[2] {
		t.Errorf("select fail 3 %v\n", w.failStatus)
		return
	}
}

func TestChangesStatusLoop(t *testing.T) {
	w := weedclient{
		hc:         &http.Client{},
		rootLock:   &sync.Mutex{},
		failStatus: []bool{false, false, false},
	}

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("test server1 \n")
		fmt.Fprintln(w, "test server1")
	}))

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("test server2 \n")
		fmt.Fprintln(w, "test server2")
	}))

	ts3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("test server3 \n")
		fmt.Fprintln(w, "test server3")
	}))

	w.masters = []string{ts1.URL, ts2.URL, ts3.URL}
	w.root = ts1.URL

	w.changeStatus()

	for _, v := range w.failStatus {
		if !v {
			t.Errorf(" changes status fail:%v\n", w.failStatus)
		}
	}
}
