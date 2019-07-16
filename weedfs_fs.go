package weedclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"

	//"net"
	"math/rand"
	"sync/atomic"
	"time"
)

type weedclient struct {
	root       string
	volMapLock *sync.RWMutex
	volMap     map[string][]string
	hc         *http.Client
	//rnd *rand.Rand
	lastVolumeUrlQuery int64
	lastMasterSwitch   int64
	masters            []string
	failStatus         []bool

	rootLock *sync.Mutex
}

func (w *weedclient) getRoot() string {
	w.rootLock.Lock()
	defer w.rootLock.Unlock()

	return w.root
}

func (w *weedclient) getClusterUrl(url string) string {
	if strings.HasPrefix(url, "http://") {
		// Test function needs to be used
		return url
	}

	return "http://" + url + "/cluster/status"
}

// curl http://xxx.xxx.xxx.xxxx:9133/cluster/status
// 当作健康检查API使用
func (w *weedclient) getClusterStatus(url string) (err error) {
	_, err = get(w.hc, url)
	return
}

func (w *weedclient) changeStatus() {

	ok := -1
	rootFail := false
	for k := 0; k < len(w.failStatus); k++ {

		var err error
		if err = w.getClusterStatus(w.getClusterUrl(w.masters[k])); err == nil {
			// todo delete print
			w.failStatus[k] = true
			ok = k
		} else {
			w.failStatus[k] = false
			if w.masters[k] == w.root {
				rootFail = true
			}
		}
		// todo lock, unlock
		// 暂定，不加锁修改状态，因为bool变量只有true, false, 两个状态
		//fmt.Printf("w = %p, k = %d, w.failStatus = %v, err = %v\n", w, k, w.failStatus, err)

	}

	if rootFail && ok >= 0 {
		w.root = w.masters[ok]
	}
}

func (w *weedclient) changeStatusLoop() {

	for {

		w.changeStatus()
		time.Sleep(time.Second * 2)
	}
}

type volLookUpRst struct {
	Locations []struct {
		PublicUrl string `json:"publicUrl"`
	} `json:"locations"`
}

func fetchVolumeUrlFromMaster(hc *http.Client, root string, id string) (ret []string, err error) {
	var buf []byte
	buf, err = get(hc, root+"/dir/lookup?volumeId="+id)
	if err != nil {
		return
	}

	var rst volLookUpRst
	if err = json.Unmarshal(buf, &rst); err != nil {
		return
	}

	ret = make([]string, 0, len(rst.Locations))
	if len(rst.Locations) > 0 {
		for _, l := range rst.Locations {
			ret = append(ret, l.PublicUrl)
		}
	}
	//NOTE: may return 0 size []string
	return
}

func fetchFile(hc *http.Client, path string) ([]byte, error) {
	buf, err := get(hc, path)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}
	return buf, nil
}

func getVolId(path string) (string, error) {
	ss := strings.Split(path, ",")
	if len(ss) == 2 {
		if ss[0] != "" {
			return ss[0], nil
		}
	}
	return "", errors.New("path in wrong format:" + path)
}

func NewWeedFsClient(hc *http.Client, masterPeers string) Fs {
	log.Println("new seaweedfs client to", masterPeers)

	ps := strings.Split(masterPeers, ",")

	ret := &weedclient{
		root:       ps[0],
		rootLock:   &sync.Mutex{},
		volMapLock: &sync.RWMutex{},
		volMap:     make(map[string][]string),
		hc:         hc,
		//rnd:rand.New(rand.NewSource(time.Now().Unix())),
		lastVolumeUrlQuery: 0,
		lastMasterSwitch:   0,
		masters:            ps,
		failStatus:         make([]bool, len(ps)),
	}

	for k := range ret.failStatus {
		ret.failStatus[k] = true
	}

	go ret.changeStatusLoop()
	return ret
}

func (this *weedclient) selectNewMasterPeer() bool {

	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	if len(this.masters) <= 1 {
		return false
	}

	this.rootLock.Lock()
	defer this.rootLock.Unlock()

	//把root节点设置为坏的
	for k := range this.masters {
		if this.masters[k] == this.root {
			this.failStatus[k] = false
			break
		}
	}

	idx := 0
	//使用随机算法选择一个url
	for maxTry := 100; maxTry > 0; maxTry-- {
		idx = rnd.Intn(len(this.masters))
		if this.failStatus[idx] == true {
			goto setRoot
		}
	}

	//随机算法没有找到url，直接遍历查找
	for idx = 0; idx < len(this.masters); idx++ {

		if this.failStatus[idx] == false {
			continue
		}
		break

	}

	// not found, master全部挂了
	if idx == len(this.masters) {
		return false
	}

setRoot:
	this.root = this.masters[idx]

	this.lastMasterSwitch = time.Now().Unix()

	return true
}

func (this *weedclient) getVUrlFromCache(vid string, exclude string) (string, error) {
	this.volMapLock.RLock()
	defer this.volMapLock.RUnlock()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	if vurls, ok := this.volMap[vid]; ok {
		if vurls == nil || len(vurls) == 0 {
			return "", CACHED_BUT_NO_VOL_EXISTS
		}
		if len(vurls) == 1 {
			if vurls[0] == exclude {
				return "", ONE_CACHED_BUT_EXCLUDED
			}
			return vurls[0], nil
		}

		idx := rnd.Intn(len(vurls))
		for vurls[idx] == exclude {
			idx = rnd.Intn(len(vurls))
		}
		return vurls[idx], nil
	}

	return "", NOT_CACHED_YET
}

func (this *weedclient) getVolumeUrl(path string, exclude string) (string, error) {
	vid, err := getVolId(path)
	if err != nil {
		return "", err
	}
	vurl, err := this.getVUrlFromCache(vid, exclude)
	if err == nil {
		return vurl, nil
	}
	switch err {
	case ONE_CACHED_BUT_EXCLUDED, CACHED_BUT_NO_VOL_EXISTS:
		lastQuerySec := atomic.LoadInt64(&this.lastVolumeUrlQuery)
		nowSec := time.Now().Unix()
		if nowSec-lastQuerySec > VOL_URL_QUERY_INTERVAL {
			vurls, err := fetchVolumeUrlFromMaster(this.hc, "http://"+this.getRoot(), vid)
			if err != nil {
				if this.selectNewMasterPeer() {
					vurls, err = fetchVolumeUrlFromMaster(this.hc, "http://"+this.getRoot(), vid)
				}
			}
			atomic.StoreInt64(&this.lastVolumeUrlQuery, time.Now().Unix())
			if err != nil {
				return "", err
			}
			this.volMapLock.Lock()
			this.volMap[vid] = vurls
			this.volMapLock.Unlock()
		}
	case NOT_CACHED_YET:
		vurls, err := fetchVolumeUrlFromMaster(this.hc, "http://"+this.getRoot(), vid)
		if err != nil {
			if this.selectNewMasterPeer() {
				vurls, err = fetchVolumeUrlFromMaster(this.hc, "http://"+this.getRoot(), vid)
			}
		}
		if err != nil {
			return "", err
		}
		this.volMapLock.Lock()
		this.volMap[vid] = vurls
		this.volMapLock.Unlock()
	}
	return this.getVUrlFromCache(vid, exclude)
}

func (this *weedclient) doReadButExcludeVurl(path string, exclude string) (int, []byte, error, string) {
	vurl, err := this.getVolumeUrl(path, exclude)
	if err != nil {
		return -1, nil, err, ""
	}

	var buf []byte
	buf, err = fetchFile(this.hc, "http://"+vurl+"/"+path)
	if err != nil {
		return -1, nil, err, vurl
	}
	return len(buf), buf, nil, vurl
}

func (this *weedclient) DoRead(path string) (l int, b []byte, err error) {
	var vurl string
	l, b, err, vurl = this.doReadButExcludeVurl(path, "")
	if err != nil && vurl != "" {
		//we have a volume server this for fid, but it failed, try other volume servers
		var err2 error
		l, b, err2, vurl = this.doReadButExcludeVurl(path, vurl)
		if err2 == ONE_CACHED_BUT_EXCLUDED {
			//no other volume servers
			err = err2
			return
		}
		return
	}
	return
}

type assignRst struct {
	Fid       string `json:"fid"`
	PublicUrl string `json:"publicUrl"`
	Url       string `json:"url"`
}
type wroteRst struct {
	Name string `json:"name"`
	Size int    `json:"size"`
}

func post(hc *http.Client, url string, content_type string, body []byte) (rspBody []byte, err error) {
	var req *http.Request
	var rsp *http.Response
	req, err = http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return
	}
	rsp, err = hc.Do(req)
	if err != nil {
		return
	}
	rspBody, err = ioutil.ReadAll(rsp.Body)
	defer rsp.Body.Close()
	return
}

func get(hc *http.Client, url string) (rspBody []byte, err error) {
	var req *http.Request
	var rsp *http.Response
	req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	rsp, err = hc.Do(req)
	if err != nil {
		return
	}
	rspBody, err = ioutil.ReadAll(rsp.Body)
	defer rsp.Body.Close()
	return
}

func multiPartPut(hc *http.Client, url string, fileName string, body []byte) (rspBody []byte, err error) {
	var req *http.Request
	var rsp *http.Response
	var wr io.Writer
	var n int64

	rawMultipart := &bytes.Buffer{}
	inStream := bytes.NewBuffer(body)
	multipartWriter := multipart.NewWriter(rawMultipart)
	wr, err = multipartWriter.CreateFormFile("file", fileName)
	if err != nil {
		return
	}
	n, err = io.Copy(wr, inStream)
	if err != nil && err != io.EOF {
		return
	}
	if int(n) != len(body) {
		err = errors.New("not copy enough data")
		return
	}
	multipartWriter.Close()

	req, err = http.NewRequest("PUT", url, bytes.NewBuffer(rawMultipart.Bytes()))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())

	rsp, err = hc.Do(req)
	if err != nil {
		return
	}
	rspBody, err = ioutil.ReadAll(rsp.Body)
	defer rsp.Body.Close()
	return
}

func (this *weedclient) DoWrite(refPath string, data []byte, params string) (string, error) {
	buf, err := post(this.hc, "http://"+this.root+"/dir/assign"+params, "text/plain", nil)
	if err != nil {
		if this.selectNewMasterPeer() {
			buf, err = post(this.hc, "http://"+this.root+"/dir/assign"+params, "text/plain", nil)
		}
	}
	if err != nil {
		return "", err
	}

	var fid string
	rst := assignRst{}
	if fid, err = func() (string, error) {
		//log.Println("to unmarshal", string(buf))
		if err := json.Unmarshal(buf, &rst); err != nil {
			return "", errors.New(fmt.Sprintf("decoding assign result %s failed %v", string(buf), err))
		}
		if rst.Fid == "" {
			return "", errors.New(fmt.Sprintf("assign failed:%v", string(buf)))
		}
		if rst.PublicUrl == "" {
			rst.PublicUrl = rst.Url
		}
		if rst.PublicUrl == "" {
			return "", errors.New("no volume url assigned")
		}
		if !strings.HasPrefix(rst.PublicUrl, "http") {
			rst.PublicUrl = "http://" + rst.PublicUrl
		}
		fid = rst.Fid
		return fid, nil
	}(); err != nil {
		return "", err
	}

	buf, err = multiPartPut(this.hc, rst.PublicUrl+"/"+fid, refPath, data)
	wroteRst := wroteRst{Size: -1}
	//log.Println("upload response:", string(buf))
	if err := json.Unmarshal(buf, &wroteRst); err != nil {
		return "", errors.New(fmt.Sprintf("decoding assign result %s failed %v", string(buf), err))
	}

	if wroteRst.Size == -1 {
		return "", errors.New("put to " + rst.PublicUrl + "/" + fid + " returns " + string(buf))
	}

	if wroteRst.Size != len(data) {
		return "", errors.New(fmt.Sprintf("wrote %d but required %d", wroteRst.Size, len(data)))
	}

	return fid, nil

}

// FileLocation 表示文件位置, 当前使用URL字段
type FileLocation struct {
	PublicURL string `json:"publicUrl"`
	URL       string `json:"url"`
}

// VolumeInfo 表示从seaweedfs中获取的文件信息
type VolumeInfo struct {
	VolumeId  string         `json:"volumeId"`
	Locations []FileLocation `json:"locations"`
}

func (this *weedclient) DoDelete(path string) error {
	//req, err := http.NewRequest("DELETE", "http://"+this.root+"/"+path, nil)
	idx := strings.IndexByte(path, ',')
	if idx == -1 {
		return fmt.Errorf("invalid seaweedfs file path: %s", path)
	}
	volumeId := path[0:idx]
	// 先获取文件所在的volume位置
	rsp, err := http.Get("http://" + this.root + "/dir/lookup?volumeId=" + volumeId)
	if err != nil {
		if this.selectNewMasterPeer() {
			rsp, err = http.Get("http://" + this.root + "/dir/lookup?volumeId=" + path)
		}
	}
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	var info VolumeInfo
	dec := json.NewDecoder(rsp.Body)
	err = dec.Decode(&info)
	if err != nil {
		return err
	}

	if len(info.Locations) == 0 || len(info.Locations[0].URL) == 0 {
		return fmt.Errorf("couldn't find the file information in seaweedfs, file: %s", path)
	}

	// 发起删除文件调用
	req, err := http.NewRequest("DELETE", "http://"+info.Locations[0].URL+"/"+path, nil)
	if err != nil {
		return err
	}
	rsp, err = this.hc.Do(req)
	if err != nil {
		return err
	}
	rsp.Body.Close()
	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		return errors.New(fmt.Sprint("response status ", rsp.StatusCode))
	}

	return nil
}

var ONE_CACHED_BUT_EXCLUDED = errors.New("only one url for vid but it is excluded")
var NOT_CACHED_YET = errors.New("volume url not cached")
var CACHED_BUT_NO_VOL_EXISTS = errors.New("no volume url reponse by master'")

const (
	VOL_URL_QUERY_INTERVAL = int64(5)
)
