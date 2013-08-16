package tempodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

var (
	ERR_INVALID_KEY = errors.New("Key is not in the correct format")
)

const (
	API_HOSTNAME    = "https://api.tempo-db.com"
	API_SECURE_PORT = 443
	ISO8601_FMT     = "2006-01-02T15:04:05.000Z0700"
	VERSION         = 0.1
)

var (
	USER_AGENT = fmt.Sprintf("%s/%s", "tempodb-go", VERSION)
	NullFilter = NewFilter()
)

type TempoTime struct {
	Time time.Time
}

type DataPoint struct {
	Ts *TempoTime `json:"t"`
	V  float64    `json:"v"`
}

type BulkDataSet struct {
	Ts   *TempoTime  `json:"t"`
	Data []BulkPoint `json:"data"`
}

type BulkPoint interface {
	GetValue() float64
}

type BulkKeyPoint struct {
	Key string  `json:"key"`
	V   float64 `json:"v"`
}

type BulkIdPoint struct {
	Id string  `json:"id"`
	V  float64 `json:"v"`
}

type Remoter interface {
	Do(*http.Request) (*http.Response, error)
}

type createSeriesRequest struct {
	Key string
}

type DataSet struct {
	Series  Series             `json:"series"`
	Start   TempoTime          `json:"start"`
	End     TempoTime          `json:"end"`
	Data    []*DataPoint       `json:"data"`
	Summary map[string]float64 `json:"summary"`
}

type Series struct {
	Id         string            `json:"id"`
	Key        string            `json:"key"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes"`
	Tags       []string          `json:"tags"`
}

type Client struct {
	Key     string
	Secret  string
	Host    string
	Port    int
	Remoter Remoter
}

type Filter struct {
	Ids        []string
	Keys       []string
	Tags       []string
	Attributes map[string]string
}

func NewClient(key string, secret string) *Client {
	client := &Client{Key: key, Secret: secret, Host: API_HOSTNAME, Port: API_SECURE_PORT}
	client.Remoter = &http.Client{}
	return client
}

func NewFilter() *Filter {
	return &Filter{
		Ids:        make([]string, 0),
		Keys:       make([]string, 0),
		Tags:       make([]string, 0),
		Attributes: make(map[string]string),
	}
}

func (tt *TempoTime) MarshalJSON() ([]byte, error) {
	formatted := fmt.Sprintf("\"%s\"", tt.Time.Format(ISO8601_FMT))
	return []byte(formatted), nil
}

func (tt *TempoTime) UnmarshalJSON(data []byte) error {
	b := bytes.NewBuffer(data)
	decoded := json.NewDecoder(b)
	var s string
	if err := decoded.Decode(&s); err != nil {
		return err
	}
	t, err := time.Parse(ISO8601_FMT, s)
	if err != nil {
		return err
	}
	tt.Time = t

	return nil
}

func (filter *Filter) AddId(id string) {
	filter.Ids = append(filter.Ids, id)
}

func (filter *Filter) AddKey(key string) {
	filter.Keys = append(filter.Keys, key)
}

func (filter *Filter) AddTag(tag string) {
	filter.Tags = append(filter.Tags, tag)
}

func (filter *Filter) AddAttribute(key string, value string) {
	filter.Attributes[key] = value
}

func (bp *BulkKeyPoint) GetValue() float64 {
	return bp.V
}

func (bp *BulkIdPoint) GetValue() float64 {
	return bp.V
}

func (client *Client) GetSeries(filter *Filter) ([]*Series, error) {
	url := client.buildUrl("/series?", "", filter.encodeUrl())
	resp, err := client.makeRequest(url, "GET", []byte{})
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.Status, b)
	}
	var series []*Series
	err = json.Unmarshal(b, &series)
	if err != nil {
		return nil, err
	}

	return series, nil
}

func (client *Client) CreateSeries(key string) (*Series, error) {
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9\.:;\-_/\\ ]*$`, key)

	if matched == false {
		return nil, ERR_INVALID_KEY
	}

	cr := &createSeriesRequest{key}
	reqBody, err := json.Marshal(cr)
	if err != nil {
		return nil, err
	}
	url := client.buildUrl("/series/", "", "")
	resp, err := client.makeRequest(url, "POST", reqBody)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.Status, b)
	}

	var series Series
	err = json.Unmarshal(b, &series)
	if err != nil {
		return nil, err
	}

	return &series, nil
}

func (client *Client) UpdateSeries(series *Series) (*Series, error) {
	endpointUrl := fmt.Sprintf("/series/id/%s/", url.QueryEscape(series.Id))
	url := client.buildUrl(endpointUrl, "", "")
	b, err := json.Marshal(series)
	if err != nil {
		return nil, err
	}
	resp, err := client.makeRequest(url, "PUT", b)
	if err != nil {
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.Status, respBody)
	}
	var responseSeries Series
	err = json.Unmarshal(respBody, &responseSeries)
	if err != nil {
		return nil, err
	}

	return &responseSeries, nil
}

func (client *Client) WriteId(id string, data []*DataPoint) error {
	return client.writeSeries("id", id, data)
}

func (client *Client) WriteKey(key string, data []*DataPoint) error {
	return client.writeSeries("key", key, data)
}

func (client *Client) WriteBulk(ts time.Time, data []BulkPoint) error {
	url := client.buildUrl("/data/", "", "")
	dataSet := &BulkDataSet{
		Ts:   &TempoTime{Time: ts},
		Data: data,
	}
	b, err := json.Marshal(dataSet)
	if err != nil {
		return err
	}
	resp, err := client.makeRequest(url, "POST", b)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return httpError(resp.Status, respBody)
	}

	return nil
}

func (client *Client) Read(start time.Time, end time.Time, filter *Filter) ([]*DataSet, error) {
	url := client.buildUrl("/data?", client.encodeTimes(start, end), filter.encodeUrl())
	resp, err := client.makeRequest(url, "GET", []byte{})
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.Status, b)
	}

	var datasets []*DataSet
	err = json.Unmarshal(b, &datasets)
	if err != nil {
		return nil, err
	}

	return datasets, nil
}

func (client *Client) ReadKey(key string, start time.Time, end time.Time) (*DataSet, error) {
	return client.readSeries("key", key, start, end)
}

func (client *Client) ReadId(id string, start time.Time, end time.Time) (*DataSet, error) {
	return client.readSeries("id", id, start, end)
}

func (client *Client) IncrementId(id string, data []*DataPoint) error {
	return client.incrementSeries("id", id, data)
}

func (client *Client) IncrementKey(key string, data []*DataPoint) error {
	return client.incrementSeries("key", key, data)
}

func (client *Client) IncrementBulk(ts time.Time, data []BulkPoint) error {
	url := client.buildUrl("/increment/", "", "")
	dataSet := &BulkDataSet{
		Ts:   &TempoTime{Time: ts},
		Data: data,
	}
	b, err := json.Marshal(dataSet)
	if err != nil {
		return err
	}
	resp, err := client.makeRequest(url, "POST", b)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return httpError(resp.Status, respBody)
	}

	return nil
}

func (client *Client) DeleteId(id string, start time.Time, end time.Time) error {
	return client.deleteSeries("id", id, start, end)
}

func (client *Client) DeleteKey(key string, start time.Time, end time.Time) error {
	return client.deleteSeries("key", key, start, end)
}

func (client *Client) readSeries(series_type string, seriesVal string, start time.Time, end time.Time) (*DataSet, error) {
	endpointUrl := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(seriesVal))
	url := client.buildUrl(endpointUrl, client.encodeTimes(start, end), "")
	resp, err := client.makeRequest(url, "GET", []byte{})
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.Status, b)
	}

	var dataset DataSet
	err = json.Unmarshal(b, &dataset)
	if err != nil {
		return nil, err
	}

	return &dataset, nil
}

func (client *Client) writeSeries(series_type string, seriesVal string, data []*DataPoint) error {
	endpointUrl := fmt.Sprintf("/series/%s/%s/data/", series_type, url.QueryEscape(seriesVal))

	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	url := client.buildUrl(endpointUrl, "", "")
	resp, err := client.makeRequest(url, "POST", b)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return httpError(resp.Status, respBody)
	}

	return nil
}

func (client *Client) incrementSeries(series_type string, seriesVal string, data []*DataPoint) error {
	endpointUrl := fmt.Sprintf("/series/%s/%s/increment/?", series_type, url.QueryEscape(seriesVal))
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	url := client.buildUrl(endpointUrl, "", "")
	resp, err := client.makeRequest(url, "POST", b)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return httpError(resp.Status, respBody)
	}

	return nil
}

func (client *Client) deleteSeries(series_type string, seriesVal string, start time.Time, end time.Time) error {
	endpointUrl := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(seriesVal))
	url := client.buildUrl(endpointUrl, client.encodeTimes(start, end), "")
	resp, err := client.makeRequest(url, "DELETE", []byte{})
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return httpError(resp.Status, respBody)
	}

	return nil
}

func (client *Client) buildUrl(endpoint string, times string, paramsStr string) string {
	if times == "" {
		return client.Host + "/v1" + endpoint + paramsStr
	}

	return client.Host + "/v1" + endpoint + times + "&" + paramsStr
}

func (client *Client) encodeTimes(start time.Time, end time.Time) string {
	v := url.Values{}
	startStr := start.Format(ISO8601_FMT)
	endStr := end.Format(ISO8601_FMT)
	v.Add("start", startStr)
	v.Add("end", endStr)

	return v.Encode()
}

func (filter *Filter) encodeUrl() string {
	v := url.Values{}
	for _, id := range filter.Ids {
		v.Add("id", id)
	}

	for _, key := range filter.Keys {
		v.Add("key", key)
	}

	for _, tag := range filter.Tags {
		v.Add("tag", tag)
	}

	for key, value := range filter.Attributes {
		v.Add(fmt.Sprintf("attr[%s]", key), value)
	}

	return v.Encode()
}

func (client *Client) makeRequest(builtUrl string, method string, formString []byte) (*http.Response, error) {
	req, err := http.NewRequest(method, builtUrl, bytes.NewReader(formString))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(client.Key, client.Secret)
	req.Header["User-Agent"] = []string{USER_AGENT}

	return client.Remoter.Do(req)
}

func httpError(status string, body []byte) error {
	length := len(body)
	if length == 0 || length == 1 {
		return errors.New(status)
	}

	return errors.New(fmt.Sprintf("%s: %s", status, string(body)))
}
