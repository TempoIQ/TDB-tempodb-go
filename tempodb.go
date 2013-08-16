package tempodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

var (
	ERR_INVALID_KEY = errors.New("Key is not in the correct format")
)

const (
	API_HOSTNAME = "https://api.tempo-db.com"
	ISO8601_FMT  = "2006-01-02T15:04:05.000Z0700"
)

type TempoTime struct {
	Time time.Time
}

type DataPoint struct {
	Ts *TempoTime `json:"t"`
	V  float64    `json:"v"`
}

type Remoter interface {
	Do(*http.Request) (*http.Response, error)
}

type createSeriesRequest struct {
	Key string
}

type DataSet struct {
	Series  Series
	Start   TempoTime
	End     TempoTime
	Data    []*DataPoint
	Summary map[string]float64
}

type Series struct {
	Id         string
	Key        string
	Name       string
	Attributes map[string]string
	Tags       []string
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

func NewClient() *Client {
	client := &Client{Host: API_HOSTNAME, Port: 443}
	client.Remoter = &http.Client{}
	return client
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

func (client *Client) GetSeries(filter *Filter) ([]*Series, error) {
	var URL string
	URL = client.buildUrl("/series?", "", filter.encodeUrl())
	resp := client.makeRequest(URL, "GET", []byte{})

	var series []*Series
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
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
	resp := client.makeRequest(url, "POST", reqBody)

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var series Series
	err = json.Unmarshal(respBody, &series)
	if err != nil {
		return nil, err
	}

	return &series, nil
}

func (client *Client) WriteId(id string, data []*DataPoint) error {
	return client.writeSeries("id", id, data)
}

func (client *Client) WriteKey(key string, data []*DataPoint) error {
	return client.writeSeries("key", key, data)
}

func (client *Client) writeSeries(series_type string, series_val string, data []*DataPoint) error {
	endpointURL := fmt.Sprintf("/series/%s/%s/data/", series_type, url.QueryEscape(series_val))

	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	URL := client.buildUrl(endpointURL, "", "")
	resp := client.makeRequest(URL, "POST", b)

	statusCode := resp.StatusCode
	if statusCode == http.StatusOK {
		return nil
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return errors.New(string(body))
}

func (client *Client) readSeries(series_type string, series_val string, start time.Time, end time.Time) (*DataSet, error) {
	endpointURL := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(series_val))
	url := client.buildUrl(endpointURL, client.encodeTimes(start, end), "")
	resp := client.makeRequest(url, "GET", []byte{})

	bodyText, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	var dataset DataSet
	err = json.Unmarshal(bodyText, &dataset)
	if err != nil {
		return nil, err
	}

	return &dataset, nil

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

func (client *Client) incrementSeries(series_type string, series_val string, data []*DataPoint) error {
	endpointURL := fmt.Sprintf("/series/%s/%s/increment/?", series_type, url.QueryEscape(series_val))
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	URL := client.buildUrl(endpointURL, "", "")
	_ = client.makeRequest(URL, "POST", b)
	// TODO: Handle non 200

	return nil
}

func (client *Client) DeleteId(id string, start time.Time, end time.Time) error {
	return client.deleteSeries("id", id, start, end)
}

func (client *Client) DeleteKey(key string, start time.Time, end time.Time) error {
	return client.deleteSeries("key", key, start, end)
}

func (client *Client) deleteSeries(series_type string, series_val string, start time.Time, end time.Time) error {
	endpointURL := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(series_val))
	URL := client.buildUrl(endpointURL, client.encodeTimes(start, end), "")
	_ = client.makeRequest(URL, "DELETE", []byte{})

	return nil
}

func (client *Client) WriteBulk(ts time.Time) int {
	return 0
}

func (client *Client) Read(start time.Time, end time.Time, filter Filter) []DataSet {
	URL := client.buildUrl("/data?", client.encodeTimes(start, end), filter.encodeUrl())
	resp := client.makeRequest(URL, "GET", []byte{})
	bodyText, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(bodyText))

	var datasets []DataSet
	err := json.Unmarshal(bodyText, &datasets)
	if err != nil {
		log.Fatal(err)
	}

	return datasets

}

func (client *Client) buildUrl(endpoint string, times string, params_str string) string {
	if times == "" {
		return client.Host + "/v1" + endpoint + params_str
	}

	return client.Host + "/v1" + endpoint + times + "&" + params_str
}

func (client *Client) encodeTimes(start time.Time, end time.Time) string {
	v := url.Values{}
	const layout = "2006-01-02T15:04:05.000-0700"
	start_str := start.Format(layout)
	end_str := end.Format(layout)
	v.Add("start", start_str)
	v.Add("end", end_str)

	return v.Encode()
}

//TODO: add attributes, tags
func (filter *Filter) encodeUrl() string {
	v := url.Values{}
	if len(filter.Ids) != 0 {
		for _, id := range filter.Ids {
			v.Add("id", id)
		}
	}
	if len(filter.Keys) != 0 {
		for _, key := range filter.Keys {
			v.Add("key", key)
		}
	}

	return v.Encode()
}

func (client *Client) makeRequest(builtURL string, method string, formString []byte) *http.Response {
	req, err := http.NewRequest(method, builtURL, bytes.NewReader(formString))
	req.SetBasicAuth(client.Key, client.Secret)
	resp, err := client.Remoter.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	return resp
}
