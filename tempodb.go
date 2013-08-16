package tempodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

type DataPoint struct {
	Ts time.Time
	V  float64
}

type Remoter interface {
	Do(*http.Request) (*http.Response, error)
}

type createSeriesRequest struct {
	Key string
}

func (dp *DataPoint) ToJSON() string {
	//TODO: implement an actual JSON encoder instead of just string formatting
	const layout = "2006-01-02T15:04:05.000-0700"

	date_string := dp.Ts.Format(layout)
	thisJson := fmt.Sprintf(`{"t":"%s","v":%v}`, date_string, dp.V)
	return thisJson

}

type DataSet struct {
	Series  Series
	Start   time.Time
	End     time.Time
	Data    []DataPoint
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

func NewClient() *Client {
	client := &Client{Host: "http://api.tempo-db.com", Port: 443}
	client.Remoter = &http.Client{}
	return client
}

type Filter struct {
	Ids        []string
	Keys       []string
	Tags       []string
	Attributes map[string]string
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

func (client *Client) GetSeries(filter Filter) []Series {

	var URL string
	URL = client.buildUrl("/series?", "", filter.encodeUrl())
	resp := client.makeRequest(URL, "GET", []byte{})

	dec := json.NewDecoder(resp.Body)
	var series []Series

	if err := dec.Decode(&series); err == io.EOF {
		fmt.Println("EOF")
	} else if err != nil {
		log.Fatal(err)
	}

	return series
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

func (client *Client) WriteId(id string, data []DataPoint) error {
	return client.writeSeries("id", id, data)
}

func (client *Client) WriteKey(key string, data []DataPoint) error {
	return client.writeSeries("key", key, data)
}

func (client *Client) writeSeries(series_type string, series_val string, data []DataPoint) error {

	endpointURL := fmt.Sprintf("/series/%s/%s/data/", series_type, url.QueryEscape(series_val))

	//TODO: Actual Encoder, not just string formatting
	formString := "["
	for i, dp := range data {
		formString = formString + dp.ToJSON()
		if i != len(data)-1 {
			formString = formString + ","
		}
	}
	formString = formString + "]"
	fmt.Println(formString)

	URL := client.buildUrl(endpointURL, "", "")
	resp := client.makeRequest(URL, "POST", []byte{})

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
	URL := client.buildUrl(endpointURL, client.encodeTimes(start, end), "")
	resp := client.makeRequest(URL, "GET", []byte{})

	bodyText, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	var dataset DataSet
	jsonErr := json.Unmarshal(bodyText, &dataset)

	if jsonErr != nil {
		return nil, jsonErr
	}

	return &dataset, nil

}

func (client *Client) ReadKey(key string, start time.Time, end time.Time) (*DataSet, error) {
	return client.readSeries("key", key, start, end)

}

func (client *Client) ReadId(id string, start time.Time, end time.Time) (*DataSet, error) {
	return client.readSeries("id", id, start, end)

}

func (client *Client) IncrementId(id string, data []DataPoint) {

	client.incrementSeries("id", id, data)
}

func (client *Client) IncrementKey(key string, data []DataPoint) {

	client.incrementSeries("key", key, data)
}

func (client *Client) incrementSeries(series_type string, series_val string, data []DataPoint) {
	endpointURL := fmt.Sprintf("/series/%s/%s/increment/?", series_type, url.QueryEscape(series_val))

	//TODO: Actual Encoder, not just string formatting
	formString := "["
	for i, dp := range data {
		formString = formString + dp.ToJSON()
		if i != len(data)-1 {
			formString = formString + ","
		}
	}
	formString = formString + "]"
	fmt.Println(formString)

	URL := client.buildUrl(endpointURL, "", "")
	resp := client.makeRequest(URL, "POST", []byte{})
	fmt.Println(resp.StatusCode)
}

func (client *Client) DeleteId(id string, start time.Time, end time.Time) {

	client.deleteSeries("id", id, start, end)
}

func (client *Client) DeleteKey(key string, start time.Time, end time.Time) {
	client.deleteSeries("key", key, start, end)
}

func (client *Client) deleteSeries(series_type string, series_val string, start time.Time, end time.Time) {
	endpointURL := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(series_val))
	URL := client.buildUrl(endpointURL, client.encodeTimes(start, end), "")
	resp := client.makeRequest(URL, "DELETE", []byte{})
	fmt.Println(resp.StatusCode)
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
