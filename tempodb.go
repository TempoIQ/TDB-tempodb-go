package tempodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

const (
	API_HOSTNAME = "api.tempo-db.com"
	API_SECURE_PORT = 443
	ISO8601_FMT = "2006-01-02T15:04:05.000Z0700"
)

var (
	//Useful shorcut when you don't want to specify optional Filter parameters.
	NullFilter = NewFilter()
	USER_AGENT = []string{"tempodb-go/0.2"}
)

type Remoter interface {
	Do(*http.Request) (*http.Response, error)
}

//Stores the session information for authenticating and accessing TempoDB. Your api key and secret is required. The Client also allows you to specify the hostname, port, and protocol (http or https). This is used if you are on a private cluster. The default hostname and port should work for the standard cluster.
//All access to data is made through a client instance.
type Client struct {
	Key     string
	Secret  string
	Host    string
	Port    int
	Remoter Remoter
	Secure  bool
}

//Create a new client instance. Must provide your TempoDB API key and secret.
func NewClient(key string, secret string) *Client {
	client := &Client{Key: key, Secret: secret, Host: API_HOSTNAME, Port: API_SECURE_PORT}
	client.Remoter = &http.Client{}
	client.Secure = true
	return client
}

//Gets a list of series filtered by the provided Filter.
func (client *Client) GetSeries(filter *Filter) ([]*Series, error) {
	url := client.buildUrl("/series?", filter.Url().Encode())
	response, err := client.makeRequest(url, "GET", []byte{})
	if err != nil {
		return nil, err
	}
	var series []*Series
	err = json.Unmarshal(response, &series)
	if err != nil {
		return nil, err
	}

	return series, nil
}

//Creates a new series in the database.
func (client *Client) CreateSeries(key string) (*Series, error) {
	cr := &createSeriesRequest{key}
	reqBody, err := json.Marshal(cr)
	if err != nil {
		return nil, err
	}
	url := client.buildUrl("/series/", "")
	response, err := client.makeRequest(url, "POST", reqBody)
	if err != nil {
		return nil, err
	}

	var series Series
	err = json.Unmarshal(response, &series)
	if err != nil {
		return nil, err
	}

	return &series, nil
}

//Deletes a list of series matching the provided Filter
func (client *Client) DeleteSeries(filter *Filter) (*DeleteSummary, error) {
	url := client.buildUrl("/series?", filter.Url().Encode())

	return client.deleteSeries(url)
}

//Deletes all series in a database
func (client *Client) DeleteAllSeries() (*DeleteSummary, error) {
	url := client.buildUrl("/series/?allow_truncation=true", "")

	return client.deleteSeries(url)
}

//Updates a Series's metadata.
func (client *Client) UpdateSeries(series *Series) (*Series, error) {
	endpointUrl := fmt.Sprintf("/series/id/%s/", url.QueryEscape(series.Id))
	url := client.buildUrl(endpointUrl, "")
	b, err := json.Marshal(series)
	if err != nil {
		return nil, err
	}
	response, err := client.makeRequest(url, "PUT", b)
	if err != nil {
		return nil, err
	}
	var responseSeries Series
	err = json.Unmarshal(response, &responseSeries)
	if err != nil {
		return nil, err
	}

	return &responseSeries, nil
}

//Writes a DataSet by id.
func (client *Client) WriteId(id string, data []*DataPoint) error {
	return client.writeSeries("id", id, data)
}

//Writes a DataSet by key.
func (client *Client) WriteKey(key string, data []*DataPoint) error {
	return client.writeSeries("key", key, data)
}

//Writes a set of datapoints for different series for the same timestamp.
func (client *Client) WriteBulk(ts time.Time, data []BulkPoint) error {
	url := client.buildUrl("/data/", "")
	dataSet := &BulkDataSet{
		Ts:   ts,
		Data: data,
	}
	b, err := json.Marshal(dataSet)
	if err != nil {
		return err
	}
	_, err = client.makeRequest(url, "POST", b)
	return err
}

//Reads a list of DataSet by the provided filter and rolluped by the interval
func (client *Client) Read(start time.Time, end time.Time, filter *Filter, readOpts *ReadOptions) ([]*DataSet, error) {
	url := client.buildUrl("/data?", urlMerge(client.encodeTimes(start, end), filter.Url(), readOpts.Url()).Encode())
	response, err := client.makeRequest(url, "GET", []byte{})
	var datasets []*DataSet
	err = json.Unmarshal(response, &datasets)
	if err != nil {
		return nil, err
	}

	return datasets, nil
}

//Reads a DataSet by key.
func (client *Client) ReadKey(key string, start time.Time, end time.Time, readOpts *ReadOptions) (*DataSet, error) {
	return client.readSeries("key", key, start, end, readOpts)
}

//Reads a DataSet by id.
func (client *Client) ReadId(id string, start time.Time, end time.Time, readOpts *ReadOptions) (*DataSet, error) {
	return client.readSeries("id", id, start, end, readOpts)
}

//Increments a DataSet by id.
func (client *Client) IncrementId(id string, data []*DataPoint) error {
	return client.incrementSeries("id", id, data)
}

//Increments a DataSet by key.
func (client *Client) IncrementKey(key string, data []*DataPoint) error {
	return client.incrementSeries("key", key, data)
}

//Increments a set of datapoints for different series for the same timestamp.
func (client *Client) IncrementBulk(ts time.Time, data []BulkPoint) error {
	url := client.buildUrl("/increment/", "")
	dataSet := &BulkDataSet{
		Ts:   ts,
		Data: data,
	}
	b, err := json.Marshal(dataSet)
	if err != nil {
		return err
	}
	_, err = client.makeRequest(url, "POST", b)
	return err
}

//Deletes a range of data from a series by id.
func (client *Client) DeleteId(id string, start time.Time, end time.Time) error {
	return client.deleteDataFromSeries("id", id, start, end)
}

//Deletes a range of data from a series by key.
func (client *Client) DeleteKey(key string, start time.Time, end time.Time) error {
	return client.deleteDataFromSeries("key", key, start, end)
}

func (client *Client) readSeries(series_type string, seriesVal string, start time.Time, end time.Time, readOpts *ReadOptions) (*DataSet, error) {
	endpointUrl := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(seriesVal))
	url := client.buildUrl(endpointUrl, urlMerge(client.encodeTimes(start, end), readOpts.Url()).Encode())
	response, err := client.makeRequest(url, "GET", []byte{})
	var dataset DataSet
	err = json.Unmarshal(response, &dataset)
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
	url := client.buildUrl(endpointUrl, "")
	_, err = client.makeRequest(url, "POST", b)
	return err
}

func (client *Client) incrementSeries(seriesType string, seriesVal string, data []*DataPoint) error {
	endpointUrl := fmt.Sprintf("/series/%s/%s/increment/?", seriesType, url.QueryEscape(seriesVal))
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	url := client.buildUrl(endpointUrl, "")
	_, err = client.makeRequest(url, "POST", b)
	return err
}

func (client *Client) deleteSeries(url string) (*DeleteSummary, error) {
	response, err := client.makeRequest(url, "DELETE", []byte{})
	var summary DeleteSummary
	err = json.Unmarshal(response, &summary)
	if err != nil {
		return nil, err
	}

	return &summary, nil
}

func (client *Client) deleteDataFromSeries(series_type string, seriesVal string, start time.Time, end time.Time) error {
	endpointUrl := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(seriesVal))
	url := client.buildUrl(endpointUrl, client.encodeTimes(start, end).Encode())
	_, err := client.makeRequest(url, "DELETE", []byte{})
	return err
}

func (client *Client) buildUrl(endpoint string, paramsStr string) string {
	var proto string
	if client.Secure {
		proto = "https://"
	} else {
		proto = "http://"
	}

	return proto + client.Host + "/v1" + endpoint + paramsStr
}

func (client *Client) encodeTimes(start time.Time, end time.Time) url.Values {
	v := url.Values{}
	startStr := start.Format(ISO8601_FMT)
	endStr := end.Format(ISO8601_FMT)
	v.Add("start", startStr)
	v.Add("end", endStr)

	return v
}

func (client *Client) makeRequest(url, method string, data []byte) ([]byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(client.Key, client.Secret)
	req.Header["User-Agent"] = USER_AGENT
	resp, err := client.Remoter.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.Status, body)
	}
	return body, nil
}

func httpError(status string, body []byte) error {
	length := len(body)
	if length == 0 || length == 1 {
		return errors.New(status)
	}

	return errors.New(fmt.Sprintf("%s: %s", status, string(body)))
}
