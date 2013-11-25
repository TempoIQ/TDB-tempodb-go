package tempodb

import (
	"io"
	"fmt"
	"time"
	"bytes"
	"errors"
	"net/url"
	"net/http"
	"io/ioutil"
	"encoding/json"
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
	var series []*Series
	url := client.buildUrl("/series?", filter.Url().Encode())
	err := client.makeRequest(url, "GET", nil, &series)
	if err != nil {
		return nil, err
	}
	return series, nil
}

//Creates a new series in the database.
func (client *Client) CreateSeries(key string) (*Series, error) {
	input := &createSeriesRequest{key}
	series := new(Series)
	url := client.buildUrl("/series/", "")
	err := client.makeRequest(url, "POST", input, series)
	if err != nil {
		return nil, err
	}
	return series, nil
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
	output := new(Series)
	endpointUrl := fmt.Sprintf("/series/id/%s/", url.QueryEscape(series.Id))
	url := client.buildUrl(endpointUrl, "")
	err := client.makeRequest(url, "PUT", series, output)
	if err != nil {
		return nil, err
	}
	return output, nil
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
	dataSet := &BulkDataSet{
		Ts:   ts,
		Data: data,
	}
	url := client.buildUrl("/data/", "")
	return client.makeRequest(url, "POST", dataSet, nil)
}

//Reads a list of DataSet by the provided filter and rolluped by the interval
func (client *Client) Read(start time.Time, end time.Time, filter *Filter, readOpts *ReadOptions) ([]*DataSet, error) {
	var datasets []*DataSet
	url := client.buildUrl("/data?", urlMerge(client.encodeTimes(start, end), filter.Url(), readOpts.Url()).Encode())
	err := client.makeRequest(url, "GET", nil, &datasets)
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
	dataSet := &BulkDataSet{
		Ts:   ts,
		Data: data,
	}
	url := client.buildUrl("/increment/", "")
	return client.makeRequest(url, "POST", dataSet, nil)
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
	dataset := new(DataSet)
	endpointUrl := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(seriesVal))
	url := client.buildUrl(endpointUrl, urlMerge(client.encodeTimes(start, end), readOpts.Url()).Encode())
	err := client.makeRequest(url, "GET", nil, dataset)
	if err != nil {
		return nil, err
	}
	return dataset, nil
}

func (client *Client) writeSeries(series_type string, seriesVal string, data []*DataPoint) error {
	endpointUrl := fmt.Sprintf("/series/%s/%s/data/", series_type, url.QueryEscape(seriesVal))
	url := client.buildUrl(endpointUrl, "")
	return client.makeRequest(url, "POST", data, nil)
}

func (client *Client) incrementSeries(seriesType string, seriesVal string, data []*DataPoint) error {
	endpointUrl := fmt.Sprintf("/series/%s/%s/increment/?", seriesType, url.QueryEscape(seriesVal))
	url := client.buildUrl(endpointUrl, "")
	return client.makeRequest(url, "POST", data, nil)
}

func (client *Client) deleteSeries(url string) (*DeleteSummary, error) {
	summary := new(DeleteSummary)
	err := client.makeRequest(url, "DELETE", nil, summary)
	if err != nil {
		return nil, err
	}
	return summary, nil
}

func (client *Client) deleteDataFromSeries(series_type string, seriesVal string, start time.Time, end time.Time) error {
	endpointUrl := fmt.Sprintf("/series/%s/%s/data/?", series_type, url.QueryEscape(seriesVal))
	url := client.buildUrl(endpointUrl, client.encodeTimes(start, end).Encode())
	return client.makeRequest(url, "DELETE", nil, nil)
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

func (client *Client) makeRequest(url, method string, input, output interface{}) (error) {
	var postBody io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		postBody = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, url, postBody)
	if err != nil {
		return err
	}
	req.SetBasicAuth(client.Key, client.Secret)
	req.Header["User-Agent"] = USER_AGENT

	resp, err := client.Remoter.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpError(resp.Status, body)
	}
	if output != nil {
		err = json.Unmarshal(body, output)
		if err != nil {
			return err
		}
	}
	return nil
}

func httpError(status string, body []byte) error {
	length := len(body)
	if length == 0 || length == 1 {
		return errors.New(status)
	}

	return errors.New(fmt.Sprintf("%s: %s", status, string(body)))
}
