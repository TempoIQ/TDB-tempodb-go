package tempodb 

import (
	"testing"
	"net/http"
	"io/ioutil"
	"io"
	"strings"
	"time"
)

type MockRemoter struct {
	nextResponse *http.Response
}

func (m *MockRemoter) Do(req *http.Request) (*http.Response, error) {
	return m.nextResponse, nil
}

func makeBody(body string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(body))
}

func NewTestClient(resp *http.Response) *Client {
	client := NewClient()
	client.Remoter = &MockRemoter{resp}
	return client
}

func TestRegexMatching(t *testing.T) {
	client := NewTestClient(&http.Response{StatusCode: 200, Body: makeBody(`{"id":"0e3178aea7964c4cb1a15db1e80e2a7f","key":"validkey","name":"","tags":[],"attributes":{}}`)})
	_, err := client.CreateSeries("#")
	if err == nil {
		t.Errorf("Should be invalid")

		return
	}
	_, err = client.CreateSeries("validkey")
	if err != nil {
		t.Error(err)

		return
	}
}

func TestCreateSeries(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status: "200 OK",
		Body: makeBody(`{"id":"0e3178aea7964c4cb1a15db1e80e2a7f","key":"key2","name":"","tags":[],"attributes":{}}`),
	}
	client := NewTestClient(resp)
	expectedKey := "key2"
	expectedId := "0e3178aea7964c4cb1a15db1e80e2a7f"
	expectedName := ""

	series, err := client.CreateSeries(expectedKey)

	if err != nil {
		t.Error(err)

		return
	}

	if series.Key != expectedKey {
		t.Errorf("Expected key to be %s but was %s", expectedKey, series.Key)
	}
	if series.Id != expectedId {
		t.Errorf("Expected id to be %s but was %s", expectedId, series.Id)
	}
	if series.Name != expectedName {
		t.Errorf("Expected name to be %s but was %s", expectedName, series.Name)
	}
	if len(series.Attributes) != 0 {
		t.Errorf("Expected len to be %s but was %s", 0, len(series.Attributes))
	}
	if len(series.Tags) != 0 {
		t.Errorf("Expected key to be %s but was %s", 0, len(series.Tags))
	}
}

func TestReadKey(t *testing.T){
	    body := makeBody(`{
							"series":{
							"id":"01868c1a2aaf416ea6cd8edd65e7a4b8",
							"key":"key1",
							"name":"",
							"tags":[
							"temp"
							],
							"attributes":{
							"temp":"1"
							}
							},
							"start":"2012-01-01T00:00:00.000+0000",
							"end":"2012-01-02T00:00:00.000+0000",
							"data":[
							{"t":"2012-01-01T00:00:00.000+0000","v":4.00},
							{"t":"2012-01-01T06:00:00.000+0000","v":3.00},
							{"t":"2012-01-01T12:00:00.000+0000","v":2.00},
							{"t":"2012-01-01T18:00:00.000+0000","v":3.00}
							],
							"rollup":{
							"interval":"PT6H",
							"function":"avg",
							"tz":"UTC"
							},
							"summary":{
							"mean":3.00,
							"sum":12.00,
							"min":2.00,
							"max":4.00,
							"stddev":0.8165,
							"ss":2.00,
							"count":4
							}
							}`)

	resp := &http.Response{
		StatusCode: 200,
		Status: "200 OK",
		Body: body,
	}

	client := NewTestClient(resp)

	start_time :=  time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)
    end_time := time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)
	key := "key1"
	dataset, err := client.ReadKey(key, start_time, end_time)

	if err != nil {
		t.Error(err)
		
		return
	}

	if dataset.Series.Key != key {
		t.Errorf("Expected key to be %s but was %s", dataset.Series.Key, key)
	}


}

func TestWriteKey(t *testing.T){
	

}

