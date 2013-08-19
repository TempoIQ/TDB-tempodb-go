package tempodb

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"
)

const (
	FIXTURE_FOLDER = "./test_fixtures"
)

type MockRemoter struct {
	nextResponse *http.Response
	lastRequest  *http.Request
}

func (m *MockRemoter) Do(req *http.Request) (*http.Response, error) {
	m.lastRequest = req
	return m.nextResponse, nil
}

func (m *MockRemoter) LastRequest() *http.Request {
	return m.lastRequest
}

func makeBody(body string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(body))
}

func testFixture(name string) string {
	b, _ := ioutil.ReadFile(path.Join(FIXTURE_FOLDER, name))
	return string(b)
}

func NewTestClient(resp *http.Response) (*Client, *MockRemoter) {
	client := NewClient("key", "secret")
	remoter := &MockRemoter{resp, nil}
	client.Remoter = remoter
	return client, remoter
}

func TestRegexMatching(t *testing.T) {
	client, _ := NewTestClient(&http.Response{StatusCode: 200, Body: makeBody(testFixture("create_series.json"))})
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

func TestGetSeries(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(testFixture("get_series.json")),
	}
	client, _ := NewTestClient(resp)
	series, err := client.GetSeries(NullFilter)
	if err != nil {
		t.Error(err)

		return
	}
	expectedLength := 7
	if len(series) != expectedLength {
		t.Errorf("Expected length to be %d, but was %d", expectedLength, len(series))

		return
	}
}

func TestCreateSeries(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(testFixture("create_series.json")),
	}
	client, _ := NewTestClient(resp)
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

func TestUpdateSeries(t *testing.T) {
	body := makeBody(testFixture("update_series.json"))
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       body,
	}

	client, _ := NewTestClient(resp)
	series := &Series{
		Id:         "0e3178aea7964c4cb1a15db1e80e2a7f",
		Key:        "key2",
		Name:       "my_series",
		Tags:       make([]string, 0),
		Attributes: make(map[string]string),
	}
	responseSeries, err := client.UpdateSeries(series)
	if err != nil {
		t.Error(err)

		return
	}

	s1 := fmt.Sprintf("%s", series)
	s2 := fmt.Sprintf("%s", responseSeries)
	if s1 != s2 {
		t.Errorf("Expected %s to equal %s", s1, s2)

		return
	}
}

func TestReadKey(t *testing.T) {
	body := makeBody(testFixture("read_id_and_key.json"))
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       body,
	}

	client, _ := NewTestClient(resp)

	startTime := time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)
	id := "3c9b4f3a19114a7eb670ff7c4917f315"
	dataset, err := client.ReadId(id, startTime, endTime)

	if err != nil {
		t.Error(err)

		return
	}

	if dataset.Series.Id != id {
		t.Errorf("Expected id to be %s but was %s", id, dataset.Series.Id)
	}

}

func TestReadId(t *testing.T) {
	body := makeBody(testFixture("read_id_and_key.json"))
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       body,
	}

	client, _ := NewTestClient(resp)

	startTime := time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)
	key := "getting_started"
	dataset, err := client.ReadId(key, startTime, endTime)

	if err != nil {
		t.Error(err)

		return
	}

	if dataset.Series.Key != key {
		t.Errorf("Expected key to be %s but was %s", key, dataset.Series.Key)
	}

}

func TestRead(t *testing.T) {
	body := makeBody(testFixture("read.json"))
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       body,
	}

	startTime := time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)

	client, _ := NewTestClient(resp)
	filter := NewFilter()
	filter.AddAttribute("thermostat", "1")
	datasets, err := client.Read(startTime, endTime, filter)
	if err != nil {
		t.Error(err)

		return
	}
	expectedLength := 2
	if len(datasets) != expectedLength {
		t.Errorf("Expected length of datasets to be %d but was %d", expectedLength, len(datasets))

		return
	}
}

func TestWriteKey(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}

	client, remoter := NewTestClient(resp)
	datapoints := []*DataPoint{
		&DataPoint{
			Ts: time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC),
			V:  1.23,
		},
		&DataPoint{
			Ts: time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC),
			V:  3.14,
		},
	}

	err := client.WriteKey("key", datapoints)
	if err != nil {
		t.Error(err)

		return
	}

	req := remoter.LastRequest()
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Error(err)

		return
	}
	var ds []DataSet
	err = json.Unmarshal(b, &ds)
	if err != nil {
		t.Error(err)

		return
	}
}

func TestWriteId(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}

	client, remoter := NewTestClient(resp)
	datapoints := []*DataPoint{
		&DataPoint{
			Ts: time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC),
			V:  1.23,
		},
		&DataPoint{
			Ts: time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC),
			V:  3.14,
		},
	}

	err := client.WriteId("0aeef415ce734b02af5325f6ad977e26", datapoints)
	if err != nil {
		t.Error(err)

		return
	}

	req := remoter.LastRequest()
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Error(err)

		return
	}
	var ds []DataSet
	err = json.Unmarshal(b, &ds)
	if err != nil {
		t.Error(err)

		return
	}
}

func TestIncrementId(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}
	client, _ := NewTestClient(resp)
	datapoints := []*DataPoint{
		&DataPoint{
			Ts: time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC),
			V:  1,
		},
		&DataPoint{
			Ts: time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC),
			V:  3,
		},
	}
	err := client.IncrementId("0aeef415ce734b02af5325f6ad977e26", datapoints)
	if err != nil {
		t.Error(err)

		return
	}
}

func TestIncrementKey(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}
	client, _ := NewTestClient(resp)
	datapoints := []*DataPoint{
		&DataPoint{
			Ts: time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC),
			V:  1,
		},
		&DataPoint{
			Ts: time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC),
			V:  3,
		},
	}
	err := client.IncrementKey("key1", datapoints)
	if err != nil {
		t.Error(err)

		return
	}
}

func TestIncrementBulk(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}

	client, remoter := NewTestClient(resp)
	dataset := []BulkPoint{
		&BulkKeyPoint{
			Key: "your-custom-key",
			V:   1,
		},
		&BulkIdPoint{
			Id: "01868c1a2aaf416ea6cd8edd65e7a4b8",
			V:  4,
		},
	}
	err := client.IncrementBulk(time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC), dataset)
	if err != nil {
		t.Error(err)

		return
	}
	lastRequest := remoter.LastRequest()
	lastBody, err := ioutil.ReadAll(lastRequest.Body)
	if err != nil {
		t.Error(err)

		return
	}
	expectedBody := `{"t":"2012-01-01T00:00:00.000Z","data":[{"key":"your-custom-key","v":1},{"id":"01868c1a2aaf416ea6cd8edd65e7a4b8","v":4}]}`
	if string(lastBody) != expectedBody {
		t.Errorf("Expected body to be %s but was %s", expectedBody, string(lastBody))
	}
}

func TestDeleteKey(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}
	client, _ := NewTestClient(resp)
	startTime := time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)
	err := client.DeleteKey("key1", startTime, endTime)
	if err != nil {
		t.Error(err)

		return
	}
}

func TestDeleteId(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}
	client, _ := NewTestClient(resp)
	startTime := time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)
	err := client.DeleteId("01868c1a2aaf416ea6cd8edd65e7a4b8", startTime, endTime)
	if err != nil {
		t.Error(err)

		return
	}
}

func TestWriteBulk(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}
	client, remoter := NewTestClient(resp)
	dataset := []BulkPoint{
		&BulkKeyPoint{
			Key: "your-custom-key",
			V:   1.23,
		},
		&BulkIdPoint{
			Id: "01868c1a2aaf416ea6cd8edd65e7a4b8",
			V:  3.14,
		},
	}
	err := client.WriteBulk(time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC), dataset)
	if err != nil {
		t.Error(err)

		return
	}
	lastRequest := remoter.LastRequest()
	lastBody, err := ioutil.ReadAll(lastRequest.Body)
	if err != nil {
		t.Error(err)

		return
	}
	expectedBody := `{"t":"2012-01-01T00:00:00.000Z","data":[{"key":"your-custom-key","v":1.23},{"id":"01868c1a2aaf416ea6cd8edd65e7a4b8","v":3.14}]}`
	if string(lastBody) != expectedBody {
		t.Errorf("Expected body to be %s but was %s", expectedBody, string(lastBody))
	}
}

func TestFilterEncoding(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(testFixture("get_series.json")),
	}
	client, remoter := NewTestClient(resp)
	expectedId := "id1"
	expectedKey := "key1"
	expectedTag := "tag1"
	expectedAttribute := "value"
	filter := NewFilter()
	filter.AddId(expectedId)
	filter.AddKey(expectedKey)
	filter.AddTag(expectedTag)
	filter.AddAttribute("key", expectedAttribute)
	_, err := client.GetSeries(filter)
	if err != nil {
		t.Error(err)

		return
	}
	lastRequest := remoter.LastRequest()

	if lastRequest.FormValue("id") != expectedId {
		t.Errorf("Expected id to be %s but was %s", expectedId, lastRequest.FormValue("id"))

		return
	}
	if lastRequest.FormValue("key") != expectedKey {
		t.Errorf("Expected key to be %s but was %s", expectedKey, lastRequest.FormValue("key"))

		return
	}
	if lastRequest.FormValue("tag") != expectedTag {
		t.Errorf("Expected tag to be %s but was %s", expectedTag, lastRequest.FormValue("tag"))

		return
	}

	if lastRequest.FormValue("attr[key]") != expectedAttribute {
		t.Errorf("Expected attribute to be %s but was %s", expectedAttribute, lastRequest.FormValue("attr[key]"))

		return
	}
}
