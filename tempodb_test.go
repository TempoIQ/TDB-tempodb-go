package tempodb

import (
	"encoding/json"
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
	client := NewClient()
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
	series, err := client.GetSeries(&Filter{})
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

func TestWriteKey(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       makeBody(""),
	}

	client, remoter := NewTestClient(resp)
	datapoints := []*DataPoint{
		&DataPoint{
			Ts: &TempoTime{Time: time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)},
			V:  1.23,
		},
		&DataPoint{
			Ts: &TempoTime{Time: time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)},
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
			Ts: &TempoTime{Time: time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)},
			V:  1.23,
		},
		&DataPoint{
			Ts: &TempoTime{Time: time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)},
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
