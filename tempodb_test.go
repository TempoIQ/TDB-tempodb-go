package tempodb

import (
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
}

func (m *MockRemoter) Do(req *http.Request) (*http.Response, error) {
	return m.nextResponse, nil
}

func makeBody(body string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(body))
}

func testFixture(name string) string {
	b, _ := ioutil.ReadFile(path.Join(FIXTURE_FOLDER, name))
	return string(b)
}

func NewTestClient(resp *http.Response) *Client {
	client := NewClient()
	client.Remoter = &MockRemoter{resp}
	return client
}

func TestRegexMatching(t *testing.T) {
	client := NewTestClient(&http.Response{StatusCode: 200, Body: makeBody(testFixture("create_series.json"))})
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
		Status:     "200 OK",
		Body:       makeBody(testFixture("create_series.json")),
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

func TestReadKey(t *testing.T) {
	body := makeBody(testFixture("read_id_and_key.json"))
	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       body,
	}

	client := NewTestClient(resp)

	start_time := time.Date(2012, time.January, 1, 0, 0, 0, 0, time.UTC)
	end_time := time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC)
	key := "getting_started"
	dataset, err := client.ReadKey(key, start_time, end_time)

	if err != nil {
		t.Error(err)

		return
	}

	if dataset.Series.Key != key {
		t.Errorf("Expected key to be %s but was %s", key, dataset.Series.Key)
	}

}

func TestWriteKey(t *testing.T) {

}
