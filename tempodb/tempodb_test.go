package tempodb 

import (
	"testing"
	"net/http"
	"io/ioutil"
	"strings"
)

type MockRemoter struct {
	nextResponse *http.Response
}

func (m *MockRemoter) Do(req *http.Request) (*http.Response, error) {
	return m.nextResponse, nil
}

func NewTestClient(resp *http.Response) *Client {
	client := NewClient()
	client.Remoter = &MockRemoter{resp}
	return client
}

func TestRegexMatching(t *testing.T) {
	client := NewTestClient(&http.Response{})
	_, err := client.CreateSeries("#")
	if err == nil {
		t.Errorf("Should be invalid")
	}
	_, err = client.CreateSeries("validkey")
	if err != nil {
		t.Errorf("Should be valid")
	}

}
func TestCreateSeries(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Status: "200 OK",
		Body: ioutil.NopCloser(strings.NewReader(`{"id":"0e3178aea7964c4cb1a15db1e80e2a7f","key":"key2","name":"","tags":[],"attributes":{}}`)),
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