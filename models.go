package tempodb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

type tempoTime struct {
	Time time.Time
}

type dataPoint struct {
	Ts tempoTime `json:"t"`
	V  float64   `json:"v"`
}

//Represents one timestamp/value pair.
type DataPoint struct {
	Ts time.Time
	V  float64
}

type bulkDataSet struct {
	Ts   tempoTime   `json:"t"`
	Data []BulkPoint `json:"data"`
}

//Set of data to send for a bulk write.
type BulkDataSet struct {
	Ts   time.Time
	Data []BulkPoint
}

//Interface representing a datapoint in a bulk write.
type BulkPoint interface {
	GetValue() float64
}

//Represents a datapoint for a series referenced by key.
type BulkKeyPoint struct {
	Key string  `json:"key"`
	V   float64 `json:"v"`
}

//Represents a datapoint for a series referenced by id.
type BulkIdPoint struct {
	Id string  `json:"id"`
	V  float64 `json:"v"`
}

type createSeriesRequest struct {
	Key string `json:"key"`
}

type dataSet struct {
	Series  Series             `json:"series"`
	Start   tempoTime          `json:"start"`
	End     tempoTime          `json:"end"`
	Data    []*DataPoint       `json:"data"`
	Summary map[string]float64 `json:"summary"`
}

//Respresents data from a time range of a series.
type DataSet struct {
	Series  Series
	Start   time.Time
	End     time.Time
	Data    []*DataPoint
	Summary map[string]float64
}

//Respresents metadata associated with the series.
type Series struct {
	Id         string            `json:"id"`
	Key        string            `json:"key"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes"`
	Tags       []string          `json:"tags"`
}

//Represents a filter on the set of Series.
type Filter struct {
	ids        []string
	keys       []string
	tags       []string
	attributes map[string]string
}

//Represents optional arguments for read operations. When not desired, use 'NullReadOptions' constant.
type ReadOptions struct {
	Function string
	Interval string
	Tz string
}

//Used to specify no read options.
var (
	NullReadOptions = &ReadOptions{}
)

//Call to get an initialized Filter struct
func NewFilter() *Filter {
	return &Filter{
		ids:        make([]string, 0),
		keys:       make([]string, 0),
		tags:       make([]string, 0),
		attributes: make(map[string]string),
	}
}

func (tt tempoTime) MarshalJSON() ([]byte, error) {
	formatted := fmt.Sprintf("\"%s\"", tt.Time.Format(ISO8601_FMT))
	return []byte(formatted), nil
}

func (tt tempoTime) UnmarshalJSON(data []byte) error {
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

func (dp *DataPoint) MarshalJSON() ([]byte, error) {
	ts := tempoTime{Time: dp.Ts}
	pdp := &dataPoint{Ts: ts, V: dp.V}
	return json.Marshal(pdp)
}

func (dp *DataPoint) UnmarshalJSON(data []byte) error {
	pdp := new(dataPoint)
	err := json.Unmarshal(data, pdp)
	if err != nil {
		return err
	}
	dp.Ts = pdp.Ts.Time
	dp.V = pdp.V

	return nil
}

func (bds *BulkDataSet) MarshalJSON() ([]byte, error) {
	ts := tempoTime{Time: bds.Ts}
	pbds := &bulkDataSet{Ts: ts, Data: bds.Data}
	return json.Marshal(pbds)
}

func (bds *BulkDataSet) UnmarshalJSON(data []byte) error {
	pbds := new(bulkDataSet)
	err := json.Unmarshal(data, pbds)
	if err != nil {
		return err
	}
	bds.Ts = pbds.Ts.Time
	bds.Data = pbds.Data

	return nil
}

func (ds *DataSet) MarshalJSON() ([]byte, error) {
	start := tempoTime{Time: ds.Start}
	end := tempoTime{Time: ds.End}
	pds := &dataSet{Start: start, End: end, Data: ds.Data, Series: ds.Series, Summary: ds.Summary}
	return json.Marshal(pds)
}

func (ds *DataSet) UnmarshalJSON(data []byte) error {
	pds := new(dataSet)
	err := json.Unmarshal(data, pds)
	if err != nil {
		return err
	}
	ds.Start = pds.Start.Time
	ds.End = pds.End.Time
	ds.Series = pds.Series
	ds.Data = pds.Data
	ds.Summary = pds.Summary

	return nil
}

//Add an id to the filter query. A filter can contain many ids.
func (filter *Filter) AddId(id string) {
	filter.ids = append(filter.ids, id)
}

//Add a key to the filter query. A filter can contain many keys.
func (filter *Filter) AddKey(key string) {
	filter.keys = append(filter.keys, key)
}

//Add a tag to the filter query. A filter can contain many tags.
func (filter *Filter) AddTag(tag string) {
	filter.tags = append(filter.tags, tag)
}

//Add an attribute to the filter query. A filter can contain many attributes.
func (filter *Filter) AddAttribute(key string, value string) {
	filter.attributes[key] = value
}

func (bp *BulkKeyPoint) GetValue() float64 {
	return bp.V
}

func (bp *BulkIdPoint) GetValue() float64 {
	return bp.V
}

func (readOpts *ReadOptions) Url() url.Values {
	v := url.Values{}
	if readOpts.Interval != "" {
		v.Add("interval", readOpts.Interval)
	}

	if readOpts.Function != "" {
		v.Add("function", readOpts.Function)
	}

	if readOpts.Tz != "" {
		v.Add("tz", readOpts.Tz)
	}

	return v
}

func (filter *Filter) Url() url.Values {
	v := url.Values{}
	for _, id := range filter.ids {
		v.Add("id", id)
	}

	for _, key := range filter.keys {
		v.Add("key", key)
	}

	for _, tag := range filter.tags {
		v.Add("tag", tag)
	}

	for key, value := range filter.attributes {
		v.Add(fmt.Sprintf("attr[%s]", key), value)
	}

	return v
}
