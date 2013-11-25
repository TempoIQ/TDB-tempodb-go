package tempodb

import (
	"fmt"
	"time"
	"net/url"
)

//Wraps time.Time implementing ISO8601 serialization
type TempoTime time.Time

//Serializes a TempoTime into JSON
func (t TempoTime) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(t).Format(ISO8601) + `"`), nil
}

//Deserializes a TempoTime from JSON
func (t *TempoTime) UnmarshalJSON(data []byte) (err error) {
  tt, err := time.Parse(`"` + ISO8601 + `"`, string(data))
  *t = TempoTime(tt)
  return
}

//Represents one timestamp/value pair.
type DataPoint struct {
	Timestamp TempoTime `json:"t"`
	Value float64 `json:"v"`
}

//Set of data to send for a bulk write.
type BulkDataSet struct {
	Timestamp TempoTime `json:"t"`
	Data []BulkPoint `json:"data"`
}

//Interface representing a datapoint in a bulk write.
type BulkPoint interface {
	GetValue() float64
}

//Represents a datapoint for a series referenced by key.
type BulkKeyPoint struct {
	Key string  `json:"key"`
	Value float64 `json:"v"`
}

//Represents a datapoint for a series referenced by id.
type BulkIdPoint struct {
	Id string  `json:"id"`
	Value  float64 `json:"v"`
}

type createSeriesRequest struct {
	Key string `json:"key"`
}

//Respresents data from a time range of a series.
type DataSet struct {
	Series *Series `json:"series"`
	Start TempoTime `json:"start"`
	End TempoTime `json:"end"`
	Data []DataPoint `json:"data"`
	Summary map[string]float64 `json:"summary"`
}

//Respresents metadata associated with the series.
type Series struct {
	Id string `json:"id"`
	Key string `json:"key"`
	Name string `json:"name"`
	Attributes map[string]string `json:"attributes"`
	Tags []string `json:"tags"`
}

//Represents a filter on the set of Series.
type Filter struct {
	ids []string
	keys []string
	tags []string
	attributes map[string]string
}

type DeleteSummary struct {
	Deleted int `json:"deleted"`
}

//Represents optional arguments for read operations. When not desired, use 'NullReadOptions' constant.
type ReadOptions struct {
	Function string
	Interval string
	Timezone string
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

//Add an id to the filter query. A filter can contain many ids.
func (filter *Filter) AddId(id string) *Filter {
	filter.ids = append(filter.ids, id)
	return filter
}

//Add a key to the filter query. A filter can contain many keys.
func (filter *Filter) AddKey(key string) *Filter {
	filter.keys = append(filter.keys, key)
	return filter
}

//Add a tag to the filter query. A filter can contain many tags.
func (filter *Filter) AddTag(tag string) *Filter {
	filter.tags = append(filter.tags, tag)
	return filter
}

//Add an attribute to the filter query. A filter can contain many attributes.
func (filter *Filter) AddAttribute(key string, value string) *Filter {
	filter.attributes[key] = value
	return filter
}

func (bp *BulkKeyPoint) GetValue() float64 {
	return bp.Value
}

func (bp *BulkIdPoint) GetValue() float64 {
	return bp.Value
}

func (readOpts *ReadOptions) Url() url.Values {
	v := url.Values{}
	if readOpts.Interval != "" {
		v.Add("interval", readOpts.Interval)
	}

	if readOpts.Function != "" {
		v.Add("function", readOpts.Function)
	}

	if readOpts.Timezone != "" {
		v.Add("tz", readOpts.Timezone)
	}

	return v
}

func (filter *Filter) Url() url.Values {
	v := url.Values{}

	if len(filter.ids) != 0 {
		v["id"] = filter.ids
	}

	if len(filter.ids) != 0 {
		v["key"] = filter.keys
	}

	if len(filter.ids) != 0 {
		v["tag"] = filter.tags
	}

	for key, value := range filter.attributes {
		v.Add(fmt.Sprintf("attr[%s]", key), value)
	}

	return v
}
