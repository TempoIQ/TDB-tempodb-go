package tempodb

import ("fmt"
        "time"
        "net/http"
        "net/url"
        "log"
        "encoding/json"
        "io"
        "regexp"
        "bytes"
        "io/ioutil"
        )

type DataPoint struct{
    Ts time.Time
    V float64
}


func (dp *DataPoint) ToJSON() string{
    //TODO: implement an actual JSON encoder instead of just string formatting
    const layout = "2006-01-02T15:04:05.000-0700"
    date_string := dp.Ts.Format(layout)
    thisJson := fmt.Sprintf(`{"t":"%s","v":%v}`, date_string, dp.V)
    return thisJson

}

type DataSet struct {
    Start time.Time
    End time.Time
    Data []DataPoint
    Summary map[string]float64
}

type Series struct{
    Id string
    Key string
    Name string
    Attributes map[string]string
    Tags []string

}

type Client struct{
    Key string
    Secret string
    Host string
    Port int
    HTTPClient http.Client

}

func NewClient() *Client {
    client := &Client{Host: "http://api.tempo-db.com", Port: 443}
    client.HTTPClient = http.Client{}
    return client
}

type Filter struct{
    Ids []string
    Keys []string
    Tags []string
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

func (client *Client) GetSeries(filter Filter) []Series{

    var URL string
    URL = client.buildUrl("/series?", "", filter.encodeUrl())
    resp := client.makeRequest(URL, "GET", "")

    dec := json.NewDecoder(resp.Body)
    var series []Series

    if err := dec.Decode(&series); err == io.EOF {
        fmt.Println("EOF")
    } else if err != nil {
        log.Fatal(err)
    }

    return series
}


func (client *Client) CreateSeries(key string) Series{

    matched, _ := regexp.MatchString(`^[a-zA-Z0-9\.:;\-_/\\ ]*$`, key)

    if matched != false{
        //regex didnt match
    }

    formString := `{"key":"` +  key + `"}"`
    URL := client.buildUrl("/series/", "", "")
    resp := client.makeRequest(URL, "POST", formString)

    bodyText, _ := ioutil.ReadAll(resp.Body)
    fmt.Println(string(bodyText))

    dec := json.NewDecoder(resp.Body)
    var series Series
    if err := dec.Decode(&series); err == io.EOF {
        fmt.Println("EOF")
    } else if err != nil {
        log.Fatal(err)
    }

    return series //returns empty series? look into this
}

func (client *Client) WriteId(id string, data []DataPoint) int{
    statusCode := client.writeSeries("id", id, data)

    return statusCode
}

func (client *Client) WriteKey(key string, data []DataPoint) int{
    statusCode :=client.writeSeries("key", key, data)

    return statusCode
}

func (client *Client) writeSeries(series_type string, series_val string, data []DataPoint) int{

    endpointURL := fmt.Sprintf("/series/%s/%s/data/",series_type, url.QueryEscape(series_val))

    //TODO: Actual Encoder, not just string formatting
    formString := "["
    for i, dp := range data{
        formString = formString + dp.ToJSON()
        if i != len(data) - 1 {
            formString = formString + ","
        }
    }
    formString = formString + "]"


    URL := client.buildUrl(endpointURL, "" , "")
    resp := client.makeRequest(URL, "POST", formString)

    return resp.StatusCode
}

func (client *Client) WriteBulk(ts time.Time) int{
    return 0 
}

func (client *Client) Read(start time.Time, end time.Time, filter Filter) []DataSet{
    
    URL := client.buildUrl("/data?", client.encodeTimes(start, end), filter.encodeUrl())
    resp := client.makeRequest(URL, "GET", "")

    dec := json.NewDecoder(resp.Body)
    var datasets []DataSet
    if err := dec.Decode(&datasets); err == io.EOF {
        fmt.Println("EOF")
    } else if err != nil {
        log.Fatal(err)
    }
    
    return datasets
    
    
}

func (client *Client) buildUrl(endpoint string, times string, params_str string) string{
    if times == ""{
        return client.Host + "/v1" + endpoint + params_str
    }
    return client.Host + "/v1" + endpoint + times + "&" + params_str
    

}

func (client *Client) encodeTimes(start time.Time, end time.Time) string{
    v := url.Values{}
    const layout = "2006-01-02T15:04:05.000-0700"
    start_str := start.Format(layout)
    end_str := end.Format(layout)
    v.Add("start", start_str)
    v.Add("end", end_str)
    return v.Encode()
}

//TODO: add attributes, tags
func (filter *Filter) encodeUrl() string{

    v := url.Values{}
    if len(filter.Ids) != 0 {
        for _, id := range filter.Ids{
        v.Add("id", id)
        }
        }
    if len(filter.Keys) != 0 {
        for _, key := range filter.Keys{
        v.Add("key", key)
        }
    }
    return v.Encode()

}


func (client *Client) makeRequest(builtURL string, method string, formString string) *http. Response{
    req, err := http.NewRequest(method, builtURL, bytes.NewBufferString(formString))
    req.SetBasicAuth(client.Key, client.Secret)
    resp, err := client.HTTPClient.Do(req)
    if err != nil{
        log.Fatal(err)
    }
    
    return resp

}

