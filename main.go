package main

import (

        //"encoding/json"
        
)

func main() {
    /*
   b := []byte(`[{"series":{"id":"cd96daa0ebd2429fbac960397dfca4c1","key":"demo-series","name":"","tags":["foo"],"attributes":{}},"start":"2013-02-24T00:00:00.000Z","end":"2013-02-24T00:02:00.000Z","data":[{"t":"2013-02-24T00:00:00.000Z","v":85.94792761803464},{"t":"2013-02-24T00:01:00.000Z","v":95.87281892782114}],"rollup":null,"summary":{"sum":181.8207465458558,"mean":90.9103732729279,"max":95.87281892782114,"min":85.94792761803464,"stddev":7.0179579476894665,"ss":49.25173375553775,"count":2}}]`) 
   var dset []tempodb.DataSet
   json.Unmarshal(b,&dset)

   fmt.Println(dset[0].Series)
    fmt.Println(dset[0].Start)
    fmt.Println(dset[0].End)
    fmt.Println(dset[0].Data)
    fmt.Println(dset[0].Summary)
    */
  //GetSeriesTest()
  //CreateSeriesTest()
  //WriteKey()
 //Read()
//ReadKey()
    //ReadId()
   //DeleteKey()
}

/*
func IncrementKey(){
    API_KEY_dev := "a755539a9e124278b04f988d39bc5ef9"
    API_SECRET_dev := "43f97dc4dbbc46499bd6694a3455210c"
    client := tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev

    key := "demo-series"

    dp1 := tempodb.DataPoint{Ts:  time.Now(),
                            V: 1.4}
    dp2 := tempodb.DataPoint{Ts:  time.Now(),
                        V: 2.8}                                                
    data := []tempodb.DataPoint{dp1, dp2} 

    client.IncrementKey(key, data)
        

}


func DeleteKey(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev

    start_time := time.Date(2013, time.February, 24, 0, 2, 0, 0, time.UTC)
    end_time   := time.Date(2013, time.February, 24, 0, 4, 0, 0, time.UTC)

    key := "demo-series"


    client.DeleteKey(key, start_time, end_time)
  

}

func ReadId(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev

    start_time :=  time.Date(2013, time.February, 24, 0, 0, 0, 0, time.UTC)
    end_time := time.Date(2013, time.February, 24, 0, 2, 0, 0, time.UTC)
    id := "ef4e1e5eeb0f4db5ba4daf1d313c231a"
    dset := client.ReadId(id, start_time, end_time)
    fmt.Println(dset.Series)
    fmt.Println(dset.Summary)
    fmt.Println(dset.Start)
    fmt.Println(dset.End)
    fmt.Println(len(dset.Data))    

}


func ReadKey(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev

    start_time :=  time.Date(2013, time.February, 24, 0, 3, 0, 0, time.UTC)
    end_time := time.Date(2013, time.February, 24, 0, 4, 0, 0, time.UTC)

    key := "demo-series"


    dset := client.ReadKey(key, start_time, end_time)
    
    fmt.Println(dset.Series)
    fmt.Println(dset.Summary)
    fmt.Println(dset.Start)
    fmt.Println(dset.End)
    fmt.Println(len(dset.Data))    

}

func Read(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev

    start_time :=  time.Date(2013, time.February, 24, 0, 0, 0, 0, time.UTC)
    end_time := time.Date(2013, time.February, 24, 0, 2, 0, 0, time.UTC)

    filter  := tempodb.Filter{}
    //filter.AddKey("demo-series")
    filter.AddId("ef4e1e5eeb0f4db5ba4daf1d313c231a")

    dset := client.Read(start_time, end_time, filter)
    
    fmt.Println(len(dset))
    fmt.Println(dset[0].Series)
    fmt.Println(dset[0].Summary)
    fmt.Println(dset[0].Start)
    fmt.Println(dset[0].End)
    fmt.Println(len(dset[0].Data))

}


func CreateSeriesTest(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"
    
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev
    series := client.CreateSeries("type:test.from:golang.num:7.1")
    fmt.Println(series.Id)
}





func WriteBulk(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"

    dp1 := tempodb.DataPoint{Ts:  time.Now(),
                            V: 1.4}
    dp2 := tempodb.DataPoint{Ts:  time.Now(),
                        V: 2.8}                                                
    data := []tempodb.DataPoint{dp1, dp2} 
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev

    status :=client.WriteKey("test:golang.1", data )
    fmt.Println(status)
}


func WriteKey(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"

    dp1 := tempodb.DataPoint{Ts:  time.Now(),
                            V: 1.4}
    dp2 := tempodb.DataPoint{Ts:  time.Now(),
                        V: 2.8}                                                
    data := []tempodb.DataPoint{dp1, dp2} 
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev

    status := client.WriteKey("test:golang.1", data )

    fmt.Println(status)
}



func GetSeriesTest(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"
    
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev
    filter := tempodb.Filter{}
    filter.AddKey("test:golang2.1")

    series := client.GetSeries(filter)

    for _, value := range series {
        fmt.Println(value)
}
    
}
//  export GOPATH=/vagrant/go_playground

*/