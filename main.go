package main

import (
        "fmt"
       "time"
        "tempodb_go/tempodb"
        
)

func main() {
    
  GetSeriesTest()
  //CreateSeriesTest()
  //WriteKey()
  //Read()

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


func Read(){
    var API_KEY_dev string = "a755539a9e124278b04f988d39bc5ef9"
    var API_SECRET_dev string = "43f97dc4dbbc46499bd6694a3455210c"
    var client tempodb.Client = *tempodb.NewClient()
    client.Key = API_KEY_dev
    client.Secret = API_SECRET_dev

    start_time :=  time.Date(2013, time.February, 24, 0, 0, 0, 0, time.UTC)
    end_time := time.Date(2013, time.February, 25, 0, 0, 0, 0, time.UTC)

    filter  := tempodb.Filter{}
    filter.AddKey("demo-series")
    filter.AddId("ef4e1e5eeb0f4db5ba4daf1d313c231a")

    dset := client.Read(start_time, end_time, filter)
    
    fmt.Println(len(dset))
    fmt.Println(dset[0].Summary)

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

