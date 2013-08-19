# TempoDB Golang API Client

The TempoDB Golang API Client makes calls to the [TempoDB API](http://tempo-db.com/api/).

# Install

``
go get github.com/tempodb/tempodb-go
``

# Basic Usage

```go
package main

import (
       "log"

       "github.com/tempodb/tempodb-go"
)

func main() {
         client := tempodb.NewClient("api-key", "api-secret")
         created, err := client.CreateSeries("my-series")
         if err != nil {
               log.Fatal(err)  
         }
         log.Println(created)
}
```

# API Docs

A full list of API docs can be found [On Godoc](http://godoc.org/github.com/tempodb/tempodb-go).
