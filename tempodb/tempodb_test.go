package tempodb 

import (
    "testing" //import go package for testing related functionality
    
    )

func TestClientInit(t *testing.T) { 
    
	var client Client = *NewClient()

    if (client.Host != "http://api.tempo-db.com") { 
        t.Error("Incorrect Host") 
    }

    if (client.Port != 443) { 
        t.Error("Incorrect Port") 
    } 
}
