package tempodb

import (
	"net/url"
)

func urlMerge(urls ...url.Values) url.Values {
	v := url.Values{}
	for _, u := range urls {
		for key, values := range u {
			for _, value := range values {
				v.Add(key, value)
			}
		}
	}

	return v
}
