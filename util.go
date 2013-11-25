package tempodb

import (
	"time"
	"net/url"
)

func urlMerge(urls []url.Values) url.Values {
	first := urls[0]
	for _, u := range urls[1:] {
		for key, values := range u {
			for _, value := range values {
				first.Add(key, value)
			}
		}
	}
	return first
}

func encodeTimes(start time.Time, end time.Time) url.Values {
	return url.Values{
		"start": []string{start.Format(ISO8601)},
		"end": []string{end.Format(ISO8601)},
	}
}
