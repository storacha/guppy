package testutil

import "net/url"

// Urls parses strings into url.URLs and panics on error.
func Urls(strs ...string) []url.URL {
	var result []url.URL
	for _, s := range strs {
		u, err := url.Parse(s)
		if err != nil {
			panic(err)
		}
		result = append(result, *u)
	}
	return result
}
