package curl

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 5000,
		},
		Timeout: 5 * time.Second,
	}
)

func Get(url string) ([]byte, error) {
	//
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("(%d) %v", resp.StatusCode, string(body))
	}
	return body, nil
}

func Post(url string, bodyType string, body io.Reader) ([]byte, error) {
	//
	resp, err := client.Post(url, bodyType, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return b, fmt.Errorf("(%d) %v", resp.StatusCode, string(b))
	}
	return b, nil
}
