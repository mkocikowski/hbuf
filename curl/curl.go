package curl

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 500,
		},
		Timeout: 5 * time.Second,
	}
)

func Do(req *http.Request) ([]byte, error) {
	//
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("(%d) %v", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return b, nil
}

func Get(url string) ([]byte, error) {
	//
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("(%d) %v", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return b, nil
}

func Post(url string, bodyType string, body io.Reader) ([]byte, error) {
	//
	resp, err := client.Post(url, bodyType, body)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return b, fmt.Errorf("(%d) %v", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return b, nil
}
