package consul

import (
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/roson9527/crypt/backend"
)

type Client struct {
	client    *api.KV
	waitIndex uint64
	aclToken  string
}

func New(machines []string, acl ...string) (*Client, error) {
	conf := api.DefaultConfig()
	if len(machines) > 0 {
		conf.Address = machines[0]
	}
	client, err := api.NewClient(conf)
	if err != nil {
		return nil, err
	}
	aclToken := ""
	if len(acl) > 0 {
		aclToken = acl[0]
	}
	return &Client{client.KV(), 0, aclToken}, nil
}

func (c *Client) getQueryOpt() *api.QueryOptions {
	var opt *api.QueryOptions
	opt = nil
	if c.aclToken != "" {
		opt = &api.QueryOptions{Token: c.aclToken}
	}
	return opt
}

func (c *Client) getWriteOpt() *api.WriteOptions {
	var opt *api.WriteOptions
	opt = nil
	if c.aclToken != "" {
		opt = &api.WriteOptions{Token: c.aclToken}
	}
	return opt
}

func (c *Client) Get(key string) ([]byte, error) {
	kv, _, err := c.client.Get(key, c.getQueryOpt())
	if err != nil {
		return nil, err
	}
	if kv == nil {
		return nil, fmt.Errorf("key ( %s ) was not found", key)
	}
	return kv.Value, nil
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	pairs, _, err := c.client.List(key, c.getQueryOpt())
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	pairsLen := len(pairs)
	ret := make(backend.KVPairs, pairsLen)
	for i, kv := range pairs {
		ret[i] = &backend.KVPair{Key: kv.Key, Value: kv.Value}
	}
	return ret, nil
}

func (c *Client) Set(key string, value []byte) error {
	key = strings.TrimPrefix(key, "/")
	kv := &api.KVPair{
		Key:   key,
		Value: value,
	}
	_, err := c.client.Put(kv, c.getWriteOpt())
	return err
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	respChan := make(chan *backend.Response, 10)
	go func() {
		for {
			opts := api.QueryOptions{
				WaitIndex: c.waitIndex,
			}
			keypair, meta, err := c.client.Get(key, &opts)
			if keypair == nil && err == nil {
				err = fmt.Errorf("key ( %s ) was not found", key)
			}
			if err != nil {
				respChan <- &backend.Response{Value: nil, Error: err}
				time.Sleep(time.Second * 5)
				continue
			}
			c.waitIndex = meta.LastIndex
			respChan <- &backend.Response{Value: keypair.Value, Error: nil}
		}
	}()
	return respChan
}
