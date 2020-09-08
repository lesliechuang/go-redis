package Redis

import (
	"github.com/gomodule/redigo/redis"
	"time"
	"fmt"
	"strconv"
	"sync"
)

type SentinelClient struct {
	Addrs []string
	ConnTimeout time.Duration
	MasterName string
	DbNum int
	Master *RedisClient
	Slavers []*RedisClient
}

type SentinelConfig struct {
	//sentinel address
	Addrs []string
	//master instance name
	MasterName string
	//connect database number
	DbNum int
	//connection timeout
	ConnTimeout time.Duration
	//channel of internal log message
	LogChan chan<-string
	//when master change will auto reset sentinel client Master field
	AutoSwitchMaster bool
}

var sentinelPool map[string]*redis.Pool = make(map[string]*redis.Pool);
var sentinelMu sync.Mutex;

//create sentinel client
func NewSentinel(c SentinelConfig) (*SentinelClient,error) {
	client := &SentinelClient {
		Addrs:c.Addrs,
		MasterName:c.MasterName,
		DbNum:c.DbNum,
		ConnTimeout:c.ConnTimeout,
	};

	client.initPool();

	master,err := client.getMaster();
	if err != nil {
		return nil,err;
	}

	slaver,err := client.getSlaver();
	if err != nil {
		return nil,err;
	}

	client.Master=master;
	client.Slavers=slaver;

	if c.AutoSwitchMaster {
		err = client.subSwitchMaster(c.LogChan);
		if err != nil {
			return nil,err;
		}
	}

	return client,nil;
}

//reset sentinel master client
func (c *SentinelClient) ResetMaster() error{
	master,err := c.getMaster();
	if err != nil {
		return err;
	}
	c.Master=master;

	return nil;
}

//reset sentinel slaver clients
func (c *SentinelClient) ResetSlaver() error {
	slavers,err := c.getSlaver();
	if err != nil {
		return err;
	}
	c.Slavers=slavers;

	return nil;
}

//subscribe sentinel channel switch-master
//when master changed,auto reset sentinel master
func (c *SentinelClient) subSwitchMaster(ch chan<-string) error{
	p,err := c.getPool();
	if err != nil {
		return err;
	}

	conn := p.Get();
	psc := redis.PubSubConn{Conn:conn};
	if err := psc.Subscribe("+switch-master") ; err != nil {
		return err;
	}

	go c.receive(psc,ch);

	return nil;
}

//receive sentinel switch-master channel message
func (c *SentinelClient) receive(psc redis.PubSubConn,ch chan<-string) {
	for {
		switch psc.Receive().(type) {
			case error:
				ch <- "receive switch-master error and exit";
				return;
			case redis.Message:
				master,err := c.getMaster();
				if err != nil {
					ch <- "switch new master error and continue listenning";
					continue;
				}
				c.Master=master;
				ch <- "switch master success";
			default:
				ch <- "recv others message";
		}
	}
}

//init sentinel client pool 
func (c *SentinelClient) initPool() {
	for _,a := range c.Addrs {
		if _,ok := sentinelPool[a]; ok {
			continue;
		}

		sentinelMu.Lock();
		if _,ok := sentinelPool[a]; ok {
			sentinelMu.Unlock();
			continue;
		}

		pool := &redis.Pool {
			MaxIdle:3,
			MaxActive:10,
			Dial:func()(redis.Conn,error) {
				c,err := redis.Dial("tcp",a,
				redis.DialConnectTimeout(c.ConnTimeout));
	
				if err != nil {
					return nil,err;
				}
	
				return c,nil;
			},
		}

		sentinelPool[a]=pool;
		sentinelMu.Unlock();
	}
}

//according address select a sentinel pool return
func (c *SentinelClient) getPool() (*redis.Pool,error) {
	for i,a := range c.Addrs {
		if p,ok := sentinelPool[a]; ok {
			if c.Addrs[0] != a {
				c.Addrs[0],c.Addrs[i]=c.Addrs[i],c.Addrs[0];
			}

			return p,nil;
		}
	}

	return nil,fmt.Errorf("no find sentinel");
}

//return master client
func (c *SentinelClient) getMaster() (*RedisClient,error) {
	pool,err := c.getPool();
	if err != nil {
		return nil,err;
	}

	sentinelConn := pool.Get();
	defer sentinelConn.Close();

	addrRes,err := redis.Strings(sentinelConn.Do("SENTINEL","get-master-addr-by-name",c.MasterName));
	if err != nil {
		return nil,err;
	}

	if len(addrRes) < 2 {
		return nil,fmt.Errorf("not find master instance");
	}

	port,_ := strconv.Atoi(addrRes[1]);

	config := &Config{
		Ip:addrRes[0],
		Port: port,
		Password:"",
		ReadTimeout:3*time.Second,
		ConnTimeout:10*time.Second,
		DbNum:c.DbNum,
	}

	master,err := NewClient(config);
	if err != nil {
		return nil,err;
	}

	return master,nil;
}

//return slaver client
func (c *SentinelClient) getSlaver() ([]*RedisClient,error) {
	pool,err := c.getPool();
	if err != nil {
		return nil,err;
	}

	sentinelConn := pool.Get();
	defer sentinelConn.Close();

	addrRes,err := redis.Values(sentinelConn.Do("SENTINEL","slaves",c.MasterName));
	if err != nil {
		return nil,err;
	}

	res := make([]*RedisClient,len(addrRes));

	for i,v := range addrRes {
		sm,err := redis.StringMap(v,err);
		if err != nil {
			return nil,err;
		}

		port,_ := strconv.Atoi(sm["port"]);

		config := &Config{
			Ip:sm["ip"],
			Port: port,
			Password:"",
			ReadTimeout:3*time.Second,
			ConnTimeout:10*time.Second,
			DbNum:c.DbNum,
		}
	
		slave,err := NewClient(config);
		if err != nil {
			return nil,err;
		}

		res[i] = slave;
	}

	return res,nil;
}