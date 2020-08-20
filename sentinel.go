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
	Slaver []*RedisClient
}

type SentinelConfig struct {
	Addrs []string
	MasterName string
	DbNum int
	ConnTimeout time.Duration
}

var sentinelPool map[string]*redis.Pool = make(map[string]*redis.Pool);
var sentinelMu sync.Mutex;

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
	client.Slaver=slaver;

	return client,nil;
}

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

func (c *SentinelClient) getPool() (*redis.Pool,error) {
	for i,a := range c.Addrs {
		if p,ok := sentinelPool[a]; ok {
			if c.Addrs[0] != a {
				temp := c.Addrs[0];
				c.Addrs[0]=a;
				c.Addrs[i]=temp;
			}

			return p,nil;
		}
	}

	return nil,fmt.Errorf("no find sentinel");
}

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