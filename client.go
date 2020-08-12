package Redis

import (
	"github.com/gomodule/redigo/redis"
	"time"
	"fmt"
	"sync"
)

type RedisClient struct {
	c redis.Conn
}

type Config struct {
	Ip string
	Port int
	Password string
	ReadTimeout time.Duration
	ConnTimeout time.Duration
	DbNum int
}

var mux sync.Mutex;
var catch map[string]*redis.Pool;

func NewClient(c *Config) (*RedisClient,error) {
	pool := getPool(c);
	client := &RedisClient{pool.Get()};
	
	return client,nil;
}

func getPool(c *Config) *redis.Pool {
	serve := fmt.Sprintf("%s:%d",c.Ip,c.Port);

	pool,ok := catch[serve];
	if !ok {
		mux.Lock();
		pool,ok = catch[serve];
		if !ok {
			pool = &redis.Pool{
				MaxIdle:3,
				Dial:func()(redis.Conn,error) {
					c,err := redis.Dial("tcp",serve,
					redis.DialPassword(c.Password),
					redis.DialReadTimeout(c.ReadTimeout),
					redis.DialConnectTimeout(c.ConnTimeout),
					redis.DialDatabase(c.DbNum));
		
					if err != nil {
						return nil,err;
					}
		
					return c,nil;
				},
			};
			catch[serve] = pool;
		}

		mux.Unlock();
	}

	return pool;
}