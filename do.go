package Redis

import (
	"github.com/gomodule/redigo/redis"
	"encoding/json"
)

//key

func (c *RedisClient) ExistsKey(key string) (bool,error) {
	result,err := redis.Bool(c.c.Do("Exists",key));
	return result,err;
}

func (c *RedisClient) Del(key string) error {
	_,err := c.c.Do("DEL",key);
	return err;
}

//key

// string

func (c *RedisClient) Set(key string,val string) error {
	_,err := c.c.Do("SET",key,val);
	return err;
}

func (c *RedisClient) SetEx(key string,val string,ex int) error {
	_,err := c.c.Do("SET",key,val,"EX",ex);
	return err;
}

func (c *RedisClient) Get(key string) (string,error) {
	val,err := redis.String(c.c.Do("GET",key));
	return val,err;
}

//string

//object

func (c *RedisClient) SetObj(key string,val interface{}) error {
	b,err := json.Marshal(val);
	if err != nil {
		return err;
	}

	_,err = c.c.Do("SET",key,b);
	return err;
}

func (c *RedisClient) SetObjEx(key string,val interface{},ex int) error {
	b,err := json.Marshal(val);
	if err != nil {
		return err;
	}

	_,err = c.c.Do("SET",key,b,"EX",ex);
	return err;
}

func (c *RedisClient) GetObj(key string,val interface{}) error {
	b,err := redis.Bytes(c.c.Do("GET",key));
	if err != nil {
		return err;
	}

	err = json.Unmarshal(b,val);
	return err;
}

//object

//list

func (c *RedisClient) RPush(key string,val ...string) error {
	args := append(val,key);
	len := len(args);
	temp := args[0];
	args[0] = args[len-1];
	args[len-1] = temp;
	_,err := c.c.Do("RPUSH","");
	return err;
}

//list

//hashtable

//hashtable

//set

//set

//zset

//zset


//script


//script

//automic

//automic