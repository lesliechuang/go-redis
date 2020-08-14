package Redis

import (
	"github.com/gomodule/redigo/redis"
	"encoding/json"
	"strconv"
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

func (c *RedisClient) SAdd(key string,val ...string) error {
	args := make([]interface{},len(val)+1);
	args[0] = key;
	for i,v := range val {
		args[i+1] = v;
	} 

	_,err := c.c.Do("SADD",args...);
	return err;
}

func (c *RedisClient) SMembers(key string) ([]string,error) {
	val,err := redis.Strings(c.c.Do("SMEMBERS",key));
	return val,err;
}

func (c *RedisClient) SCard(key string) (int,error) {
	val,err := redis.Int(c.c.Do("SCARD",key));
	return val,err;
}

func (c *RedisClient) SIsmember(key string,val string) (bool,error) {
	result,err := redis.Bool(c.c.Do("SISMEMBER",key,val));
	return result,err;
}

func (c *RedisClient) SInter(keys ...interface{}) ([]string,error) {
	result,err := redis.Strings(c.c.Do("SINTER",keys...));
	return result,err;
}

func (c *RedisClient) SUnion(keys ...interface{}) ([]string,error) {
	result,err := redis.Strings(c.c.Do("SUNION",keys...));
	return result,err;
}

//set

//zset

type ZSetEntity struct {
	Score int
	Value string
}

func (c *RedisClient) ZAdd(key string,val ...ZSetEntity) error {
	args := make([]interface{},len(val)*2+1);
	args[0]=key;
	for i,v := range val {
		args[(i<<1)+1] = v.Score;
		args[(i<<1)+2] = v.Value;
	}

	_,err := c.c.Do("ZADD",args...);
	return err;
}

func (c *RedisClient) ZRange(key string,start int,stop int,withScores bool) ([]ZSetEntity,error) {
	extendFlag := "";
	if withScores {
		extendFlag = "WITHSCORES";
	}

	result,err := redis.Strings(c.c.Do("ZRANGE",key,start,stop,extendFlag));
	if err != nil {
		return nil,err;
	}

	var entitis []ZSetEntity;
	if withScores {
		entitis = make([]ZSetEntity,(len(result)>>1));
		for i:=0;i<(len(result)>>1);i++ {
			score,err := strconv.Atoi(result[(i<<1+1)]);
			if err != nil {
				return nil,err;
			}

			val := result[(i<<1)];
			entitis[i] = ZSetEntity{Score:score,Value:val};
		}
	} else {
		entitis = make([]ZSetEntity,len(result));
		for i,v := range result {
			entitis[i] = ZSetEntity{Value:v,};
		}
	}

	return entitis,nil;
}

func (c *RedisClient) ZRevrange(key string,start int,stop int,withScores bool) ([]ZSetEntity,error) {
	extendFlag := "";
	if withScores {
		extendFlag = "WITHSCORES";
	}

	result,err := redis.Strings(c.c.Do("ZREVRANGE",key,start,stop,extendFlag));
	if err != nil {
		return nil,err;
	}

	var entitis []ZSetEntity;
	if withScores {
		entitis = make([]ZSetEntity,(len(result)>>1));
		for i:=0;i<(len(result)>>1);i++ {
			score,err := strconv.Atoi(result[(i<<1)+1]);
			if err != nil {
				return nil,err;
			}

			val := result[(i<<1)];
			entitis[i] = ZSetEntity{Score:score,Value:val};
		}
	} else {
		entitis = make([]ZSetEntity,len(result));
		for i,v := range result {
			entitis[i] = ZSetEntity{Value:v,};
		}
	}

	return entitis,nil;
}

func (c *RedisClient) ZRank(key string,val string) (int,error) {
	result,err := redis.Int(c.c.Do("ZRANK",key,val));
	return result,err;
}

func (c *RedisClient) ZRevrank(key string,val string) (int,error) {
	result,err := redis.Int(c.c.Do("ZREVRANK",key,val));
	return result,err;
}

func (c *RedisClient) ZScore(key string,val string) (int,error) {
	result,err := redis.Int(c.c.Do("ZSCORE",key,val));
	return result,err;
}

//zset


//script

func (c *RedisClient) Increment(key string,val int) (int,error) {
	script := redis.NewScript(1,`local amount=tonumber(ARGV[1])
	local current
	current = redis.call('INCRBY',KEYS[1],amount)
	current = tonumber(current)
	return current`);

	result,err := redis.Int(script.Do(c.c,key,val));
	return result,err;
}

func (c *RedisClient) CAS(key string,oldVal interface{},newVal interface{}) (bool,error) {
	script := redis.NewScript(1,`
		local oldVal = ARGV[1]
		local newVal = ARGV[2]
		local curVal = redis.call('GET',KEYS[1]);
		if oldVal == curVal 
		then
			redis.call('SET',KEYS[1],newVal)
			return true
		end

		return false
	`);

	result,err := redis.Bool(script.Do(c.c,key,oldVal,newVal));
	return result,err;
}

//script

//automic

//automic