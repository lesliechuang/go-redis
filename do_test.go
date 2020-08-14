package Redis

import (
	"testing"
	"time"
)

var config Config = Config{
	Ip:"10.1.11.132",
	Port:9510,
	Password:"",
	ReadTimeout:3*time.Second,
	ConnTimeout:10*time.Second,
	DbNum:60,
};

func TestZAdd(t *testing.T) {
	client,err := NewClient(&config);
	if err != nil {
		t.Errorf("TestZAdd faild");
	}

	args := []ZSetEntity{
		ZSetEntity{
			Score:10,
			Value:"v1",
		},
		ZSetEntity{
			Score:20,
			Value:"v2",
		},
		ZSetEntity{
			Score:5,
			Value:"v3",
		},
	}

	err = client.ZAdd("myzset",args...);
	if err != nil {
		t.Errorf("TestZAdd faild");
	}
}

func TestZRange(t *testing.T) {
	client,err := NewClient(&config);
	if err != nil {
		t.Errorf("TestZRange faild");
	}

	result,err := client.ZRange("myzset",0,-1,true);
	if err != nil {
		t.Errorf("TestZRange faild");
		t.Log(err);
	}
	t.Log(result);
}

func TestZRank(t *testing.T) {
	client,err := NewClient(&config);
	if err != nil {
		t.Errorf("TestZRank faild");
	}

	result,err := client.ZRank("myzset","v2");
	if err != nil {
		t.Errorf("TestZRank faild");
		t.Log(err);
	}
	t.Log(result);
}

func TestZRevrank(t *testing.T) {
	client,err := NewClient(&config);
	if err != nil {
		t.Errorf("TestZRank faild");
	}

	result,err := client.ZRevrank("myzset","v2");
	if err != nil {
		t.Errorf("TestZRank faild");
		t.Log(err);
	}
	t.Log(result);
}

func TestZScore(t *testing.T) {
	client,err := NewClient(&config);
	if err != nil {
		t.Errorf("TestZScore faild");
	}

	result,err := client.ZScore("myzset","v2");
	if err != nil {
		t.Errorf("TestZScore faild");
		t.Log(err);
	}
	t.Log(result);
}


func TestIncrement(t *testing.T) {
	client,err := NewClient(&config);
	if err != nil {
		t.Errorf("TestIncrement faild");
	}

	result,err := client.Increment("inc1",-4);
	if err != nil {
		t.Errorf("TestIncrement faild");
		t.Log(err);
	}
	t.Log(result);
}

func TestCAS(t *testing.T) {
	client,err := NewClient(&config);
	if err != nil {
		t.Errorf("TestCAS faild");
	}

	result,err := client.CAS("inc1",13,20);
	if err != nil {
		t.Errorf("TestCAS faild");
		t.Log(err);
	}
	t.Log(result);
}