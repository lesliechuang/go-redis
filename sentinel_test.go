package Redis

import (
	"testing"
	"time"
	"fmt"
)

func TestNewSentinel(t *testing.T) {

	ch := make(chan string,1);
	c:=SentinelConfig{
		Addrs:[]string{"10.1.11.132:20001"},
		MasterName:"Common",
		DbNum:60,
		ConnTimeout:10*time.Second,
		LogChan:ch,
		AutoSwitchMaster:true,
	}

	client,err := NewSentinel(c);
	if err != nil {
		t.Error(err);
	}
	
	fmt.Printf("master is %s\n",client.Master.Addr);
	for _,v := range client.Slavers {
		fmt.Printf("slaver is %s \n",v.Addr);
	}

	for v := range ch {
		fmt.Printf("log chan message is %s \n",v);
	}
}