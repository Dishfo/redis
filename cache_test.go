package redis

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSyncMap(t *testing.T) {
	var m sync.Map
	m.Store("xxx", nil)
	val, exist := m.Load("xxx")
	fmt.Println(val, exist)

	ret, err := net.LookupTXT("baidu.com")
	fmt.Println(err)
	for _, str := range ret {
		fmt.Println("txt  ", str)
	}

}

func TestClientCache(t *testing.T) {

	enableClientCache(&Options{
		Addr: "127.0.0.1:6379",
		OnConnect: func(ctx context.Context, cn *Conn) error {
			cid, _ := cn.ClientID(ctx).Result()
			fmt.Println(cid)
			return nil
		},
		ClientSideOptions: &ClientSideOptions{
			KeyPrefix: []string{"device:"},
		},
	})

	time.Sleep(time.Second)
	opts2 := &Options{
		Addr: "127.0.0.1:6379",
	}
	//enableClientCacheOptions(opts2)
	testCli := NewClient(opts2)

	testCli.AddHook(&NormalHook{})

	err := testCli.Get(context.Background(), "a").Err()
	fmt.Println(err)
	val, err := testCli.Get(context.Background(), "a").Result()
	fmt.Println(err, val)
	testCli.MGet(context.Background(), "a1", "a2", "a3")
	testCli.MGet(context.Background(), "a1", "a2", "a3")
	testCli.Get(context.Background(), "a2")
	testCli.Set(context.Background(), "a1", "qqqqqqqq", 0)
	time.Sleep(time.Second)
	testCli.MGet(context.Background(), "a1", "a2", "a3")
	testCli.MGet(context.Background(), "a1", "a2", "a3")

	testCli.Set(context.TODO(), "device:1", "1231", 0)
	testCli.Set(context.TODO(), "device:2", "1232", 0)
	testCli.MGet(context.TODO(), "device:1", "device:2")
	testCli.MGet(context.TODO(), "device:1", "device:2")
	//testCli.Publish(context.Background(),"TestC","aaxxwadwad")
	//
	//registerListener(testCli)
	select {}

}
