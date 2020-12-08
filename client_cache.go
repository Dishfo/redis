package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	specificKeyInvalidChanel = "__redis__:invalidate"
)

var (
	globalClientCache sync.Map
	//all client use a cache map
	keysListener     *Client
	keysBroadcast    *Client
	listenerID       int64
	initedFlag       sync.WaitGroup
	once             sync.Once
	globalKeyChecker func(key string) bool

	//internal key
	registeredConnection sync.Map //todo eliminate invalid connection
	keySubCtxFlag        struct{}
)

func init() {
	initedFlag.Add(1)
}

func listenID() int64 {
	return atomic.LoadInt64(&listenerID)
}

type ClientSideOptions struct {
	KeyPrefix []string
}

func enableClientCache(opt *Options) {
	once.Do(func() {
		var listenOpt Options
		listenOpt = *opt
		listenOpt.IdleTimeout = -1

		listenOpt.OnConnect = func(ctx context.Context, cn *Conn) error {
			if isListenCtx(ctx) {
				clientId, err := cn.ClientID(ctx).Result()
				if err != nil {
					return err
				}
				atomic.StoreInt64(&listenerID, clientId)
			}

			return nil
		}
		listenOpt.init()
		keysListener = NewClient(&listenOpt)

		go startListenKeysEvent()
		initedFlag.Wait()
		//init broadcast bootstrap
		broadcastOpt := *opt
		broadcastOpt.IdleTimeout = -1
		broadcastOpt.OnConnect = func(ctx context.Context, cn *Conn) error {
			args := []interface{}{
				"CLIENT", "TRACKING", "on", "REDIRECT", listenerID, "BCAST",
			}
			for _, keyPrefix := range broadcastOpt.ClientSideOptions.KeyPrefix {
				args = append(args, "PREFIX", keyPrefix)
			}

			fmt.Println(args)
			cmd := NewCmd(ctx, args...)

			err := cn.Process(ctx, cmd)

			return err
		}

		keysBroadcast = NewClient(&broadcastOpt)
		keysBroadcast.Ping(context.TODO())
		globalKeyChecker = func(key string) bool {
			for _, keyPrefix := range broadcastOpt.ClientSideOptions.KeyPrefix {
				if strings.HasPrefix(key, keyPrefix) {
					return true
				}
			}

			return false
		}
	})
}

// Deprecated:  use other model
func enableClientCacheOptions(opts *Options) {
	oldCallback := opts.OnConnect
	wrappedOnConnectCallback := func(ctx context.Context, cn *Conn) error {
		if oldCallback != nil {
			err := oldCallback(ctx, cn)
			if err != nil {
				return err
			}
		}
		cmd := NewCmd(ctx, "CLIENT", "TRACKING", "on", "REDIRECT", listenerID)

		err := cn.Process(ctx, cmd)
		if err == nil {
			//todo be careful memory leak
			registeredConnection.Store(cn.ClientID(ctx), cn)
			oldOnClose := cn.onClose
			onCloseFunc := func() error {
				var err error
				registeredConnection.Delete(cn.ClientID(ctx))

				if oldOnClose != nil {
					err = oldOnClose()
				}

				return err
			}
			cn.onClose = onCloseFunc

		}

		return err
	}
	//register reconnection callback

	opts.OnConnect = wrappedOnConnectCallback
}

func isListenCtx(ctx context.Context) bool {
	val := ctx.Value(keySubCtxFlag)
	return val != nil
}

func setListenCtxFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, keySubCtxFlag, true)
}

func startListenKeysEvent() {
	ctx := context.Background()
	keysListener.ClientID(ctx)
	pubSub := keysListener.Subscribe(setListenCtxFlag(ctx), specificKeyInvalidChanel)
	//
	//message, err := pubSub.Receive(context.Background())
	//fmt.Println(message)
	//if err != nil {
	//	panic(err)
	//}

	messageC := pubSub.Channel()
	initedFlag.Done()
	for message := range messageC {
		keys := message.PayloadSlice
		for _, key := range keys {
			fmt.Println("key need handle ", key)
			globalClientCache.Delete(key)
		}

	}

}

var (
	keyTemporaryResultBundle struct{}
)

type paddingValue struct{}

type temporaryResultBundle struct {
	key []interface{}
	val []interface{}
}

type NormalHook struct {
}

func (n *NormalHook) BeforeProcess(ctx context.Context, cmd Cmder) (context.Context, error) {
	var (
		err      error = nil
		redisCmd string
	)

	if cmd.Err() != nil {
		return ctx, nil
	}

	redisCmd = cmd.Name()
	if redisCmd != "get" && redisCmd != "mget" {
		return ctx, nil
	}

	switch cmd.(type) {
	case *StringCmd:
		stringCmd := cmd.(*StringCmd)
		key := cmd.Args()[1]
		realKey := key.(string)
		if !globalKeyChecker(realKey) {
			return ctx, nil
		}

		val, loaded := globalClientCache.LoadOrStore(key, paddingValue{})
		if loaded {
			stringCmd.val = val.(string)
			fmt.Println("hit local cache")
			return ctx, ErrSkip
		}
		fmt.Println("not hit local cache")

	case *SliceCmd:
		sliceCmd := cmd.(*SliceCmd)
		bundle := &temporaryResultBundle{}
		keys := cmd.Args()[1:]
		var newArgs = []interface{}{"mget"}
		for _, key := range keys {
			if !globalKeyChecker(key.(string)) {
				newArgs = append(newArgs, key)
				continue
			}

			val, exist := globalClientCache.LoadOrStore(key, paddingValue{})
			if exist {
				bundle.val = append(bundle.val, val)
				bundle.key = append(bundle.key, key)
				fmt.Println("hit ", key)
			} else {
				newArgs = append(newArgs, key)
			}
		}
		if len(newArgs) == 1 {
			err = ErrSkip
		}
		sliceCmd.args = newArgs

		ctx = context.WithValue(ctx, keyTemporaryResultBundle, bundle)
		return ctx, err
	}

	return ctx, err
}

func (n *NormalHook) AfterProcess(ctx context.Context, cmd Cmder) error {

	if cmd.Err() != nil {
		return nil
	}

	redisCmd := cmd.Name()
	if redisCmd != "get" && redisCmd != "mget" {
		return nil
	}

	switch cmd.(type) {
	case *StringCmd:
		key := cmd.Args()[1]
		realKey := key.(string)
		if !globalKeyChecker(realKey) {
			return nil
		}
		res, _ := cmd.(*StringCmd).Result()
		//race condition
		padding, exist := globalClientCache.Load(key)

		if _, ok := padding.(paddingValue); ok && exist {
			globalClientCache.Store(key, res)
		}
	case *SliceCmd:
		sliceCmd := cmd.(*SliceCmd)
		bundleVal := ctx.Value(keyTemporaryResultBundle)
		//
		keys := cmd.Args()[1:]
		for i, key := range keys {
			padding, exist := globalClientCache.Load(key)
			if _, ok := padding.(paddingValue); ok && exist && sliceCmd.val[i] != nil {
				globalClientCache.Store(key, sliceCmd.val[i])
			}
		}

		bound, ok := bundleVal.(*temporaryResultBundle)
		if ok && bound != nil {
			sliceCmd.val = append(sliceCmd.val, bound.val...)
			sliceCmd.args = append(sliceCmd.args, bound.key...)
		}
	}

	return nil
}

func (n *NormalHook) BeforeProcessPipeline(ctx context.Context, cmds []Cmder) (context.Context, error) {
	return ctx, nil
}

func (n *NormalHook) AfterProcessPipeline(ctx context.Context, cmds []Cmder) error {
	return nil
}
