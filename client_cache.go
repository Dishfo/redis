package redis

import "sync"

var (
	globalClientCache sync.Map

	//all client use a cache map
)
