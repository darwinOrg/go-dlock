package dglock

import (
	"time"

	"github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
)

type RedisLocker struct {
	lua bool
}

func NewRedisLocker(lua bool) *RedisLocker {
	return &RedisLocker{
		lua: lua,
	}
}

func (l *RedisLocker) DoLock(ctx *dgctx.DgContext, name string, lockMilli int64) bool {
	ok, err := redisdk.AcquireLock(name, time.Millisecond*time.Duration(lockMilli))
	if err != nil {
		dglogger.Errorf(ctx, "redis acquire lock error: %v | name: %s", err, name)
		return false
	}

	return ok
}

func (l *RedisLocker) Unlock(ctx *dgctx.DgContext, name string) bool {
	if l.lua {
		err := redisdk.ReleaseLock(name)
		if err != nil {
			dglogger.Errorf(ctx, "redis release lock error: %v | name: %s", err, name)
			return false
		}
		return true
	}

	return l.DeLock(ctx, name)
}

func (l *RedisLocker) DeLock(ctx *dgctx.DgContext, name string) bool {
	rt, err := redisdk.Del(name)
	if err != nil {
		dglogger.Errorf(ctx, "redis delete error: %v | name: %s", err, name)
		return false
	}
	return rt > 0
}

func (l *RedisLocker) OnceLock(ctx *dgctx.DgContext, name string, lockMilli int64, action func()) bool {
	if !l.DoLock(ctx, name, lockMilli) {
		dglogger.Errorf(ctx, "redis once do lock fail: %s, %d", name, lockMilli)
		return false
	}

	defer func() {
		if !l.DeLock(ctx, name) {
			dglogger.Errorf(ctx, "redis once de lock fail: %s, %d", name, lockMilli)
		}
	}()

	dglogger.Infof(ctx, "redis begin do action in lock section")
	start := time.Now()
	action()
	dglogger.Infof(ctx, "redis finish do action in lock section, spend %s", time.Since(start))

	return true
}
