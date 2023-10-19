package dglock

import dgctx "github.com/darwinOrg/go-common/context"

type Locker interface {
	DoLock(ctx *dgctx.DgContext, name string, lockMilli int64) bool
	Unlock(ctx *dgctx.DgContext, name string) bool
	DeLock(ctx *dgctx.DgContext, name string) bool
	OnceLock(ctx *dgctx.DgContext, name string, lockMilli int64, action func()) bool
}
