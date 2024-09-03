package dglock

import (
	daogext "github.com/darwinOrg/daog-ext"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/rolandhe/daog"
	"os"
	"time"
)

type Shedlock struct {
	Name      string
	LockUntil time.Time
	LockedAt  time.Time
	LockedBy  string
}

type DbLocker struct {
	db  daog.Datasource
	dao daog.QuickDao[Shedlock]
}

func NewDbLocker(db daog.Datasource) *DbLocker {
	return NewDbLockerWithTableName(db, "shedlock")
}

func NewDbLockerWithTableName(db daog.Datasource, tableName string) *DbLocker {
	return &DbLocker{db: db, dao: daog.NewBaseQuickDao(buildShedlockMeta(tableName))}
}

func (l *DbLocker) DoLock(ctx *dgctx.DgContext, name string, lockMilli int64) bool {
	return l.insert(ctx, name, lockMilli) || l.update(ctx, name, lockMilli)
}

func (l *DbLocker) insert(ctx *dgctx.DgContext, name string, lockMilli int64) bool {
	result, _ := daogext.WriteWithResult(ctx, func(tc *daog.TransContext) (bool, error) {
		tc.LogSQL = false
		_, err := l.dao.Insert(tc, &Shedlock{
			Name:      name,
			LockUntil: time.Now().Add(time.Duration(lockMilli) * time.Millisecond),
			LockedAt:  time.Now(),
			LockedBy:  localHostName(),
		})
		if err != nil {
			return false, err
		}

		return true, nil
	})

	return result
}

func (l *DbLocker) update(ctx *dgctx.DgContext, name string, lockMilli int64) bool {
	result, _ := daogext.WriteWithResult(ctx, func(tc *daog.TransContext) (bool, error) {
		now := time.Now()
		modifier := daog.NewModifier()
		modifier.Add(shedlockFields.LockedAt, now)
		modifier.Add(shedlockFields.LockedBy, localHostName())
		modifier.Add(shedlockFields.LockUntil, now.Add(time.Duration(lockMilli)*time.Millisecond))

		matcher := daog.NewMatcher()
		matcher.Eq(shedlockFields.Name, name)
		matcher.Lte(shedlockFields.LockUntil, now)

		tc.LogSQL = false
		cnt, err := l.dao.UpdateByModifier(tc, modifier, matcher)
		if err != nil {
			return false, err
		}

		return cnt > 0, nil
	})

	return result
}

func (l *DbLocker) Unlock(ctx *dgctx.DgContext, name string) bool {
	result, _ := daogext.WriteWithResult(ctx, func(tc *daog.TransContext) (bool, error) {
		now := time.Now()
		modifier := daog.NewModifier()
		modifier.Add(shedlockFields.LockUntil, now)

		matcher := daog.NewMatcher()
		matcher.Eq(shedlockFields.Name, name)
		matcher.Gt(shedlockFields.LockUntil, now)

		tc.LogSQL = false
		cnt, err := l.dao.UpdateByModifier(tc, modifier, matcher)
		if err != nil {
			dglogger.Errorln(ctx, err)
			return false, err
		}

		return cnt > 0, nil
	})

	return result
}

func (l *DbLocker) DeLock(ctx *dgctx.DgContext, name string) bool {
	result, _ := daogext.WriteWithResult(ctx, func(tc *daog.TransContext) (bool, error) {
		matcher := daog.NewMatcher()
		matcher.Eq(shedlockFields.Name, name)

		tc.LogSQL = false
		cnt, err := l.dao.DeleteByMatcher(tc, matcher)
		if err != nil {
			dglogger.Errorln(ctx, err)
			return false, err
		}

		return cnt > 0, nil
	})

	return result
}

func (l *DbLocker) OnceLock(ctx *dgctx.DgContext, name string, lockMilli int64, action func()) bool {
	if !l.DoLock(ctx, name, lockMilli) {
		dglogger.Errorf(ctx, "once do lock fail: %s, %d", name, lockMilli)
		return false
	}

	defer func() {
		if !l.DeLock(ctx, name) {
			dglogger.Errorf(ctx, "once de lock fail: %s, %d", name, lockMilli)
		}
	}()

	dglogger.Infof(ctx, "begin do action in lock section")
	start := time.Now().UnixMilli()
	action()
	dglogger.Infof(ctx, "finish do action in lock section, spend %d ms", time.Now().UnixMilli()-start)

	return true
}

var shedlockFields = struct {
	Name      string
	LockUntil string
	LockedAt  string
	LockedBy  string
}{
	"name",
	"lock_until",
	"locked_at",
	"locked_by",
}

func buildShedlockMeta(table string) *daog.TableMeta[Shedlock] {
	if table == "" {
		table = "shedlock"
	}

	return &daog.TableMeta[Shedlock]{
		Table: table,
		Columns: []string{
			shedlockFields.Name,
			shedlockFields.LockUntil,
			shedlockFields.LockedAt,
			shedlockFields.LockedBy,
		},
		LookupFieldFunc: func(columnName string, ins *Shedlock, point bool) any {
			if shedlockFields.Name == columnName {
				if point {
					return &ins.Name
				}
				return ins.Name
			}
			if shedlockFields.LockUntil == columnName {
				if point {
					return &ins.LockUntil
				}
				return ins.LockUntil
			}
			if shedlockFields.LockedAt == columnName {
				if point {
					return &ins.LockedAt
				}
				return ins.LockedAt
			}
			if shedlockFields.LockedBy == columnName {
				if point {
					return &ins.LockedBy
				}
				return ins.LockedBy
			}

			return nil
		},
	}
}

func localHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return hostname
}
