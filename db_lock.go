package dglock

import (
	daogext "github.com/darwinOrg/daog-ext"
	dgctx "github.com/darwinOrg/go-common/context"
	dgsys "github.com/darwinOrg/go-common/sys"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/rolandhe/daog"
	"time"
)

type shedlock struct {
	Name      string
	LockUntil time.Time
	LockedAt  time.Time
	LockedBy  string
}

type DbLocker struct {
	db  daog.Datasource
	dao daog.QuickDao[shedlock]
}

func NewDbLocker(db daog.Datasource) *DbLocker {
	return NewDbLockerWithTableName(db, "shedlock")
}

func NewDbLockerWithTableName(db daog.Datasource, tableName string) *DbLocker {
	return &DbLocker{db: db, dao: daog.NewBaseQuickDao(buildShedlockMeta(tableName))}
}

func (l *DbLocker) DoLock(ctx *dgctx.DgContext, name string, lockMilli int64) bool {
	time.Sleep(time.Microsecond * time.Duration(utils.RandomIntInRange(1000, 3000)))
	var result bool

	_ = daogext.Write(ctx, func(tc *daog.TransContext) error {
		tc.LogSQL = false
		if l.exists(ctx, tc, name) {
			result = l.update(ctx, tc, name, lockMilli)
		} else {
			result = l.insert(tc, name, lockMilli)
		}

		return nil
	})

	return result
}

func (l *DbLocker) Unlock(ctx *dgctx.DgContext, name string) bool {
	now := time.Now()
	modifier := daog.NewModifier()
	modifier.Add(shedlockFields.LockUntil, now)

	matcher := daog.NewMatcher()
	matcher.Eq(shedlockFields.Name, name)
	matcher.Gt(shedlockFields.LockUntil, now)

	result, _ := daogext.WriteWithResult(ctx, func(tc *daog.TransContext) (bool, error) {
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
	matcher := daog.NewMatcher()
	matcher.Eq(shedlockFields.Name, name)

	result, _ := daogext.WriteWithResult(ctx, func(tc *daog.TransContext) (bool, error) {
		tc.LogSQL = false
		cnt, err := l.dao.DeleteByMatcher(tc, matcher)
		if err != nil {
			dglogger.Errorf(ctx, "shedlock delock error: %v | name: %s", err, name)
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
	start := time.Now()
	action()
	dglogger.Infof(ctx, "finish do action in lock section, spend %s", time.Since(start))

	return true
}

func (l *DbLocker) exists(ctx *dgctx.DgContext, tc *daog.TransContext, name string) bool {
	cnt, err := l.dao.Count(tc, daog.NewMatcher().Eq(shedlockFields.Name, name))
	if err != nil {
		dglogger.Errorf(ctx, "shedlock count error: %v | name: %s", err, name)
	}
	return cnt > 0
}

func (l *DbLocker) insert(tc *daog.TransContext, name string, lockMilli int64) bool {
	_, err := l.dao.Insert(tc, &shedlock{
		Name:      name,
		LockUntil: time.Now().Add(time.Duration(lockMilli) * time.Millisecond),
		LockedAt:  time.Now(),
		LockedBy:  dgsys.GetHostName(),
	})
	if err != nil {
		return false
	}

	return true
}

func (l *DbLocker) update(ctx *dgctx.DgContext, tc *daog.TransContext, name string, lockMilli int64) bool {
	now := time.Now()
	modifier := daog.NewModifier()
	modifier.Add(shedlockFields.LockedAt, now)
	modifier.Add(shedlockFields.LockedBy, dgsys.GetHostName())
	modifier.Add(shedlockFields.LockUntil, now.Add(time.Duration(lockMilli)*time.Millisecond))

	matcher := daog.NewMatcher()
	matcher.Eq(shedlockFields.Name, name)
	matcher.Lte(shedlockFields.LockUntil, now)

	tc.LogSQL = false
	cnt, err := l.dao.UpdateByModifier(tc, modifier, matcher)
	if err != nil {
		dglogger.Errorf(ctx, "shedlock update error: %v | name: %s", err, name)
		return false
	}

	return cnt > 0
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

func buildShedlockMeta(table string) *daog.TableMeta[shedlock] {
	if table == "" {
		table = "shedlock"
	}

	return &daog.TableMeta[shedlock]{
		Table: table,
		Columns: []string{
			shedlockFields.Name,
			shedlockFields.LockUntil,
			shedlockFields.LockedAt,
			shedlockFields.LockedBy,
		},
		LookupFieldFunc: func(columnName string, ins *shedlock, point bool) any {
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
