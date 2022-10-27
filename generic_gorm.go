package gorm

import (
	"context"
	"database/sql"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// where M is a gorm model
type DB[M any] struct {
	db      *gorm.DB
	timeout time.Duration
}

func NewDB[M any](db *gorm.DB) DB[M] {
	return DB[M]{db: db.Model(new(M))}
}

func NewDBWithTimeout[M any](db *gorm.DB, timeOut time.Duration) DB[M] {
	return DB[M]{db: db.Model(new(M)), timeout: timeOut}
}

func (container DB[M]) Begin(opts ...*sql.TxOptions) DB[M] {
	container.db = container.db.Begin(opts...)
	return container
}

func (container DB[M]) Commit() error {
	return container.db.Commit().Error
}

func (container DB[M]) RollBack() error {
	return container.db.Rollback().Error
}

func (container DB[M]) Create(ctx context.Context, instances []M) error {
	if container.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, container.timeout)
		defer cancel()
	}

	return container.db.WithContext(ctx).Create(&instances).Error
}

func (container DB[M]) Delete(ctx context.Context, condition M) (rawsAffected int64, err error) {
	if container.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, container.timeout)
		defer cancel()
	}

	res := container.db.WithContext(ctx).Delete(&condition)
	return res.RowsAffected, res.Error
}

func (container DB[M]) Where(condition M) DB[M] {
	container.db = container.db.Where(&condition)
	return container
}

func (container DB[M]) WhereRaw(condition string, args ...interface{}) DB[M] {
	container.db = container.db.Where(condition, args...)
	return container
}

func (container DB[M]) Find(ctx context.Context) (result []M, err error) {
	if container.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, container.timeout)
		defer cancel()
	}

	err = container.db.WithContext(ctx).Find(&result).Error
	return
}

func (container DB[M]) Take(ctx context.Context) (result M, err error) {
	if container.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, container.timeout)
		defer cancel()
	}

	err = container.db.WithContext(ctx).Take(&result).Error
	return
}

func (container DB[M]) Count(ctx context.Context) (result int64, err error) {
	if container.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, container.timeout)
		defer cancel()
	}

	err = container.db.WithContext(ctx).Count(&result).Error
	return
}

func (container DB[M]) Updates(ctx context.Context, instance M) (rowsAffected int64, err error) {
	if container.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, container.timeout)
		defer cancel()
	}

	res := container.db.WithContext(ctx).Updates(&instance)
	return res.RowsAffected, res.Error
}

func (container DB[M]) ForUpdate(opts ...forUpdateOption) DB[M] {
	var opt string
	if parseForUpdateOptions(opts...).NoWait {
		opt = "NOWAIT"
	}
	container.db = container.db.Clauses(clause.Locking{
		Strength: "UPDATE",
		Options:  opt,
	})

	return container
}

type forUpdateOptions struct {
	NoWait bool
}

type forUpdateOption func(*forUpdateOptions)

func NoWait() forUpdateOption {
	return func(fuo *forUpdateOptions) {
		fuo.NoWait = true
	}
}

func parseForUpdateOptions(opts ...forUpdateOption) (res forUpdateOptions) {
	for _, opt := range opts {
		opt(&res)
	}
	return
}

func (container DB[M]) FindForUpdate(ctx context.Context, opts ...forUpdateOption) (result []M, err error) {
	return container.ForUpdate(opts...).Find(ctx)
}

func (container DB[M]) TakeForUpdate(ctx context.Context, opts ...forUpdateOption) (result M, err error) {
	return container.ForUpdate(opts...).Take(ctx)
}

func (container DB[M]) Upsert(ctx context.Context, instances []M, clause clause.OnConflict) error {
	if container.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, container.timeout)
		defer cancel()
	}

	return container.db.WithContext(ctx).Clauses(clause).Create(&instances).Error
}

func (container DB[M]) Joins(query string, args ...interface{}) DB[M] {
	container.db = container.db.Joins(query, args...)
	return container
}

func (container DB[M]) OrderByAscending(columnName string) DB[M] {
	container.db = container.db.Order(columnName)
	return container
}

func (container DB[M]) OrderByDescending(columnName string) DB[M] {
	container.db = container.db.Order(columnName + " desc")
	return container
}

func (container DB[M]) OrderBy(columnName string, order Order) DB[M] {
	if order == Asc {
		return container.OrderByAscending(columnName)
	} else {
		return container.OrderByDescending(columnName)
	}
}

func (container DB[M]) Limit(limit int) DB[M] {
	container.db = container.db.Limit(limit)
	return container
}

func (container DB[M]) Offset(offset int) DB[M] {
	container.db = container.db.Offset(offset)
	return container
}

type Order bool

const (
	Desc Order = true
	Asc  Order = false
)

func (container DB[M]) Scope(f func(*gorm.DB) *gorm.DB) DB[M] {
	container.db = f(container.db)
	return container
}

func (container DB[M]) Error() error {
	return container.db.Error
}
