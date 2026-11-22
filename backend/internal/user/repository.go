package user

import (
	"context"
	"errors"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// User 对应已有 users 表；密码哈希永远不直接序列化为 HTTP 响应。
type User struct {
	ID           uint64 `gorm:"primaryKey"`
	Username     string
	PasswordHash string    `json:"-"`
	CreatedAt    time.Time `gorm:"autoCreateTime:false"`
}

func (User) TableName() string { return "users" }

var (
	ErrNotFound       = errors.New("用户不存在")
	ErrUsernameExists = errors.New("用户名已被使用")
)

// Repository 只处理持久化，使用启动入口传入的 GORM 实例。
type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, account *User) error {
	err := r.db.WithContext(ctx).Create(account).Error
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		// 并发注册同名账号时，以数据库唯一约束为最终依据。
		return ErrUsernameExists
	}
	return err
}

func (r *Repository) FindByUsername(ctx context.Context, username string) (User, error) {
	var account User
	err := r.db.WithContext(ctx).Where("username = ?", username).Take(&account).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return User{}, ErrNotFound
	}
	return account, err
}

func (r *Repository) FindByID(ctx context.Context, id uint64) (User, error) {
	var account User
	// 当前用户查询只需要公开字段，不读取密码哈希。
	query := r.db.WithContext(ctx).Select("id", "username", "created_at")
	err := query.Where("id = ?", id).Take(&account).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return User{}, ErrNotFound
	}
	return account, err
}
