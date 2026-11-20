package user

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalidUsername    = errors.New("用户名须为 3–32 位 ASCII 字母、数字或下划线，并以字母开头")
	ErrInvalidPassword    = errors.New("密码须为 12–128 个字符，且不超过 512 个 UTF-8 字节")
	ErrInvalidCredentials = errors.New("用户名或密码不正确")
	ErrCapacity           = errors.New("当前登录或注册请求较多，请稍后再试")
	ErrUnavailable        = errors.New("数据库暂不可用")
	usernamePattern       = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{2,31}$`)
)

type Service struct {
	repository *Repository
	passwords  chan struct{}
	dummyHash  string
}

func NewService(repository *Repository) (*Service, error) {
	// 不存在的账号也做一次正常密码计算，减少通过响应速度判断账号的差异。
	dummyHash, err := hashPassword("livestream-dummy-login")
	if err != nil {
		return nil, err
	}
	return &Service{repository: repository, passwords: make(chan struct{}, 4), dummyHash: dummyHash}, nil
}

func normalizeUsername(username string) (string, error) {
	username = strings.Trim(username, " ")
	// 先限制 ASCII，再转小写，避免 Unicode 大小写转换扩大合法字符范围。
	if !usernamePattern.MatchString(username) {
		return "", ErrInvalidUsername
	}
	return strings.ToLower(username), nil
}

func validatePassword(password string) error {
	length := utf8.RuneCountInString(password)
	if !utf8.ValidString(password) || length < 12 || length > 128 || len(password) > 512 {
		return ErrInvalidPassword
	}
	return nil
}

// acquirePasswordSlot 不排队等待；达到计算上限直接让接入层返回 429。
func (s *Service) acquirePasswordSlot(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	select {
	case s.passwords <- struct{}{}:
		return nil
	default:
		return ErrCapacity
	}
}

func (s *Service) Register(ctx context.Context, username, password string) (User, error) {
	name, err := normalizeUsername(username)
	if err != nil {
		return User{}, err
	}
	if err = validatePassword(password); err != nil {
		return User{}, err
	}
	if err = s.acquirePasswordSlot(ctx); err != nil {
		return User{}, err
	}
	defer func() { <-s.passwords }()
	encoded, err := hashPassword(password)
	if err != nil {
		return User{}, err
	}
	account := User{
		Username: name, PasswordHash: encoded,
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	err = s.repository.Create(ctx, &account)
	if errors.Is(err, ErrUsernameExists) {
		return User{}, ErrUsernameExists
	}
	if err != nil {
		return User{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return account, nil
}

func (s *Service) Login(ctx context.Context, username, password string) (User, error) {
	name, err := normalizeUsername(username)
	if err != nil || validatePassword(password) != nil {
		return User{}, ErrInvalidCredentials
	}
	if err = s.acquirePasswordSlot(ctx); err != nil {
		return User{}, err
	}
	defer func() { <-s.passwords }()
	account, err := s.repository.FindByUsername(ctx, name)
	missing := errors.Is(err, ErrNotFound)
	if err != nil && !missing {
		return User{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	encoded := account.PasswordHash
	if missing {
		encoded = s.dummyHash
	}
	matched, err := verifyPassword(password, encoded)
	if err != nil {
		return User{}, err
	}
	if err = ctx.Err(); err != nil {
		return User{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if missing || !matched {
		return User{}, ErrInvalidCredentials
	}
	return account, nil
}

func (s *Service) Current(ctx context.Context, id uint64) (User, error) {
	account, err := s.repository.FindByID(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return account, nil
}
