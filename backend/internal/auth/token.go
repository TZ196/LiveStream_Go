package auth

import (
	"bytes"
	"errors"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const Lifetime = 30 * time.Minute

var (
	ErrUnauthorized = errors.New("登录凭证无效，请重新登录")
	ErrTokenExpired = errors.New("登录已过期，请重新登录")
)

type Identity struct {
	UserID    uint64
	ExpiresAt time.Time
}

// Tokens 只签发和验证令牌，不依赖 Hertz 或数据库。
type Tokens struct {
	key []byte
}

func NewTokens(key []byte) *Tokens { return &Tokens{key: bytes.Clone(key)} }

func (t *Tokens) Issue(userID uint64) (string, error) {
	now := time.Now().UTC().Truncate(time.Second)
	claims := jwt.RegisteredClaims{
		Issuer: "livestream-api", Audience: jwt.ClaimStrings{"livestream-browser"},
		Subject:  strconv.FormatUint(userID, 10),
		IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(Lifetime)),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(t.key)
}

func (t *Tokens) Parse(encoded string) (Identity, error) {
	if len(encoded) == 0 || len(encoded) > 4096 {
		return Identity{}, ErrUnauthorized
	}
	claims := &jwt.RegisteredClaims{}
	// JWT 库要求回调提供验签密钥；算法由 WithValidMethods 固定，不信任客户端选择。
	keyFunc := func(token *jwt.Token) (any, error) { return t.key, nil }
	token, err := jwt.ParseWithClaims(encoded, claims, keyFunc,
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer("livestream-api"), jwt.WithAudience("livestream-browser"),
		jwt.WithExpirationRequired(), jwt.WithIssuedAt(),
		jwt.WithLeeway(30*time.Second), jwt.WithStrictDecoding(),
	)
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return Identity{}, ErrTokenExpired
		}
		return Identity{}, ErrUnauthorized
	}
	if !token.Valid || claims.IssuedAt == nil || claims.ExpiresAt == nil {
		return Identity{}, ErrUnauthorized
	}
	// sub 只能是无前导零的正整数，不能从请求参数替换用户身份。
	id, err := strconv.ParseUint(claims.Subject, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != claims.Subject {
		return Identity{}, ErrUnauthorized
	}
	duration := claims.ExpiresAt.Sub(claims.IssuedAt.Time)
	if duration <= 0 || duration > Lifetime {
		return Identity{}, ErrUnauthorized
	}
	return Identity{UserID: id, ExpiresAt: claims.ExpiresAt.Time}, nil
}
