package user

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

const passwordParameters = "m=19456,t=2,p=1"

var errInvalidPasswordHash = errors.New("密码哈希格式或参数不受支持")

func hashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	_, err := rand.Read(salt)
	if err != nil {
		return "", fmt.Errorf("生成密码盐失败: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, 2, 19456, 1, 32)
	saltText := base64.RawStdEncoding.EncodeToString(salt)
	keyText := base64.RawStdEncoding.EncodeToString(key)
	// 参数、随机盐和哈希一起保存，原始密码不入库。
	encoded := "$argon2id$v=19$" + passwordParameters + "$" + saltText + "$" + keyText
	return encoded, nil
}

func verifyPassword(password, encoded string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v=19" {
		return false, errInvalidPasswordHash
	}
	// 只接受当前已支持的参数，避免异常存量数据触发巨额内存分配。
	if parts[3] != passwordParameters {
		return false, errInvalidPasswordHash
	}
	salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if err != nil || len(salt) != 16 {
		return false, errInvalidPasswordHash
	}
	expected, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if err != nil || len(expected) != 32 {
		return false, errInvalidPasswordHash
	}
	actual := argon2.IDKey([]byte(password), salt, 2, 19456, 1, 32)
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}
