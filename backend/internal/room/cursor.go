package room

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

var ErrInvalidCursor = errors.New("分页游标无效或与当前筛选条件不匹配")

type listCursor struct {
	BeforeID string `json:"before_id"`
	Status   string `json:"status"`
	OwnerID  string `json:"owner_id"`
}

func encodeCursor(id uint64, options ListOptions) (string, error) {
	cursor := listCursor{
		BeforeID: strconv.FormatUint(id, 10),
		Status:   options.Status,
		OwnerID:  strconv.FormatUint(options.OwnerID, 10),
	}
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeCursor(options ListOptions) (uint64, error) {
	if options.Cursor == "" {
		return 0, nil
	}
	if len(options.Cursor) > 1024 {
		return 0, ErrInvalidCursor
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(options.Cursor)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	var cursor listCursor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cursor); err != nil {
		return 0, ErrInvalidCursor
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return 0, ErrInvalidCursor
	}
	// owner=me 绑定认证后的真实用户，不能把另一个用户的筛选游标接着用。
	if cursor.Status != options.Status || cursor.OwnerID != strconv.FormatUint(options.OwnerID, 10) {
		return 0, ErrInvalidCursor
	}
	id, err := strconv.ParseUint(cursor.BeforeID, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != cursor.BeforeID {
		return 0, ErrInvalidCursor
	}
	return id, nil
}
