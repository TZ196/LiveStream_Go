package room

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalidTitle = errors.New("房间标题去除首尾空白后须为 1–80 个字符")
	ErrInvalidList  = errors.New("房间状态须为 OPEN 或 CLOSED，每页条数须为 1–100")
	ErrForbidden    = errors.New("只有房主可以开播或结束直播")
	ErrUnavailable  = errors.New("数据库暂不可用")
)

type ListOptions struct {
	Status  string
	OwnerID uint64
	Limit   int
	Cursor  string
}

type Service struct {
	repository *Repository
}

func NewService(repository *Repository) *Service { return &Service{repository: repository} }

func (s *Service) Create(ctx context.Context, ownerID uint64, title string) (Room, error) {
	title = strings.TrimSpace(title)
	length := utf8.RuneCountInString(title)
	if !utf8.ValidString(title) || length < 1 || length > 80 {
		return Room{}, ErrInvalidTitle
	}
	room := Room{
		OwnerID:   ownerID,
		Title:     title,
		Status:    Open,
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	if err := s.repository.Create(ctx, &room); err != nil {
		return Room{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return room, nil
}

func (s *Service) Get(ctx context.Context, id uint64) (Room, error) {
	room, err := s.repository.FindByID(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return Room{}, ErrNotFound
	}
	if err != nil {
		return Room{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return room, nil
}

func (s *Service) List(ctx context.Context, options ListOptions) ([]Room, *string, error) {
	if (options.Status != Open && options.Status != Closed) || options.Limit < 1 || options.Limit > 100 {
		return nil, nil, ErrInvalidList
	}
	beforeID, err := decodeCursor(options)
	if err != nil {
		return nil, nil, err
	}
	rooms, err := s.repository.List(ctx, options, beforeID)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(rooms) <= options.Limit {
		return rooms, nil, nil
	}
	rooms = rooms[:options.Limit]
	next, err := encodeCursor(rooms[len(rooms)-1].ID, options)
	if err != nil {
		return nil, nil, err
	}
	return rooms, &next, nil
}

func (s *Service) Close(ctx context.Context, id, userID uint64) (Room, error) {
	room, err := s.Get(ctx, id)
	if err != nil {
		return Room{}, err
	}
	if room.OwnerID != userID {
		return Room{}, ErrForbidden
	}
	room, err = s.repository.SetStatus(ctx, id, userID, Closed)
	if err != nil {
		return Room{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return room, nil
}

func (s *Service) Reopen(ctx context.Context, id, userID uint64) (Room, error) {
	room, err := s.Get(ctx, id)
	if err != nil {
		return Room{}, err
	}
	if room.OwnerID != userID {
		return Room{}, ErrForbidden
	}
	room, err = s.repository.SetStatus(ctx, id, userID, Open)
	if err != nil {
		return Room{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return room, nil
}

// ActiveSession 返回指定开放房间的当前场次。
func (s *Service) ActiveSession(ctx context.Context, roomID uint64) (Session, error) {
	value, err := s.Get(ctx, roomID)
	if err != nil {
		return Session{}, err
	}
	if value.Status != Open {
		return Session{}, errors.New("直播间已关闭")
	}
	return s.repository.ActiveSession(ctx, roomID)
}
