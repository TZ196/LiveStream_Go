package room

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// TestSessionLifecycleIntegration 验证重复开关不重复创建场次，重开创建新场次。
func TestSessionLifecycleIntegration(t *testing.T) {
	dsn := os.Getenv("LIVESTREAM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("未指定隔离数据库")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	owner := uint64(time.Now().UnixNano()%1000000000 + 3000000000)
	if err := db.Exec("INSERT INTO users (id,username,password_hash,created_at) VALUES (?,?,?,?)", owner, fmt.Sprintf("t%d", owner), "test", now).Error; err != nil {
		t.Fatal(err)
	}
	service := NewService(NewRepository(db))
	ctx := context.Background()
	value, err := service.Create(ctx, owner, "测试场次")
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.ActiveSession(ctx, value.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Reopen(ctx, value.ID, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Close(ctx, value.ID, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Close(ctx, value.ID, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Reopen(ctx, value.ID, owner); err != nil {
		t.Fatal(err)
	}
	second, err := service.ActiveSession(ctx, value.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatal("重新开播必须创建新场次")
	}
	var count int64
	if err := db.Table("live_sessions").Where("room_id = ?", value.ID).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("场次数=%d, error=%v", count, err)
	}
	if err := db.Create(&Session{RoomID: value.ID, StartedAt: &now}).Error; err == nil {
		t.Fatal("数据库未拒绝重复活动场次")
	}
}
