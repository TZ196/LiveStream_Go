package migrations

import "gorm.io/gorm"

// addDanmakuReceivedAtV9 保留旧事件的未知时间，不用客户端或落库时间伪造。
func addDanmakuReceivedAtV9(db *gorm.DB) error {
	return db.Exec(`ALTER TABLE danmaku_messages ADD COLUMN server_received_at DATETIME(3) NULL`).Error
}
