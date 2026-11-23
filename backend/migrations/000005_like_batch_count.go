package migrations

import "gorm.io/gorm"

// createLikeBatchCountV5 既有记录均是一次点击，默认值也使它们可参加 SUM 汇总。
func createLikeBatchCountV5(db *gorm.DB) error {
	return db.Exec(`ALTER TABLE interaction_events
		ADD COLUMN click_count BIGINT NOT NULL DEFAULT 1,
		ADD CONSTRAINT chk_like_click_count CHECK (click_count > 0)`).Error
}
