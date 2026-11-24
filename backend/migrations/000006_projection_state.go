package migrations

import "gorm.io/gorm"

// createProjectionStateV6 只保存成功启用的投影，不记录恢复任务或执行阶段。
func createProjectionStateV6(db *gorm.DB) error {
	return db.Exec(`CREATE TABLE projection_state (
        id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
        epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        topic_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        CONSTRAINT chk_projection_singleton CHECK (id = 1)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`).Error
}
