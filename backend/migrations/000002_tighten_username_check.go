package migrations

import "gorm.io/gorm"

func tightenUsernameV2(db *gorm.DB) error {
	// 用 GORM 执行单条 ALTER，保证同名约束原子替换。
	// 分别调用 DropConstraint/CreateConstraint 会在中途失败时留下没有约束的表。
	return db.Exec(`ALTER TABLE users
        DROP CHECK chk_users_username,
        ADD CONSTRAINT chk_users_username CHECK (
            CHAR_LENGTH(username) BETWEEN 3 AND 32
            AND REGEXP_LIKE(username, '^[a-z]', 'c')
            AND NOT REGEXP_LIKE(username, '[^a-z0-9_]', 'c')
        )`).Error
}
