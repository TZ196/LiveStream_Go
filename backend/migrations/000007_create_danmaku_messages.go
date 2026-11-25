package migrations

import "gorm.io/gorm"

// createDanmakuV7 只新增弹幕明细；唯一键同时承担持久化去重，不改点赞 Inbox。
func createDanmakuV7(db *gorm.DB) error {
	return db.Exec(`CREATE TABLE danmaku_messages (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        request_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        username VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        room_id BIGINT UNSIGNED NOT NULL,
        session_id BIGINT UNSIGNED NOT NULL,
        content VARCHAR(200) NOT NULL,
        moderation_version CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        created_at DATETIME(3) NOT NULL,
        persisted_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        UNIQUE KEY uq_danmaku_event (event_id),
        UNIQUE KEY uq_danmaku_request (user_id, request_id),
        KEY idx_danmaku_room (room_id, id),
        KEY idx_danmaku_session (room_id, session_id, id),
        CONSTRAINT ck_danmaku_content CHECK (CHAR_LENGTH(content) BETWEEN 1 AND 200),
        CONSTRAINT fk_danmaku_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
        CONSTRAINT fk_danmaku_session FOREIGN KEY (room_id, session_id) REFERENCES live_sessions(room_id, id) ON DELETE RESTRICT ON UPDATE RESTRICT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`).Error
}
