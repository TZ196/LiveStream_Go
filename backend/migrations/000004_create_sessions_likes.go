package migrations

import (
	"fmt"
	"gorm.io/gorm"
)

// createLikesV4 新建场次与点赞持久化表；不改写已应用的历史迁移。
// 旧开放房间的实际开播时间未知，迁移只建立 started_at 为 NULL 的活动场次。
func createLikesV4(db *gorm.DB) error {
	statements := []string{
		`CREATE TABLE live_sessions (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            room_id BIGINT UNSIGNED NOT NULL,
            started_at DATETIME(3) NULL,
            ended_at DATETIME(3) NULL,
            active_room_id BIGINT UNSIGNED GENERATED ALWAYS AS (CASE WHEN ended_at IS NULL THEN room_id ELSE NULL END) STORED,
            UNIQUE KEY uq_active_room (active_room_id),
            UNIQUE KEY uq_session_room (room_id, id),
            KEY idx_session_room_id (room_id, id),
            CONSTRAINT fk_session_room FOREIGN KEY (room_id) REFERENCES live_rooms(id) ON DELETE RESTRICT ON UPDATE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
		`INSERT INTO live_sessions (room_id, started_at, ended_at) SELECT id, NULL, NULL FROM live_rooms WHERE status = 'OPEN'`,
		`CREATE TABLE consumer_inbox (
            event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
            request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            request_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            event_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            user_id BIGINT UNSIGNED NOT NULL,
            room_id BIGINT UNSIGNED NOT NULL,
            session_id BIGINT UNSIGNED NOT NULL,
            event_time DATETIME(3) NOT NULL,
            event_json JSON NOT NULL,
            UNIQUE KEY uq_inbox_request (user_id, request_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
		`CREATE TABLE interaction_events (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            room_id BIGINT UNSIGNED NOT NULL,
            session_id BIGINT UNSIGNED NOT NULL,
            user_id BIGINT UNSIGNED NOT NULL,
            event_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            created_at DATETIME(3) NOT NULL,
            persisted_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
            UNIQUE KEY uq_interaction_event (event_id),
            KEY idx_interaction_session (session_id, id),
            CONSTRAINT fk_interaction_session FOREIGN KEY (room_id, session_id) REFERENCES live_sessions(room_id, id) ON DELETE RESTRICT ON UPDATE RESTRICT,
            CONSTRAINT fk_interaction_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
		`CREATE TABLE projection_sessions (
            epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            session_id BIGINT UNSIGNED NOT NULL,
            room_id BIGINT UNSIGNED NOT NULL,
            status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            PRIMARY KEY (epoch, session_id),
            CONSTRAINT fk_projection_session FOREIGN KEY (room_id, session_id) REFERENCES live_sessions(room_id, id) ON DELETE RESTRICT ON UPDATE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
		`CREATE TABLE worker_checkpoints (
            consumer_group VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            topic_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            partition_id INT NOT NULL,
            next_offset BIGINT NOT NULL,
            updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
            PRIMARY KEY (consumer_group, topic_id, partition_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
	}
	for index, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			return fmt.Errorf("场次和点赞迁移第 %d 步失败: %w", index+1, err)
		}
	}
	return nil
}
