package migrations

import "gorm.io/gorm"

// createFailedEventsV8 只新增异常消息表；旧迁移和点赞恢复数据保持原样。
func createFailedEventsV8(db *gorm.DB) error {
	return db.Exec(`CREATE TABLE failed_events (
        consumer_group VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        topic_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        partition_id INT NOT NULL,
        record_offset BIGINT NOT NULL,
        raw_key MEDIUMBLOB NOT NULL,
        raw_payload MEDIUMBLOB NOT NULL,
        key_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        payload_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        key_bytes BIGINT UNSIGNED NOT NULL,
        payload_bytes BIGINT UNSIGNED NOT NULL,
        truncated BOOLEAN NOT NULL,
        error_code VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        error_message VARCHAR(512) NOT NULL,
        attempts INT UNSIGNED NOT NULL,
        status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'OPEN',
        resolution_details VARCHAR(1024) NOT NULL DEFAULT '',
        first_failed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        resolved_at DATETIME(3) NULL,
        PRIMARY KEY (consumer_group, topic_id, partition_id, record_offset),
        KEY idx_failed_events_status (consumer_group, status, first_failed_at),
        CONSTRAINT ck_failed_events_position CHECK (partition_id >= 0 AND record_offset >= 0),
        CONSTRAINT ck_failed_events_attempts CHECK (attempts BETWEEN 1 AND 3),
        CONSTRAINT ck_failed_events_status CHECK (status IN ('OPEN', 'RESOLVED')),
        CONSTRAINT ck_failed_events_size CHECK (OCTET_LENGTH(raw_key) <= 65536 AND OCTET_LENGTH(raw_payload) <= 65536)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`).Error
}
