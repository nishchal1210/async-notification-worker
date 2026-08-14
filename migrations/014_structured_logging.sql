ALTER TABLE notification.notifications
    ADD COLUMN IF NOT EXISTS request_id VARCHAR(100);

CREATE INDEX IF NOT EXISTS ix_notifications_request_id
    ON notification.notifications (request_id);
