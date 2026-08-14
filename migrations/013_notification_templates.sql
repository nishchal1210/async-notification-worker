CREATE TABLE IF NOT EXISTS notification.notification_templates (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    subject_template VARCHAR(200),
    body_template TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_notification_template_name_channel_version
        UNIQUE (name, channel, version)
);

ALTER TABLE notification.notifications
    ADD COLUMN IF NOT EXISTS template_id BIGINT,
    ADD COLUMN IF NOT EXISTS template_version INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_notifications_template'
    ) THEN
        ALTER TABLE notification.notifications
            ADD CONSTRAINT fk_notifications_template
            FOREIGN KEY (template_id)
            REFERENCES notification.notification_templates(id);
    END IF;
END
$$;

INSERT INTO notification.notification_templates (
    name,
    channel,
    subject_template,
    body_template,
    version
)
VALUES
    (
        'otp',
        'email',
        'Your OTP',
        'Hello {{ name }}, your OTP is {{ otp }}.',
        1
    ),
    (
        'otp',
        'sms',
        NULL,
        'Your OTP is {{ otp }}.',
        1
    ),
    (
        'order_shipped',
        'push',
        'Order update',
        'Your order {{ order_id }} has been shipped.',
        1
    )
ON CONFLICT (name, channel, version) DO NOTHING;
