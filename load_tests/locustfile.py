import random
import uuid

from locust import HttpUser, between, task


class NotificationApiUser(HttpUser):
    wait_time = between(0.1, 0.5)

    def send_notification(
        self,
        *,
        channel: str,
        recipient: str,
        subject: str | None,
        body: str,
        priority: str,
    ) -> None:
        payload = {
            "user_id": random.randint(1, 100),
            "channel": channel,
            "recipient": recipient,
            "subject": subject,
            "body": body,
            "priority": priority,
        }

        headers = {
            "Idempotency-Key": str(uuid.uuid4()),
            "X-Request-ID": str(uuid.uuid4()),
        }

        with self.client.post(
            "/notifications",
            json=payload,
            headers=headers,
            name=f"/notifications [{channel}]",
            catch_response=True,
        ) as response:
            if response.status_code == 202:
                response.success()
            else:
                response.failure(
                    f"HTTP {response.status_code}: "
                    f"{response.text[:150]}"
                )

    @task(6)
    def send_normal_email(self) -> None:
        user_id = random.randint(1, 100)

        self.send_notification(
            channel="email",
            recipient=f"loadtest-{user_id}@example.com",
            subject="Order update",
            body="Your order is being processed.",
            priority="NORMAL",
        )

    @task(3)
    def send_low_priority_push(self) -> None:
        self.send_notification(
            channel="push",
            recipient="fake-device-token",
            subject="Weekend offer",
            body="Special promotional offer.",
            priority="LOW",
        )

    @task(1)
    def send_high_priority_sms(self) -> None:
        self.send_notification(
            channel="sms",
            recipient="+919999999999",
            subject=None,
            body="Your OTP is 123456.",
            priority="HIGH",
        )