from abc import ABC, abstractmethod


class NotificationProvider(ABC):
    """Common contract implemented by every delivery channel."""

    @abstractmethod
    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
    ) -> None:
        """Deliver a notification or raise an exception on failure."""
