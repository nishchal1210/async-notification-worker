import os

import firebase_admin
from firebase_admin import credentials, messaging

from providers.base import NotificationProvider


_FIREBASE_ENV_FIELDS = {
    "FIREBASE_PROJECT_ID": "project_id",
    "FIREBASE_PRIVATE_KEY_ID": "private_key_id",
    "FIREBASE_PRIVATE_KEY": "private_key",
    "FIREBASE_CLIENT_EMAIL": "client_email",
    "FIREBASE_CLIENT_ID": "client_id",
    "FIREBASE_CLIENT_X509_CERT_URL": "client_x509_cert_url",
}


def _service_account_info() -> dict[str, str]:
    missing = [name for name in _FIREBASE_ENV_FIELDS if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Firebase configuration is incomplete; missing: "
            + ", ".join(missing)
        )

    info = {
        json_field: os.environ[env_name]
        for env_name, json_field in _FIREBASE_ENV_FIELDS.items()
    }
    info.update(
        {
            "type": "service_account",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": (
                "https://www.googleapis.com/oauth2/v1/certs"
            ),
            "universe_domain": "googleapis.com",
        }
    )
    info["private_key"] = info["private_key"].replace("\\n", "\n")
    return info


def _get_firebase_app():
    try:
        return firebase_admin.get_app()
    except ValueError:
        credential = credentials.Certificate(_service_account_info())
        return firebase_admin.initialize_app(credential)


class PushProvider(NotificationProvider):
    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
    ) -> None:
        message = messaging.Message(
            notification=messaging.Notification(
                title=subject or "Notification",
                body=body,
            ),
            token=recipient,
        )
        messaging.send(message, app=_get_firebase_app())
