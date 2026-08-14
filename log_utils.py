def mask_email(email: str) -> str:
    local, separator, domain = email.partition("@")
    if not separator:
        return "***"

    if len(local) <= 2:
        masked_local = local[0:1] + "***"
    else:
        masked_local = local[:2] + "***"
    return f"{masked_local}@{domain}"


def mask_phone(phone: str) -> str:
    if len(phone) <= 4:
        return "****"
    return f"***{phone[-4:]}"


def mask_device_token(token: str) -> str:
    if len(token) <= 8:
        return "********"
    return f"{token[:4]}...{token[-4:]}"


def mask_recipient(recipient: str, channel: str) -> str:
    if channel == "email":
        return mask_email(recipient)
    if channel == "sms":
        return mask_phone(recipient)
    if channel == "push":
        return mask_device_token(recipient)
    return "***"
