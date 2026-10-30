import ipaddress
from urllib.parse import urlparse


class InvalidWebhookUrl(ValueError):
    pass


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if ip.version == 6 and ip.ipv4_mapped is not None:
        return _is_blocked_ip(ip.ipv4_mapped)
    return bool(
        ip.is_loopback
        or ip.is_private
        or ip.is_link_local
        or ip.is_unspecified
        or ip.is_multicast
        or ip.is_reserved
    )


def validate_webhook_url(url: str, *, allow_private: bool) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise InvalidWebhookUrl("webhook_url scheme must be http or https")
    if parsed.username is not None or parsed.password is not None:
        raise InvalidWebhookUrl("webhook_url must not contain userinfo")
    host = parsed.hostname
    if not host:
        raise InvalidWebhookUrl("webhook_url must include a host")

    folded = host.casefold()
    if folded == "localhost" or folded.endswith(".localhost"):
        if not allow_private:
            raise InvalidWebhookUrl("webhook_url must not point to a private host")
        return url

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return url

    if not allow_private and _is_blocked_ip(ip):
        raise InvalidWebhookUrl(
            "webhook_url must not point to a loopback, private, "
            "link-local or metadata address"
        )
    return url
