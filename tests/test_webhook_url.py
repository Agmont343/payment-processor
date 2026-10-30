import pytest

from app.common.webhook_url import InvalidWebhookUrl, validate_webhook_url


def test_public_https_url_is_allowed() -> None:
    url = "https://example.com/webhook"
    assert validate_webhook_url(url, allow_private=False) == url


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/hook",
        "http://localhost/hook",
        "http://127.0.0.1/hook",
        "http://10.0.0.1/hook",
        "http://192.168.1.10/hook",
        "http://169.254.169.254/latest",
        "http://[::1]/hook",
        "https://user:pass@example.com/hook",
    ],
)
def test_private_or_invalid_urls_are_rejected(url: str) -> None:
    with pytest.raises(InvalidWebhookUrl):
        validate_webhook_url(url, allow_private=False)


def test_private_url_allowed_when_flag_enabled() -> None:
    url = "http://127.0.0.1:8080/hook"
    assert validate_webhook_url(url, allow_private=True) == url
