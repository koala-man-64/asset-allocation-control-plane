from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from api.service.settings import NotificationSettings

logger = logging.getLogger("asset-allocation.notifications.delivery")


@dataclass(frozen=True)
class DeliverySendResult:
    provider: str
    providerMessageId: str | None = None


class NotificationDeliveryClient:
    def send_email(self, *, to: str, subject: str, body: str) -> DeliverySendResult:
        raise NotImplementedError

    def send_sms(self, *, to: str, body: str) -> DeliverySendResult:
        raise NotImplementedError


class LogNotificationDeliveryClient(NotificationDeliveryClient):
    def send_email(self, *, to: str, subject: str, body: str) -> DeliverySendResult:
        logger.info("Notification email logged: to=%s subject=%s body_length=%s", to, subject, len(body))
        return DeliverySendResult(provider="log", providerMessageId=f"log-email-{uuid.uuid4().hex}")

    def send_sms(self, *, to: str, body: str) -> DeliverySendResult:
        logger.info("Notification SMS logged: to=%s body_length=%s", to, len(body))
        return DeliverySendResult(provider="log", providerMessageId=f"log-sms-{uuid.uuid4().hex}")


class AcsNotificationDeliveryClient(NotificationDeliveryClient):
    def __init__(self, settings: NotificationSettings) -> None:
        self._settings = settings

    def send_email(self, *, to: str, subject: str, body: str) -> DeliverySendResult:
        if not self._settings.acs_email_sender:
            raise RuntimeError("NOTIFICATIONS_ACS_EMAIL_SENDER is required for ACS email delivery.")
        try:
            from azure.communication.email import EmailClient
        except ImportError as exc:
            raise RuntimeError("azure-communication-email is required for ACS email delivery.") from exc

        client = EmailClient.from_connection_string(self._settings.acs_connection_string or "")
        poller = client.begin_send(
            {
                "senderAddress": self._settings.acs_email_sender,
                "recipients": {"to": [{"address": to}]},
                "content": {"subject": subject, "plainText": body},
            }
        )
        result = poller.result()
        provider_message_id = _result_id(result)
        return DeliverySendResult(provider="acs-email", providerMessageId=provider_message_id)

    def send_sms(self, *, to: str, body: str) -> DeliverySendResult:
        if not self._settings.acs_sms_from:
            raise RuntimeError("NOTIFICATIONS_ACS_SMS_FROM is required for ACS SMS delivery.")
        try:
            from azure.communication.sms import SmsClient
        except ImportError as exc:
            raise RuntimeError("azure-communication-sms is required for ACS SMS delivery.") from exc

        client = SmsClient.from_connection_string(self._settings.acs_connection_string or "")
        result = client.send(from_=self._settings.acs_sms_from, to=[to], message=body, enable_delivery_report=True)
        first = result[0] if result else None
        provider_message_id = _result_id(first)
        return DeliverySendResult(provider="acs-sms", providerMessageId=provider_message_id)


def build_notification_delivery_client(settings: NotificationSettings) -> NotificationDeliveryClient:
    if settings.delivery_provider == "acs":
        return AcsNotificationDeliveryClient(settings)
    return LogNotificationDeliveryClient()


def _result_id(result: object) -> str | None:
    if result is None:
        return None
    value = getattr(result, "id", None) or getattr(result, "message_id", None)
    if value:
        return str(value)
    if isinstance(result, dict):
        raw = result.get("id") or result.get("messageId") or result.get("message_id")
        return str(raw) if raw else None
    return None
