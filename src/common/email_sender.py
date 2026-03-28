import gzip
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import getaddresses
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from common.exceptions import (
    EmptyRecipientsError,
    ResourceNotFoundError,
    SizeLimitExceededError,
)


@dataclass(frozen=True)
class EmailContent:
    attachment_name: str
    subject: str
    text_body: str
    html_body: str


class EmailSender:
    _MAX_SEND_ATTEMPTS = 3
    _DEFAULT_MAX_RAW_EMAIL_SIZE_BYTES = 10 * 1024 * 1024
    _TEMPLATE_RELATIVE_PATH = Path("static") / "templates" / "email_export.html"

    def __init__(
        self,
        sender: str = "data@company.com",
        bcc: str | None = None,
        max_raw_email_size_bytes: int = _DEFAULT_MAX_RAW_EMAIL_SIZE_BYTES,
        file_name: str = "export.csv",
        category: str | None = None,
        brands: str | None = None,
        min_date: str | None = None,
        max_date: str | None = None,
        row_count: int | None = None,
    ):
        self.sender = sender
        self.bcc = bcc
        self.max_raw_email_size_bytes = max_raw_email_size_bytes
        self.file_name = file_name
        self.category = category
        self.brands = brands
        self.min_date = min_date
        self.max_date = max_date
        self.row_count = row_count
        self._html_template: str | None = None
        self.ses = boto3.client("ses")

    def normalize_recipients(self, recipients: Sequence[str]) -> list[str]:
        addresses = [
            addr.strip() for _, addr in getaddresses(recipients) if addr.strip()
        ]
        deduped = list(dict.fromkeys(addresses))
        if not deduped:
            raise EmptyRecipientsError
        return deduped

    def send(
        self,
        recipients: Sequence[str],
        csv_bytes: bytes,
    ) -> None:
        recipients = self.normalize_recipients(recipients)
        file_size = self._format_megabytes(len(csv_bytes))
        content = self._build_content(
            file_size=file_size,
        )

        compressed_payload = gzip.compress(csv_bytes)
        max_attachment_size = self._compute_max_attachment_size(
            recipients=recipients,
            subject=content.subject,
            text_body=content.text_body,
            html_body=content.html_body,
            attachment_name=content.attachment_name,
        )
        self._validate_attachment_size(compressed_payload, max_attachment_size)

        message = self._build_message(
            recipients=recipients,
            subject=content.subject,
            text_body=content.text_body,
            html_body=content.html_body,
            attachment_name=content.attachment_name,
            attachment_payload=compressed_payload,
        )

        raw_email = message.as_string().encode("utf-8")
        if len(raw_email) > self.max_raw_email_size_bytes:
            raise SizeLimitExceededError(len(raw_email), self.max_raw_email_size_bytes)

        self._send_raw_email_with_retry(recipients, raw_email)

    def _build_content(
        self,
        file_size: str,
    ) -> EmailContent:
        file_name = self.file_name
        base_name = file_name.rsplit(".", 1)[0]
        attachment_name = self._gzip_attachment_name(file_name)
        category = self.category or "Uncategorized"
        brands = self.brands or ""
        subject = f"Data Export Available - {category}"

        text_body = (
            "Hello,\n\n"
            f"Your export file '{file_name}' is attached to this email.\n\n"
            "Best regards."
        )

        html_template = self._load_html_template()
        date_range_block = self._build_date_range_block(
            min_date=self.min_date, max_date=self.max_date
        )
        html_body = html_template.format(
            export_name=base_name,
            attachment_name=file_name,
            category=category,
            brands=brands,
            date_range_block=date_range_block,
            file_size=file_size,
            row_count=str(self.row_count) if self.row_count is not None else "N/A",
            year=datetime.now(UTC).year,
        )
        return EmailContent(
            attachment_name=attachment_name,
            subject=subject,
            text_body=text_body,
            html_body=html_body,
        )

    @staticmethod
    def _format_megabytes(size_bytes: int) -> str:
        return f"{size_bytes / 1024**2:.2f} MB"

    @staticmethod
    def _gzip_attachment_name(file_name: str) -> str:
        return file_name if file_name.lower().endswith(".gz") else f"{file_name}.gz"

    @staticmethod
    def _build_date_range_block(min_date: str | None, max_date: str | None) -> str:
        min_date = min_date.strip() if min_date else None
        max_date = max_date.strip() if max_date else None

        if min_date and max_date:
            value = f"{min_date} to {max_date}"
            label = "Period"
        elif max_date:
            value = max_date
            label = "Date"
        else:
            value = datetime.now(UTC).date().isoformat()
            label = "Date"

        return f'<span style="display:block;"><strong>{label}:</strong> {value}</span>'

    def _load_html_template(self) -> str:
        if self._html_template is not None:
            return self._html_template

        base_path = Path() if os.environ.get("AWS_EXECUTION_ENV") else Path("src")
        template_path = base_path / self._TEMPLATE_RELATIVE_PATH
        if not template_path.exists():
            raise ResourceNotFoundError(template_path)

        self._html_template = template_path.read_text(encoding="utf-8").strip()
        return self._html_template

    def _build_message(
        self,
        recipients: Sequence[str],
        subject: str,
        text_body: str,
        html_body: str,
        attachment_name: str,
        attachment_payload: bytes,
    ) -> MIMEMultipart:
        message = MIMEMultipart("mixed")
        message["Subject"] = subject
        message["From"] = self.sender
        message["To"] = ", ".join(recipients)
        if self.bcc:
            message["Bcc"] = self.bcc

        alternative = MIMEMultipart("alternative")
        alternative.attach(MIMEText(text_body, "plain", "utf-8"))
        alternative.attach(MIMEText(html_body, "html", "utf-8"))
        message.attach(alternative)

        attachment = MIMEBase("application", "gzip")
        attachment.set_payload(attachment_payload)
        encoders.encode_base64(attachment)
        attachment.add_header(
            "Content-Disposition",
            "attachment",
            filename=attachment_name,
        )
        message.attach(attachment)

        return message

    def _validate_attachment_size(
        self, compressed_payload: bytes, max_attachment_size: int
    ) -> None:
        usage_ratio = (
            len(compressed_payload) / max_attachment_size
            if max_attachment_size > 0
            else float("inf")
        )
        remaining_bytes = max(max_attachment_size - len(compressed_payload), 0)
        print(
            "Email attachment capacity usage: "
            f"{self._format_megabytes(len(compressed_payload))} / "
            f"{self._format_megabytes(max_attachment_size)} "
            f"({usage_ratio:.2%} used, {self._format_megabytes(remaining_bytes)} remaining)"
        )

        if len(compressed_payload) > max_attachment_size:
            raise SizeLimitExceededError(len(compressed_payload), max_attachment_size)

    def _send_raw_email_with_retry(
        self, recipients: Sequence[str], raw_email: bytes
    ) -> None:
        destinations = list(recipients) + ([self.bcc] if self.bcc else [])
        for attempt in range(self._MAX_SEND_ATTEMPTS):
            try:
                self.ses.send_raw_email(
                    Source=self.sender,
                    Destinations=destinations,
                    RawMessage={"Data": raw_email},
                )
                print(f"Email sent to {list(recipients)}")
            except ClientError as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self._MAX_SEND_ATTEMPTS - 1:
                    time.sleep(2**attempt)
                else:
                    raise
            else:
                return

    def _compute_max_attachment_size(
        self,
        recipients: Sequence[str],
        subject: str,
        text_body: str,
        html_body: str,
        attachment_name: str,
    ) -> int:
        low = 0
        high = self.max_raw_email_size_bytes

        while low < high:
            mid = (low + high + 1) // 2
            probe_message = self._build_message(
                recipients=recipients,
                subject=subject,
                text_body=text_body,
                html_body=html_body,
                attachment_name=attachment_name,
                attachment_payload=b"x" * mid,
            )
            probe_size = len(probe_message.as_string().encode("utf-8"))
            if probe_size <= self.max_raw_email_size_bytes:
                low = mid
            else:
                high = mid - 1

        return low
