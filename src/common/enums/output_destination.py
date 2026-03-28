from enum import Enum


class OutputDestination(Enum):
    DATABASE = "database"
    S3 = "s3"
    EMAIL = "email"
    S3_EMAIL = "s3_email"

    @property
    def uses_db(self) -> bool:
        return self == OutputDestination.DATABASE

    @property
    def uses_s3(self) -> bool:
        return self in {OutputDestination.S3, OutputDestination.S3_EMAIL}

    @property
    def uses_email(self) -> bool:
        return self in {OutputDestination.EMAIL, OutputDestination.S3_EMAIL}
