"""Fondat module for AWS Secrets Manager."""

import logging

from collections.abc import Iterable
from fondat.aws import Client, wrap_client_error
from fondat.data import datacls
from fondat.http import AsBody, InBody
from fondat.resource import resource, operation
from fondat.security import Policy
from typing import Annotated, Any, Union


_logger = logging.getLogger(__name__)


@datacls
class Secret:
    value: Union[str, bytes]


def secrets_resource(client: Client, policies: Iterable[Policy] = None) -> Any:
    """
    Create secrets resource.

    Parameters:
    • client: AWS secretsmanager client
    • kms_key_id: ARN, key ID or alias of AMS KMS key to encrypt secrets
    • policies: security policies to apply to all operations
    """

    if client.service_name != "secretsmanager":
        raise TypeError("expecting secretsmanager client")

    @resource
    class SecretResource:
        """..."""

        def __init__(self, name: str):
            self.name = name

        @operation(policies=policies)
        async def get(self, version_id: str = None, version_stage: str = None) -> Secret:
            """Get secret."""
            with wrap_client_error():
                kwargs = {}
                kwargs["SecretId"] = self.name
                if version_id is not None:
                    kwargs["VersionId"] = version_id
                if version_stage is not None:
                    kwargs["VersionStage"] = version_stage
                secret = await client.get_secret_value(**kwargs)
            return Secret(value=secret.get("SecretString") or secret.get("SecretBinary"))

        @operation(policies=policies)
        async def put(self, secret: Annotated[Secret, AsBody]):
            """Update secret."""
            args = {
                "SecretString"
                if isinstance(secret.value, str)
                else "SecretBinary": secret.value
            }
            with wrap_client_error():
                await client.put_secret_value(SecretId=self.name, **args)

        @operation(policies=policies)
        async def delete(self):
            """Delete secret."""
            with wrap_client_error():
                await client.delete_secret(SecretId=self.name)

    @resource
    class SecretsResource:
        """..."""

        @operation(policies=policies)
        async def post(
            self,
            name: Annotated[str, InBody],
            secret: Annotated[Secret, InBody],
            kms_key_id: Annotated[str, InBody] = None,
            tags: Annotated[dict[str, str], InBody] = None,
        ):
            """Create secret."""
            kwargs = {}
            kwargs["Name"] = name
            if isinstance(secret.value, str):
                kwargs["SecretString"] = secret.value
            else:
                kwargs["SecretBinary"] = secret.value
            if kms_key_id is not None:
                kwargs["KmsKeyId"] = kms_key_id
            if tags is not None:
                kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            with wrap_client_error():
                await client.create_secret(**kwargs)

        def __getitem__(self, name: str) -> SecretResource:
            return SecretResource(name)

    return SecretsResource()
