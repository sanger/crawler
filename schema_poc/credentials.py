CREDENTIAL_KEY_API_KEY = "schema-api-key"
CREDENTIAL_KEY_RABBITMQ = "rabbit-mq-user-pass"


def pam_credentials():
    # credentials_pam.py is not in the default repository clone and will need creating
    from credentials_pam import RABBITMQ_PASSWORD, RABBITMQ_USERNAME, SCHEMA_API_KEY  # type: ignore

    return {CREDENTIAL_KEY_API_KEY: SCHEMA_API_KEY, CREDENTIAL_KEY_RABBITMQ: (RABBITMQ_USERNAME, RABBITMQ_PASSWORD)}


def psd_credentials():
    # credentials_psd.py is not in the default repository clone and will need creating
    from credentials_psd import RABBITMQ_PASSWORD, RABBITMQ_USERNAME, SCHEMA_API_KEY  # type: ignore

    return {CREDENTIAL_KEY_API_KEY: SCHEMA_API_KEY, CREDENTIAL_KEY_RABBITMQ: (RABBITMQ_USERNAME, RABBITMQ_PASSWORD)}


PUBLISH_CREDENTIALS = {
    "cherrypicked-samples": psd_credentials,
    "create-plate-map": pam_credentials,
    "create-plate-map-feedback": psd_credentials,
    "update-plate-map-sample": pam_credentials,
    "update-plate-map-sample-feedback": psd_credentials,
}

CONSUME_CREDENTIALS = {
    "cherrypicked-samples": pam_credentials,
    "create-plate-map": psd_credentials,
    "create-plate-map-feedback": pam_credentials,
    "update-plate-map-sample": psd_credentials,
    "update-plate-map-sample-feedback": pam_credentials,
}
