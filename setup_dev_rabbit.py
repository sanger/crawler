import json
import os
import stat
import subprocess
import urllib.request

RABBITMQ_ADMIN_FILE = "rabbitmqadmin"

HOST = "localhost"
PORT = "8080"
USERNAME = "admin"
PASSWORD = "development"

VHOST = "heron"
EXCHANGE_TYPE = "topic"
AE_EXCHANGE_TYPE = "fanout"
DL_EXCHANGE_TYPE = "topic"
QUEUE_TYPE = "classic"

DL_EXCHANGE = "heron.dead-letters"
CRUD_DL_QUEUE = "heron.crud-operations.dead-letters"
FEEDBACK_DL_QUEUE = "heron.feedback.dead-letters"

PAM_AE_EXCHANGE = "pam.heron.unrouted"
PAM_UNROUTED_QUEUE = "pam.heron.unrouted"

PAM_EXCHANGE = "pam.heron"
CRUD_QUEUE = "heron.crud-operations"
CRUD_ROUTING_KEY = "crud.#"

PSD_AE_EXCHANGE = "psd.heron.unrouted"
PSD_UNROUTED_QUEUE = "psd.heron.unrouted"

PSD_EXCHANGE = "psd.heron"
FEEDBACK_QUEUE = "heron.feedback"
FEEDBACK_ROUTING_KEY = "feedback.#"


def print_command_output(specific_command):
    command_parts = [
        f"./{RABBITMQ_ADMIN_FILE}",
        f"--host={HOST}",
        f"--port={PORT}",
        f"--username={USERNAME}",
        f"--password={PASSWORD}",
        f"--vhost={VHOST}",
        *specific_command,
    ]

    print(subprocess.run(command_parts, encoding="utf-8", stdout=subprocess.PIPE).stdout)


print(f"Downloading {RABBITMQ_ADMIN_FILE} tool and setting as executable for your user")
urllib.request.urlretrieve(f"http://{HOST}:{PORT}/cli/{RABBITMQ_ADMIN_FILE}", RABBITMQ_ADMIN_FILE)
st = os.stat(RABBITMQ_ADMIN_FILE)
os.chmod(RABBITMQ_ADMIN_FILE, st.st_mode | stat.S_IXUSR)
print()

print(f"Declaring vhost '{VHOST}'")
print_command_output(["declare", "vhost", f"name={VHOST}"])

print(f"Declaring dead letter exchange '{DL_EXCHANGE}'")
print_command_output(
    [
        "declare",
        "exchange",
        f"name={DL_EXCHANGE}",
        f"type={DL_EXCHANGE_TYPE}",
    ]
)

print(f"Declaring CRUD dead letters queue '{CRUD_DL_QUEUE}'")
print_command_output(
    [
        "declare",
        "queue",
        f"name={CRUD_DL_QUEUE}",
        f"queue_type={QUEUE_TYPE}",
        f'arguments={json.dumps({"x-queue-type": QUEUE_TYPE})}',
    ]
)

print("Declaring CRUD dead letters binding")
print_command_output(
    [
        "declare",
        "binding",
        f"source={DL_EXCHANGE}",
        f"destination={CRUD_DL_QUEUE}",
        f"routing_key={CRUD_ROUTING_KEY}",
    ]
)

print(f"Declaring feedback dead letters queue '{FEEDBACK_DL_QUEUE}'")
print_command_output(
    [
        "declare",
        "queue",
        f"name={FEEDBACK_DL_QUEUE}",
        f"queue_type={QUEUE_TYPE}",
        f'arguments={json.dumps({"x-queue-type": QUEUE_TYPE})}',
    ]
)

print("Declaring feedback dead letters binding")
print_command_output(
    [
        "declare",
        "binding",
        f"source={DL_EXCHANGE}",
        f"destination={FEEDBACK_DL_QUEUE}",
        f"routing_key={FEEDBACK_ROUTING_KEY}",
    ]
)

print(f"Declaring PAM alternate exchange '{PAM_AE_EXCHANGE}'")
print_command_output(
    [
        "declare",
        "exchange",
        f"name={PAM_AE_EXCHANGE}",
        f"type={AE_EXCHANGE_TYPE}",
    ]
)

print(f"Declaring PAM unrouted queue '{PAM_UNROUTED_QUEUE}'")
print_command_output(
    [
        "declare",
        "queue",
        f"name={PAM_UNROUTED_QUEUE}",
        f"queue_type={QUEUE_TYPE}",
        f'arguments={json.dumps({"x-queue-type": QUEUE_TYPE})}',
    ]
)

print("Declaring PAM unrouted binding")
print_command_output(
    [
        "declare",
        "binding",
        f"source={PAM_AE_EXCHANGE}",
        f"destination={PAM_UNROUTED_QUEUE}",
    ]
)

print(f"Declaring PAM exchange '{PAM_EXCHANGE}'")
print_command_output(
    [
        "declare",
        "exchange",
        f"name={PAM_EXCHANGE}",
        f"type={EXCHANGE_TYPE}",
        f'arguments={json.dumps({"alternate-exchange": PAM_AE_EXCHANGE})}',
    ]
)

print(f"Declaring CRUD queue '{CRUD_QUEUE}'")
print_command_output(
    [
        "declare",
        "queue",
        f"name={CRUD_QUEUE}",
        f"queue_type={QUEUE_TYPE}",
        f'arguments={json.dumps({"x-queue-type": QUEUE_TYPE, "x-dead-letter-exchange": DL_EXCHANGE})}',
    ]
)

print("Declaring CRUD binding")
print_command_output(
    [
        "declare",
        "binding",
        f"source={PAM_EXCHANGE}",
        f"destination={CRUD_QUEUE}",
        f"routing_key={CRUD_ROUTING_KEY}",
    ]
)

print(f"Declaring PSD alternate exchange '{PSD_AE_EXCHANGE}'")
print_command_output(
    [
        "declare",
        "exchange",
        f"name={PSD_AE_EXCHANGE}",
        f"type={AE_EXCHANGE_TYPE}",
    ]
)

print(f"Declaring PSD unrouted queue '{PSD_UNROUTED_QUEUE}'")
print_command_output(
    [
        "declare",
        "queue",
        f"name={PSD_UNROUTED_QUEUE}",
        f"queue_type={QUEUE_TYPE}",
        f'arguments={json.dumps({"x-queue-type": QUEUE_TYPE})}',
    ]
)

print("Declaring PAM unrouted binding")
print_command_output(
    [
        "declare",
        "binding",
        f"source={PSD_AE_EXCHANGE}",
        f"destination={PSD_UNROUTED_QUEUE}",
    ]
)

print(f"Declaring feedback exchange '{PSD_EXCHANGE}'")
print_command_output(
    [
        "declare",
        "exchange",
        f"name={PSD_EXCHANGE}",
        f"type={EXCHANGE_TYPE}",
        f'arguments={json.dumps({"alternate-exchange": PSD_AE_EXCHANGE})}',
    ]
)

print(f"Declaring feedback queue '{FEEDBACK_QUEUE}'")
print_command_output(
    [
        "declare",
        "queue",
        f"name={FEEDBACK_QUEUE}",
        f"queue_type={QUEUE_TYPE}",
        f'arguments={json.dumps({"x-queue-type": QUEUE_TYPE, "x-dead-letter-exchange": DL_EXCHANGE})}',
    ]
)

print("Declaring feedback binding")
print_command_output(
    [
        "declare",
        "binding",
        f"source={PSD_EXCHANGE}",
        f"destination={FEEDBACK_QUEUE}",
        f"routing_key={FEEDBACK_ROUTING_KEY}",
    ]
)
