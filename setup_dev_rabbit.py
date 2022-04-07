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
QUEUE_TYPE = "classic"
CRUD_QUEUE = "heron.crud-operations"


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

print(f"Declaring CRUD queue '{CRUD_QUEUE}'")
print_command_output(
    [
        "declare",
        "queue",
        f"name={CRUD_QUEUE}",
        f"queue_type={QUEUE_TYPE}",
        f'arguments={json.dumps({"x-queue-type": QUEUE_TYPE})}',
    ]
)
