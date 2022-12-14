# use publicly acessible env variables in this file
#   https://flask.palletsprojects.com/en/1.1.x/cli/#environment-variables-from-dotenv

# https://flask.palletsprojects.com/en/1.1.x/cli/#application-discovery
FLASK_APP=crawler

# https://flask.palletsprojects.com/en/1.1.x/cli/#setting-command-options
FLASK_RUN_HOST=0.0.0.0
FLASK_RUN_PORT=8000

# https://flask.palletsprojects.com/en/2.2.x/config/#DEBUG
FLASK_DEBUG=true

SETTINGS_MODULE=crawler.config.development
