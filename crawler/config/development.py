from crawler.config.defaults import *  # noqa: F403,F401

# setting here will overwrite those in 'defaults.py'


# MLWH database details
MLWH_DB_DBNAME = "unified_warehouse_test"
MLWH_DB_HOST = "172.27.16.48"
MLWH_DB_PORT = 3306
MLWH_DB_RO_USER = "root"
MLWH_DB_RO_PASSWORD = ""
MLWH_DB_RW_USER = "root"
MLWH_DB_RW_PASSWORD = ""

# DART database details
DART_DB_DBNAME = "dart_test"
DART_DB_HOST = "DARTDEV-DB-SRV"
DART_DB_PORT = 1433
DART_DB_RO_USER = "root"
DART_DB_RO_PASSWORD = ""
DART_DB_RW_USER = "dart_dev_rw"
DART_DB_RW_PASSWORD = "Dtrw6789!"

# logging config
LOGGING["loggers"]["crawler"]["level"] = "INFO"  # noqa: F405
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]  # noqa: F405
