from crawler.config_helpers import get_centre_details, get_config


def test_get_config():
    test_config = {"MONGO_DB": "crawler_test", "CENTRE_DETAILS_FILE_PATH": "path/test.json"}
    config = get_config(test_config)

    assert config["MONGO_DB"] == "crawler_test"
    assert config["CENTRE_DETAILS_FILE_PATH"] == "path/test.json"

    assert "MONGO_HOST" in config.keys()
    assert "MONGO_PORT" in config.keys()


def test_get_centre_details(config):
    assert "Alderley" in [centre["name"] for centre in get_centre_details(config)]
