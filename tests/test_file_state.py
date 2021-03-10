from unittest.mock import patch

from crawler.file_processing import ERRORS_DIR, Centre, CentreFile
from crawler.helpers.enums import CentreFileState


def test_set_state_for_file_when_file_in_black_list(config, blacklist_for_centre, testing_centres):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", False, centre)
    centre_file.set_state_for_file()

    assert centre_file.file_state == CentreFileState.FILE_IN_BLACKLIST


def test_set_state_for_file_when_never_seen_before(config, testing_centres):
    centre = Centre(config, config.CENTRES[0])
    centre_file = CentreFile("AP_sanger_report_200503_2338.csv", False, centre)
    centre_file.set_state_for_file()

    assert centre_file.file_state == CentreFileState.FILE_NOT_PROCESSED_YET


def test_set_state_for_file_when_in_error_folder(config, tmpdir, testing_centres):
    with patch.dict(config.CENTRES[0], {"backups_folder": tmpdir.realpath()}):
        errors_folder = tmpdir.mkdir(ERRORS_DIR)

        # configure to use the backups folder for this test
        centre = Centre(config, config.CENTRES[0])

        # create a backup of the file inside the errors directory as if previously processed there
        filename = "AP_sanger_report_200518_2132.csv"
        centre_file = CentreFile(filename, False, centre)
        centre_file.logging_collection.add_error("TYPE 4", "Some error happened")
        centre_file.backup_file()

        assert len(errors_folder.listdir()) == 1

        # check the file state again now the error version exists
        centre_file.set_state_for_file()

        assert centre_file.file_state == CentreFileState.FILE_PROCESSED_WITH_ERROR
