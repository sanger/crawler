# flake8: noqa
# SQL query to insert multiple rows into the MLWH
SQL_MLWH_MULTIPLE_INSERT = """
INSERT INTO lighthouse_sample (
mongodb_id,
root_sample_id,
rna_id,
plate_barcode,
coordinate,
result,
date_tested_string,
date_tested,
source,
lab_id,
ch1_target,
ch1_result,
ch1_cq,
ch2_target,
ch2_result,
ch2_cq,
ch3_target,
ch3_result,
ch3_cq,
ch4_target,
ch4_result,
ch4_cq,
filtered_positive,
filtered_positive_version,
filtered_positive_timestamp,
lh_sample_uuid,
lh_source_plate_uuid,
created_at,
updated_at
)
VALUES (
%(mongodb_id)s,
%(root_sample_id)s,
%(rna_id)s,
%(plate_barcode)s,
%(coordinate)s,
%(result)s,
%(date_tested_string)s,
%(date_tested)s,
%(source)s,
%(lab_id)s,
%(ch1_target)s,
%(ch1_result)s,
%(ch1_cq)s,
%(ch2_target)s,
%(ch2_result)s,
%(ch2_cq)s,
%(ch3_target)s,
%(ch3_result)s,
%(ch3_cq)s,
%(ch4_target)s,
%(ch4_result)s,
%(ch4_cq)s,
%(filtered_positive)s,
%(filtered_positive_version)s,
%(filtered_positive_timestamp)s,
%(lh_sample_uuid)s,
%(lh_source_plate_uuid)s,
%(created_at)s,
%(updated_at)s
)
ON DUPLICATE KEY UPDATE
plate_barcode=VALUES(plate_barcode),
coordinate=VALUES(coordinate),
date_tested_string=VALUES(date_tested_string),
date_tested=VALUES(date_tested),
source=VALUES(source),
lab_id=VALUES(lab_id),
updated_at=VALUES(updated_at),
lh_sample_uuid=VALUES(lh_sample_uuid),
lh_source_plate_uuid=VALUES(lh_source_plate_uuid);
"""

SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE = """\
UPDATE lighthouse_sample
SET
filtered_positive = %(filtered_positive)s,
filtered_positive_version = %(filtered_positive_version)s,
filtered_positive_timestamp = %(filtered_positive_timestamp)s
WHERE mongodb_id = %(mongodb_id)s
"""


SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE_BATCH = """\
UPDATE lighthouse_sample
SET
filtered_positive = %%s,
filtered_positive_version = %%s,
filtered_positive_timestamp = %%s,
updated_at= %%s
WHERE mongodb_id IN (%s)
"""


# DART SQL queries
SQL_DART_GET_PLATE_PROPERTY = """\
SET NOCOUNT ON
DECLARE @output_value nvarchar(256)
EXECUTE [dbo].[plDART_PlatePropGet] @plate_barcode = ?, @prop_name = ?, @value = @output_value OUTPUT
SELECT @output_value
"""

SQL_DART_SET_PLATE_PROPERTY = """\
SET NOCOUNT ON
DECLARE @return_code int
EXECUTE @return_code = [dbo].[plDART_PlatePropSet] @plate_barcode = ?, @prop_name = ?, @prop_value = ?
SELECT @return_code
"""

SQL_DART_GET_PLATE_BARCODES = """\
SELECT DISTINCT [Labware LIMS BARCODE] FROM dbo.view_plate_maps WHERE [Labware state] = ?
"""

SQL_DART_SET_WELL_PROPERTY = "{CALL dbo.plDART_PlateUpdateWell (?,?,?,?)}"

SQL_DART_ADD_PLATE = "{CALL dbo.plDART_PlateCreate (?,?,?)}"
