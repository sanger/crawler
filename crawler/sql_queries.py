## SQL query to insert multiple rows into the MLWH
SQL_MLWH_MULTIPLE_INSERT = """
INSERT INTO lighthouse_sample (
mongodb_id,
root_sample_id,
cog_uk_id,
rna_id,
plate_barcode,
coordinate,
result,
date_tested_string,
date_tested,
source,
lab_id,
created_at_external,
updated_at_external
)
VALUES (
%(mongodb_id)s,
%(root_sample_id)s,
%(cog_uk_id)s,
%(rna_id)s,
%(plate_barcode)s,
%(coordinate)s,
%(result)s,
%(date_tested_string)s,
%(date_tested)s,
%(source)s,
%(lab_id)s,
%(created_at_external)s,
%(updated_at_external)s
)
ON DUPLICATE KEY UPDATE
plate_barcode=VALUES(plate_barcode),
coordinate=VALUES(coordinate),
date_tested_string=VALUES(date_tested_string),
date_tested=VALUES(date_tested),
source=VALUES(source),
lab_id=VALUES(lab_id),
created_at_external=VALUES(created_at_external),
updated_at_external=VALUES(updated_at_external);
"""