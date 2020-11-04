def update_filtered_positives(config):
    # Get pending plate barcodes from DART - no way to do this yet
    # Pull all RESULT=positive samples from mongo with these plate barcodes - can probably do this. Get everything structured from mongo as we pass it to mongo
    # Re-determine whether filtered-positive - just pass what we get from mongo through the identifier
    # Update filtered-positive version etc. in mongo and MLWH
    # Re-upload well properties to DART - call insert_plates_and_wells_from_docs_into_dart?