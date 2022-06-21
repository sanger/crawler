#!/bin/bash

mongo <<EOF
var config = {
    _id: "heron_rs",
    members: [{ _id: 0, host: "mongo:27017"}]
};
rs.initiate(config);
EOF
