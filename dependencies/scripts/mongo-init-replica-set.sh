#!/bin/bash

mongo <<EOF
var config = {
    _id: "heron_rs",
    members: [{ _id: 0, host: "127.0.0.1:27017"}]
};
rs.initiate(config);
EOF
