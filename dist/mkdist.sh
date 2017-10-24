#!/bin/bash

cd $(dirname $0)/..

version=$(ls sql/*.sql | tail -1 | sed 's/.*--\([0-9]\+\.[0-9]\+\)\.sql/\1/')

echo "Packaging version $version"

tar zcvf dist/cyanaudit-${version}.tar.gz \
    --transform="s|^|cyanaudit-${version}/|" \
    install.pl LICENSE README.md doc sql/*.sql tools
