#!/bin/bash

cd $(dirname $0)/..

version=$(ls sql/*.sql | tail -1 | sed 's/.*--\([0-9]\+\.[0-9]\+\.[0-9]\)\.sql/\1/')

echo "Packaging version $version"

sed "s/CYANAUDIT_VERSION/$version/" template_META.json > META.json

rm -f dist/cyanaudit-${version}.{tar.gz,zip}

# Make tarball
tar zcvf dist/cyanaudit-${version}.tar.gz \
    --transform="s|^|cyanaudit-${version}/|" \
    install.pl META.json LICENSE README.md doc sql/*.sql tools/*.p[lm]

# Make zip file for pgxn
ln -s . cyanaudit-$version
zip -r dist/cyanaudit-${version}.zip \
    cyanaudit-${version}/{install.pl,META.json,LICENSE,README.md,doc,sql/*.sql,tools/*.p[lm]}
rm cyanaudit-$version
