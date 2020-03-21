#!/usr/bin/env bash

gitbranch="dev"

git checkout $gitbranch

version=$(mvn help:evaluate -Dexpression=project.version | tail -15 | head -1)

if [[ "$version" == *-SNAPSHOT ]]
then
    echo "the version of pom $version is not release candidate *-RC !"
    exit 1
fi

mvn build-helper:parse-version versions:set -DnewVersion='${parsedVersion.majorVersion}.${parsedVersion.minorVersion}.${parsedVersion.incrementalVersion}'

git add pom.xml oozie/pom.xml spark/pom.xml assembly/pom.xml

finalVersion=$(mvn help:evaluate -Dexpression=project.version | tail -15 | head -1)

echo "Businessviews - Updated the component final version to v$finalVersion"

git commit -m "Businessviews - Updated the component final version to finalVersion"

git push -u origin $gitbranch

git tag "v$finalVersion"

git push --tags