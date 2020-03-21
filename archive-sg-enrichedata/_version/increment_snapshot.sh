#!/usr/bin/env bash

gitbranch="dev"

git checkout $gitbranch

version=$(mvn help:evaluate -Dexpression=project.version | tail -15 | head -1)

mvn build-helper:parse-version versions:set -DnewVersion='${parsedVersion.majorVersion}.${parsedVersion.minorVersion}.${parsedVersion.nextIncrementalVersion}-SNAPSHOT'

git add pom.xml oozie/pom.xml spark/pom.xml assembly/pom.xml

nextVersion=$(mvn help:evaluate -Dexpression=project.version | tail -15 | head -1)

echo "Businessviews - Updated the component version to v$nextVersion"

git commit -m "Businessviews - Updated the component version to v$nextVersion"

git push -u origin $gitbranch
