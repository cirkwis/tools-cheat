#!/usr/bin/env bash

gitbranch="dev"

git checkout $gitbranch

version=$(mvn help:evaluate -Dexpression=project.version | tail -15 | head -1)

if [[ "$version" == *-SNAPSHOT ]]
then
    mvn build-helper:parse-version versions:set -DnewVersion='${parsedVersion.majorVersion}.${parsedVersion.minorVersion}.${parsedVersion.incrementalVersion}-RC'
elif [[ "$version" == *-RC ]]
then
    mvn build-helper:parse-version versions:set -DnewVersion='${parsedVersion.majorVersion}.${parsedVersion.minorVersion}.${parsedVersion.nextIncrementalVersion}-RC'
else
    echo "the version of pom $version is not correct !"
    exit 1
fi

git add pom.xml oozie/pom.xml spark/pom.xml assembly/pom.xml

rcVersion=$(mvn help:evaluate -Dexpression=project.version | tail -15 | head -1)

echo "Businessviews - Updated the component version to v$rcVersion"

git commit -m "Businessviews - Updated the component version to v$rcVersion"

git push -u origin $gitbranch

git tag "v$rcVersion"

git push --tags

