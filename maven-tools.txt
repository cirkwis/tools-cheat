1. Add an external jar to Maven project

SET MVM_PATH=[Maven installation]\bin
SET LIB_PATH=[Root jar localization]\lib
SET LOCAL_REPO=[Path to .m2 location]\.m2\\repository
SET VERSION=1.41

%MVM_PATH%\mvn.cmd install:install-file -Dfile=%LIB_PATH%\\[jar name]-%VERSION%.jar -DgroupId=[jar group id] -DartifactId=[jar artifact id] -Dversion=%VERSION% -Dpackaging=jar -DgeneratePom=true -DlocalRepositoryPath=%LOCAL_REPO%

In project pom.xml file, add dependency: 
<dependency>
         <groupId>[jar group id]</groupId>
         <artifactId>[jar artifact id]</artifactId>
         <version>1.41</version>
      </dependency>
