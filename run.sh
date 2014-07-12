#!/bin/sh
which java 2>/dev/null
if [ $? -ne 0 ]; then
    JAVA=/usr/local/jdk-1.7.0/bin/java
fi

mvn package && cat data.txt | $JAVA -Xms128m -Xmx128m -cp 'target/lib/*' -jar target/no-0.1-SNAPSHOT.jar
