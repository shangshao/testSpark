#!/usr/bin/env bash
mvn clean assembly:assembly source:jar -DskipTests
echo “Starting to sftp…”
sftp shangyongqiang@192.168.3.172 <<EOF
cd sanxing
put  D:/myproject/testSpark/target/sanxing.jar
bye
EOF
echo “done”