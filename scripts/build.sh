#!/bin/bash
set -e

export GOPATH="$(cd .. && echo "${PWD}")"
export GOBIN="${GOPATH}/bin"
tar -xzf $GOPATH/go1.10.8.linux-amd64.tar.gz -C $GOPATH
export GOROOT=${GOPATH}/go
echo "CURRENT_VERSION=[${CURRENT_VERSION}], GOPATH=[${GOPATH}], GOROOT=[${GOROOT}], GOVersion=`${GOROOT}/bin/go version`"


${GOROOT}/bin/go install -v hisense.com/fusion

cp -a ../src/hisense.com/conf/fusiondataprocessing.conf  ../bin/fusiondataprocessing.conf
cp -a mysqldump ../bin/
cp -a mysqldump8 ../bin/
cp -a start_fusion.sh ../bin
chmod +x ../bin/mysqldump
chmod +x ../bin/mysqldump8
chmod +x ../bin/start_fusion.sh

