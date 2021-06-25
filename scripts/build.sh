#!/bin/bash
set -e

export GOPATH="$(cd .. && echo "${PWD}")"
export GOBIN="${GOPATH}/bin"
tar -xzf $GOPATH/go1.10.8.linux-amd64.tar.gz -C $GOPATH
export GOROOT=${GOPATH}/go
echo "CURRENT_VERSION=[${CURRENT_VERSION}], GOPATH=[${GOPATH}], GOROOT=[${GOROOT}], GOVersion=`${GOROOT}/bin/go version`"


${GOROOT}/bin/go install -v hisense.com/fusion
mv "$GOBIN/fusion" "$GOBIN/hitrain"
cp -a ../src/hisense.com/conf/hitrain.conf  ../bin/hitrain.conf
cp -a mysqldump ../bin/
cp -a mysqldump8 ../bin/
cp -a start_hitrain.sh ../bin
chmod +x ../bin/mysqldump
chmod +x ../bin/mysqldump8
chmod +x ../bin/start_hitrain.sh

