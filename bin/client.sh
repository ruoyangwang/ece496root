#!/usr/bin/env bash
#java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Client localhost:2181


ZOOBIN="${BASH_SOURCE-$0}"
ZOOBIN="$(dirname "${ZOOBIN}")"
ZOOBINDIR="$(cd "${ZOOBIN}"; pwd)"

if [ -e "$ZOOBIN/../libexec/zkEnv.sh" ]; then
  . "$ZOOBINDIR"/../libexec/zkEnv.sh
else
  . "$ZOOBINDIR"/zkEnv.sh
fi

#echo "CLASSPATH=$CLASSPATH"

"$JAVA" -cp ../src:"$CLASSPATH" Client localhost:2181
