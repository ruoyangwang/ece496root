
#!/usr/bin/env bash

ZOOBIN="${BASH_SOURCE-$0}"
ZOOBIN="$(dirname "${ZOOBIN}")"
ZOOBINDIR="$(cd "${ZOOBIN}"; pwd)"

if [ -e "$ZOOBIN/../libexec/zkEnv.sh" ]; then
. "$ZOOBINDIR"/../libexec/zkEnv.sh
else
. "$ZOOBINDIR"/zkEnv.sh
fi

#echo "CLASSPATH=$CLASSPATH"

"$JAVA" -cp ../src:"$CLASSPATH" Worker $1:2181
