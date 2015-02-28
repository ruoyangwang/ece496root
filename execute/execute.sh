
#!/bin/bash
cd ../NPAIRS/data/$1
sleep 1
bash run_npairs.sh $2
retcode=$?
exit $retcode
