
#!/bin/bash

#generic execution script for launching the program, this is for NPARIS program specific only
cd ../NPAIRS/data/$1
sleep 1
bash run_npairs.sh $2
retcode=$?
exit $retcode
