
#!/bin/bash
cd ../NPAIRS/data/$1
sleep 1
sh run_npairs.sh $2
wait
return 0

