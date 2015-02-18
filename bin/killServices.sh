kill $(ps aux | grep -v grep | grep 'zoo' | awk '{print $2}')
