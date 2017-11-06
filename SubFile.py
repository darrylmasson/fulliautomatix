
ProcessJob = r'''#!/bin/sh

#PBS -l walltime={walltime}
#PBS -l naccesspolicy={nodeaccess}
#PBS -l {nodecount}
#PBS -q {queue}
#PBS -e /depot/darkmatter/apps/asterix/subs/{name}.stderr
#PBS -o /depot/darkmatter/apps/asterix/subs/{name}.stdout

RAW={raw_data}
PROCESSED={processed}

date -Iseconds

export PATH=/depot/darkmatter/apps/anaconda3/bin:$PATH
cd /depot/darkmatter/apps/asterix/daemons
source ./ThreadsafeActivate asterix
python UpdateDB.py start {name}

date -Iseconds

echo "paxer --config {config} --input $RAW --output $PROCESSED"
paxer --config {config} --input $RAW --output $PROCESSED
if [ $? -eq 0 ]; then
    echo "Success"

    cd /depot/darkmatter/apps/asterix/daemons
    python UpdateDB.py end {name}
fi
date -Iseconds
'''
