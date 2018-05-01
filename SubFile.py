'''File containing the submission script templates for processing and archiving
'''


ProcessJob = r'''#!/bin/sh

#PBS -l walltime={walltime}
#PBS -l naccesspolicy={nodeaccess}
#PBS -l {nodecount}
#PBS -q {queue}
#PBS -e /depot/darkmatter/apps/asterix/subs/{name}.stderr
#PBS -o /depot/darkmatter/apps/asterix/subs/{name}.stdout

date -Iseconds

export PATH=/depot/darkmatter/apps/anaconda3-4.4.0/bin:$PATH
cd /depot/darkmatter/apps/asterix/daemons
source /depot/darkmatter/apps/asterix/daemons/ThreadsafeActivate asterix
python UpdateDB.py start {name} $PBS_JOBID

date -Iseconds

echo "paxer --config {config} --input {raw_data} --output {processed}"
paxer --config {config} --input {raw_data} --output {processed}
if [ $? -eq 0 ]; then
    cd /depot/darkmatter/apps/asterix/daemons
    python UpdateDB.py end {name}
else
    cd /depot/darkmatter/apps/asterix/daemons
    python UpdateDB.py fail {name}
fi
cd /depot/darkmatter/apps/asterix/subs
rm {name}.sub
date -Iseconds
'''

CompressJob = r'''#!/bin/sh

#PBS -l walltime={walltime}
#PBS -l naccesspolicy={nodeaccess}
#PBS -l {nodecount}
#PBS -q {queue}
#PBS -e /depot/darkmatter/apps/asterix/subs/{name}.stderr
#PBS -o /depot/darkmatter/apps/asterix/subs/{name}.stdout

date -Iseconds

export PATH=/depot/darkmatter/apps/anaconda3-4.4.0/bin:$PATH
cd /depot/darkmatter/apps/asterix/daemons
python UpdateDB.py compress start {name}

cd $RCAC_SCRATCH
cd asterix/raw

echo "tar --create --file {name}.tar.gz --preserve-permissions --remove-files --verbose --gzip {name}"
tar --create --file {name}.tar.gz --preserve-permissions --remove-files --verbose --gzip {name}
if [ $? -eq ]; then
    cd /depot/darkmatter/app/asterix/daemons
    python UpdateDB.py compress end {name}
fi

date -Iseconds
'''