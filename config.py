from subprocess import PIPE
import os

# common arguments to pass to Popen
popen_args = {'shell' : True, 'stdout' : PIPE, 'stderr' : PIPE}

# fully qualified domain names
cluster_fqdn='{cluster}.rcac.purdue.edu'

# which clusters we can use
cluster = 'brown'

# username of process owner
whoami = os.environ['LOGNAME']

# daq machine name
daq_fqdn='zinc.physics.purdue.edu'

# where on daq machine the raw data is written
raw_data_daq='/data/ASTERIX/raw'

# where on the cluster the raw data is stored
raw_data_cluster='/depot/darkmatter/data/asterix/raw'

# where the log files are stored
log_directory='/depot/darkmatter/apps/asterix/logs'

# where the job submission files are written
sub_directory='/depot/darkmatter/apps/asterix/subs'

# where processed data is written
processed_directory='/depot/darkmatter/data/asterix/processed'

# which daq software we're using
daq_software='obelix'

# address for the runs db
runs_db_address='/depot/darkmatter/apps/asterix/asterix_runs_db.db'

# where to put data on fortress
fortress_directory='/group/darkmatter/asterix'

# if a run has been 'transferring' for longer than this, restart it
max_transfer_time = 1800