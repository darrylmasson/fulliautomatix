from subprocess import PIPE
import os

popen_args = {'shell' : True, 'stdout' : PIPE, 'stderr' : PIPE}

cluster_fqdn='{cluster}.rcac.purdue.edu'
clusters = ['conte','halstead','rice','brown']
whoami = os.environ['USERNAME'] if 'USERNAME' in os.environ else os.environ['USER']
daq_fqdn='zinc.physics.purdue.edu'
raw_data_daq='/data/ASTERIX/raw'
raw_data_cluster='/scratch/{cluster}/{username_first_char}/{username}/asterix/raw'
log_directory='/depot/darkmatter/apps/asterix/logs'
sub_directory='/depot/darkmatter/apps/asterix/subs'
processed_directory='/depot/darkmatter/data/asterix/processed'
daq_software='obelix'
runs_db_address='/depot/darkmatter/apps/asterix/asterix_runs_db.db'
fortress_directory='/group/darkmatter/asterix'