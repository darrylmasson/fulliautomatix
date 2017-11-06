from subprocess import PIPE

popen_args = {'shell' : True, 'stdout' : PIPE, 'stderr' : PIPE}

cluster_name='conte'
cluster_fqdn='%s.rcac.purdue.edu' % cluster_name

zinc_fqdn='zinc.physics.purdue.edu'
raw_data_zinc='/data/ASTERIX/raw'
raw_data_cluster='/scratch/%s/d/dmasson/asterix/raw' % cluster_name
log_directory='/depot/darkmatter/apps/asterix/logs'
sub_directory='/depot/darkmatter/apps/asterix/subs'
processed_directory='/depot/darkmatter/data/asterix/processed'

runs_db_address='/depot/darkmatter/apps/asterix/asterix_runs_db.db'
