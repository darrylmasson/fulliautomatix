#!/depot/darkmatter/etc/conda/env/asterix/bin/python3

import os
import argparse
import logging
import datetime

import config
import Daemons
from RunsDB import RunsDB

parser = argparse.ArgumentParser(description='Fulliautomatix')
parser.add_argument('--transfer', action='store_true', help='Transfer data from daq to cluster')
parser.add_argument('--process', action='store_true', help='Process data')
parser.add_argument('--reprocess', nargs='+', help='Reprocessed given runs. Planned feature')
parser.add_argument('--log', default='info', type=str, choices=['debug','info','warning','error','critical'], help='Logging level')
parser.add_argument('--run', nargs='+', help='Specific runs (name or number) to handle. Space-separated list')
parser.add_argument('--dry_run', action='store_true', help='Dry run (don\'t actually transfer or process anything)')
parser.add_argument('--cluster', default=config.clusters[0], type=str, choices=config.clusters, help='Which cluster to process on')

args = parser.parse_args()

def main():
    print('Welcome to Fulliautomatix!')

    logging.basicConfig(filename=os.path.join(config.log_directory,
                                              datetime.date.today().isoformat() + '.log'),
                        level=getattr(logging, args.log.upper()),
                        format='%(asctime)s | %(module)s | %(message)s')

    RunsDB.Initialize()

    Daemons.Daemon.SetCluster(args.cluster)

    transfer_daemon = Daemons.TransferDaemon()
    process_daemon = Daemons.ProcessDaemon()

    transfer_daemon.dry = args.dry_run
    process_daemon.dry = args.dry_run

    if args.transfer:
        if args.run:
            print('Transferring %i runs' % len(args.run))
            for run in args.run:
                if '_' in run:  # run name, yyyymmdd_hhmm
                    transfer_daemon.DoOneRun(run)
                else: # run number
                    for row in RunsDB.Select(selection='name',cuts='number==%s' % run):
                        transfer_daemon.DoOneRun(row[0])
        else:
            print('Transferring runs...')
            transfer_daemon.DoAllRuns()

    if args.process:
        if args.run:
            print ('Processing %i runs' % len(args.run))
            for run in args.run:
                if '_' in run:  # run name, yyyymmdd_hhmm
                    process_daemon.DoOneRun(run)
                else: # run number
                    for row in RunsDB.Select(selection='name',cuts='number==%s' % run):
                        process_daemon.DoOneRun(row[0])
        else:
            print('Processing runs...')
            process_daemon.DoAllRuns()

    RunsDB.Shutdown()
    return 0

if __name__ == '__main__':
    main()