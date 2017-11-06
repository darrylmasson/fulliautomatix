from subprocess import Popen
import os
import logging

import config
from RunsDB import RunsDB
import SubFile

class Daemon:
    '''Daemon base class'''
    ssh_command = 'ssh {username}@{host} {command}'
    def __init__(self):
        self.runs_to_do = []
        self.logger = logging.getLogger(__name__)
        self.dry = False

    def FindRuns(self):
        raise NotImplementedError()

    def DoOneRun(self, name):
        raise NotImplementedError()

    def DoAllRuns(self):
        self.FindRuns()
        self.logger.info('%s found %i runs' % (self.__class__.__name__, len(self.runs_to_do)))
        for name in self.runs_to_do:
            self.DoOneRun(name)

class TransferDaemon(Daemon):
    '''Daemon to move files from zinc to cluster
    '''
    rsync_command = 'rsync --quiet --remove-source-files --archive --compress --whole-file -e ssh {source} {dest}'  # rsync does checksum

    def FindRuns(self):
        self.runs_to_do = []
        for run in RunsDB.Select(selection='name', cuts='raw_location==\'zinc\''):
            self.runs_to_do.append(run[0])

    def DoOneRun(self, name):
        format_dict = {'username' : os.environ['USERNAME'],
                       'host' : config.zinc_fqdn,
                       'command' : self.rsync_command.format(source=os.path.join(config.raw_data_zinc, name),
                                                             dest='%s:%s' % (config.cluster_fqdn, config.raw_data_cluster))}
        transfer_command = self.ssh_command.format(**format_dict)
        if self.dry:
            print(transfer_command)
        else:
            self.logger.info('transferring %s' % name)
            RunsDB.Update(update='raw_status=\'copying\'', cuts='name==\'%s\'' % name)
            _, err = Popen(transfer_command, **config.popen_args).communicate()
            if len(err):
                self.logger.error('rsync error on %s: %s' % (name, err.decode()))
                return -1
            else:
                rm_command = self.ssh_command.format(**{'username' : os.environ['USERNAME'],
                                                        'host' : config.zinc_fqdn,
                                                        'command' : 'rm -r %s' % os.path.join(config.raw_data_zinc, name)})
                _, err = Popen(rm_command, **config.popen_args).communicate()
                if len(err):
                    self.logger.error('rm error on %s: %s' % (name, err.decode()))
                    return -1
            self.logger.debug('transferred %s' % name)
            RunsDB.Update(update='raw_status=\'ondeck\',raw_location=\'cluster\'', cuts='name==\'%s\'' % name)

        return 0


class ProcessDaemon(Daemon):
    '''Daemon to drop jobs into the cluster for processing
    '''
    qsub_command = 'qsub {subfile}'

    def FindRuns(self):
        self.runs_to_do = []
        for row in RunsDB.Select(selection='name,events,source', cuts='raw_location==\'cluster\' AND raw_status==\'ondeck\' AND processed_status!=\'processed\''):
            self.runs_to_do.append({'name' : row[0],
                                    'events' : int(row[1]),
                                    'source' : row[2]})

    def ProcessTime(self, events, source):
        fudge_factor = 1.2
        rate = 6e4 if source == 'LED' else 1e6/3600 # TODO calibrate
        minutes = int(max(events/rate, 5)*fudge_factor)
        hours = int(minutes/60)
        return ('%02i:%02i:00' % (hours, minutes), hours + minutes/60)

    def DoOneRun(self, run_info):
        filename = os.path.join(config.sub_directory, run_info['name'] + '.sub')
        walltime_str, walltime_f = self.ProcessTime(run_info['events'], run_info['source'])
        format_dict = {'username' : os.environ['USERNAME'],
                       'host' : config.cluster_fqdn,
                       'command' : self.qsub_command.format(subfile=filename)}
        queue_command = self.ssh_command.format(**format_dict)
        sub_file = SubFile.ProcessJob.format(**{
                    'name' : run_info['name'],
                    'walltime' : walltime_str,
                    'queue' : 'standby' if walltime_f < 4.0 else 'physics',
                    'nodecount' : 'nodes=1:ppn=1',
                    'nodeaccess' : 'shared',
                    'config' : 'ASTERIX_LED' if run_info['source'] == 'LED' else 'ASTERIX',
                    'raw_data' : '%s' % os.path.join(config.raw_data_cluster, run_info['name']),
                    'processed' : '%s' % os.path.join(config.processed_directory, run_info['name']),
                })
        if self.dry:
            print(sub_file)
            print(queue_command)
        else:
            self.logger.debug('Making subfile %s' % filename)
            with open(filename, 'w') as f:
                f.write(sub_file)
            self.logger.info('queueing %s' % run_info['name'])
            RunsDB.Update(update='processed_status=\'queueing\'', cuts='name==\'%s\'' % run_info['name'])
            _, err = Popen(queue_command, **config.popen_args).communicate()
            if len(err):
                self.logger.error('Error queueing %s: %s' % (run_info['name'], err.decode()))
                return -1
        return 0
