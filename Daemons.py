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

    @classmethod
    def SetCluster(cls, cluster):
        cls.default_cluster = cluster

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
    '''Daemon to move files from daq to cluster
    '''
    rsync_command = 'rsync --quiet --remove-source-files --archive --compress --whole-file -e ssh {source} {dest}'

    def FindRuns(self):
        self.runs_to_do = []
        if IsDAQLive():
            return
        for row in RunsDB.Select(selection='name,comments', cuts='raw_location==\'zinc\''):
            if '#DNT' not in row[1].upper():
                self.runs_to_do.append(row[0])

    def DoOneRun(self, name):
        transfer_command = self.ssh_command.format(**{
            'username' : config.whoami,
            'host' : config.zinc_fqdn,
            'command' : self.rsync_command.format(source=os.path.join(config.raw_data_daq, name),
                                                  dest='%s:%s' % (config.cluster_fqdn.format(cluster=self.default_cluster),
                                                                  config.raw_data_cluster.format(cluster=self.default_cluster,
                                                                                                 username_first_char=config.whoami[0],
                                                                                                 username=config.whoami
                                                                                                 )
                                                                  )
                                                 )
            })
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
                rm_command = self.ssh_command.format(**{'username' : config.whoami,
                                                        'host' : config.zinc_fqdn,
                                                        'command' : 'rm -r %s' % os.path.join(config.raw_data_zinc, name)
                                                     })
                _, err = Popen(rm_command, **config.popen_args).communicate()
                if len(err):
                    self.logger.error('rm error on %s: %s' % (name, err.decode()))
            self.logger.debug('transferred %s' % name)
            RunsDB.Update(update='raw_status=\'ondeck\',raw_location=\'%s/%s\'', cuts='name==\'%s\'' % (self.default_cluster, config.whoami, name))

        return 0

    def IsDAQLive(self):
        command = self.ssh_command.format(username=config.whoami,
                                          host=config.zinc_fqdn,
                                          command = 'top -b -n 1 | grep {daq}'.format(daq=config.daq_software))
        out, err = Popen(command, **config.popen_args).communicate()
        if len(err):
            self.logger.error('Could not check daq status: %s' % err.decode())
            return -1
        elif len(out):
            self.logger.info('DAQ is currently live, not transferring anything')
            return -1
        return 0

class ProcessDaemon(Daemon):
    '''Daemon to drop jobs into the cluster for processing
    '''
    qsub_command = 'qsub {subfile}'

    def FindRuns(self):
        self.runs_to_do = []
        for row in RunsDB.Select(selection='name,events,source,raw_location,comments', cuts='raw_location!=\'zinc\' AND raw_status==\'ondeck\' AND processed_status!=\'processed\''):
            if '#DNP' not in row[4].upper():
                self.runs_to_do.append({'name' : row[0],
                                        'events' : int(row[1]),
                                        'source' : row[2],
                                        'location' : {'cluster' : row[3].split('/')[0],
                                                      'owner' : row[3].split('/')[1]},
                                        })

    def ProcessTime(self, events, source):
        fudge_factor = 1.2
        rate = 6e4 if source == 'LED' else 1e6/3600 # TODO calibrate
        minutes = int(max(events/rate, 5)*fudge_factor)
        return ('%02i:%02i:00' % (minutes/60, minutes % 60), minutes/60)

    def DoOneRun(self, run_info):
        filename = os.path.join(config.sub_directory, run_info['name'] + '.sub')
        walltime_str, walltime_f = self.ProcessTime(run_info['events'], run_info['source'])
        queue_command = self.ssh_command.format(**{
            'username' : config.whoami,
            'host' : config.cluster_fqdn.format(cluster=run_info['location']['cluster']),
            'command' : self.qsub_command.format(subfile=filename)
            })
        sub_file = SubFile.ProcessJob.format(**{
                    'name' : run_info['name'],
                    'walltime' : walltime_str,
                    'queue' : 'standby' if walltime_f < 4.0 else 'physics',
                    'nodecount' : 'nodes=1:ppn=1',
                    'nodeaccess' : 'shared',
                    'config' : 'ASTERIX_LED' if run_info['source'] == 'LED' else 'ASTERIX',
                    'raw_data' : '%s' % os.path.join(config.raw_data_cluster.format(cluster=run_info['location']['cluster'],
                                                                                    username_first_char=run_info['location']['owner'][0],
                                                                                    username=run_info['location']['owner']),
                                                     run_info['name']),
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

class CompressDaemon(Daemon):
    '''Daemon to handle compressing data
    '''

    def FindRuns(self):
        self.runs_to_do = []
        for row in RunsDB.Select(selection='name,raw_location,raw_size', cuts='raw_status==\'compressing\' OR raw_status==\'ondeck\' AND processed_status==\'processed\''):
            self.runs_to_do.append({'name' : row[0],
                                    'location' : row[1],
                                    'size' : row[2],})

    def CompressTime(self, size):
        fudge_factor = 1.2
        size_mult = {'M' : 1/1024,
                     'G' : 1,
                     'T' : 1024,}
        rate = 0.6  # gzip does 10 MB/s = 0.6 GB/min
        minutes = int(float(size[:-1])*size_mult[size[-1]]/rate * fudge_factor)
        return ('%02i:%02i:00' % (minutes/60, minutes % 60), minutes/60)

    def DoOneRun(self, run_info):
        filename = os.path.join(config.sub_directory, run_info['name'] + '.sub')
        walltime_str, walltime_f = self.CompressTime(run_info['size'])
        queue_command = self.ssh_command.format(**{
            'username' : config.whoami,
            'host' : config.cluster_fqdn.format(cluster=run_info['location']['cluster']),
            'command' : self.qsub_command.format(subfile=filename)
            })
        sub_file = SubFile.CompressJob.format(**{
                    'name' : run_info['name'],
                    'walltime' : walltime_str,
                    'queue' : 'standby' if walltime_f < 4.0 else 'physics',
                    'nodecount' : 'nodes=1:ppn=1',
                    'nodeaccess' : 'shared',
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
