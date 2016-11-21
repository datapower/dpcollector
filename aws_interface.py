import datetime
import boto3
import dpower_tools
from systemlog import *
import time, os

'''
Set timezone to be compatible with zabbix server
'''
os.environ['TZ'] = dict_setup["metric_sent_timezone"]
timestamp = int(time.time())

class AWSInterface(object):
    cloudwatch = None

    period_seconds = dict_setup["aws_cloud_watch_period"]
    now_utc = datetime.datetime.utcnow()  # .isoformat()
    now_diff = now_utc - datetime.timedelta(minutes=dict_setup["aws_diff_between_start_end_minutes"])
    starttime = dpower_tools.datetime_iso8601(now_diff)
    endtime = dpower_tools.datetime_iso8601(now_utc)

    def __init__(self, rds_name, aws_access_key_id, aws_secret_access_key, aws_region):
        self.rds_name = rds_name
        try:
            if len(aws_secret_access_key) < 20 or aws_secret_access_key is None:
                self.cloudwatch = boto3.client(service_name='cloudwatch', region_name=aws_region)
            else:
                self.cloudwatch = boto3.client(service_name='cloudwatch', region_name=aws_region,
                                               aws_access_key_id=aws_access_key_id,
                                               aws_secret_access_key=aws_secret_access_key)
        except Exception as e:
            logger.debug("Error to get access on AWS API {}".format(e))

    def parseavarage(self, clouldwatchstats):
        try:
            datapoints = clouldwatchstats['Datapoints']
            if len(datapoints) > 0:
                avarage = float(datapoints[0]['Average'])
                return avarage
            else:
                return 0
        except Exception as e:
            print("Error on get AWS credentials")
            logger.debug("Something was wrong in function getavarage {}".format(e))

    def cloudwatchRDSQuery(self, metricname, starttime, endtime, unit='Percent', period=60):
        rds_name = self.rds_name
        cloudwatch = self.cloudwatch
        try:
            query = {
                "Namespace": 'AWS/RDS',
                "MetricName": metricname,
                "Dimensions": [{'Name': 'DBInstanceIdentifier', 'Value': rds_name}],
                "StartTime": starttime,
                "EndTime": endtime,
                "Period": period,
                "Statistics": ["Average"],
                "Unit": unit
            }
            logger.debug("Get RDS Metrics with query: {}".format(query))
            response = cloudwatch.get_metric_statistics(**query)
            return response
        except Exception as e:
            logger.debug("Error to execute this query {} on  AWS... The error returned is {}".format(query, e))
            return None

    def getRDSCPU(self):
        result = self.cloudwatchRDSQuery('CPUUtilization', starttime=self.starttime, endtime=self.endtime,
                                         unit='Percent', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_cpu = {
            "aws.rds[cpu]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_cpu

    def getRDSReadIOPS(self):
        result = self.cloudwatchRDSQuery('ReadIOPS', starttime=self.starttime, endtime=self.endtime,
                                         unit='Count/Second', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_read_iops = {
            "aws.rds[read_iops]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_read_iops

    def getRDSReadLatency(self):
        result = self.cloudwatchRDSQuery('ReadLatency', starttime=self.starttime, endtime=self.endtime, unit='Seconds',
                                         period=self.period_seconds)
        avg = self.parseavarage(result)
        status_read_latency = {
            "aws.rds[read_latency]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_read_latency

    def getRDSWriteIOPS(self):
        result = self.cloudwatchRDSQuery('WriteIOPS', starttime=self.starttime, endtime=self.endtime,
                                         unit='Count/Second', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_write_iops = {
            "aws.rds[write_iops]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_write_iops

    def getRDSReadThroughput(self):
        result = self.cloudwatchRDSQuery('ReadThroughput', starttime=self.starttime, endtime=self.endtime,
                                         unit='Bytes/Second', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_read_throughput = {
            "aws.rds[read_throughput]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_read_throughput

    def getRDSWriteLatency(self):
        result = self.cloudwatchRDSQuery(metricname='WriteLatency', starttime=self.starttime, endtime=self.endtime,
                                         unit='Seconds',
                                         period=self.period_seconds)
        avg = self.parseavarage(result)
        status_write_latency = {
            "aws.rds[write_latency]": avg,
            "timestamp": timestamp

        }
        return status_write_latency

    def getRDSWriteThroughput(self):
        result = self.cloudwatchRDSQuery(metricname='WriteThroughput', starttime=self.starttime, endtime=self.endtime,
                                         unit='Bytes/Second', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_write_throughput = {
            "aws.rds[write_throughput]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_write_throughput

    def getRDSSwapUsage(self):
        result = self.cloudwatchRDSQuery('SwapUsage', starttime=self.starttime, endtime=self.endtime, unit='Bytes',
                                         period=self.period_seconds)
        avg = self.parseavarage(result)
        status_swap_usage = {
            "aws.rds[swap_usage]": avg,
            "timestamp": timestamp

        }
        return status_swap_usage

    def getRDSFreeableMemory(self):
        result = self.cloudwatchRDSQuery('FreeableMemory', starttime=self.starttime, endtime=self.endtime, unit='Bytes',
                                         period=self.period_seconds)
        avg = self.parseavarage(result)
        status_freeable_memomory = {
            "aws.rds[freeable_memomory]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_freeable_memomory

    def getRDSFreeStorageSpace(self):
        result = self.cloudwatchRDSQuery('FreeStorageSpace', starttime=self.starttime, endtime=self.endtime,
                                         unit='Bytes', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_freeable_storage_space = {
            "aws.rds[freeable_storage_space]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_freeable_storage_space

    def getRDSDiskQueueDepth(self):
        result = self.cloudwatchRDSQuery('DiskQueueDepth', starttime=self.starttime, endtime=self.endtime, unit='Count',
                                         period=self.period_seconds)
        avg = self.parseavarage(result)
        status_disk_queue_depth = {
            "aws.rds[disk_queue_depth]": avg,
            "timestamp": timestamp

        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_disk_queue_depth

    def getRDSNetworkTransmitThroughput(self):
        result = self.cloudwatchRDSQuery('NetworkTransmitThroughput', starttime=self.starttime, endtime=self.endtime,
                                         unit='Bytes/Second', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_network_transmit_throughput = {
            "aws.rds[network_transmit_throughput]": avg,
            "timestamp": timestamp
        }
        return status_network_transmit_throughput

    def getRDSNetworkReceiveThroughput(self):
        result = self.cloudwatchRDSQuery(metricname='NetworkReceiveThroughput', starttime=self.starttime,
                                         endtime=self.endtime,
                                         unit='Bytes/Second', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_network_receive_throughput = {
            "aws.rds[network_receive_throughput]": avg,
            "timestamp": timestamp
        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_network_receive_throughput

    def getRDSDatabaseConnections(self):
        result = self.cloudwatchRDSQuery('DatabaseConnections', starttime=self.starttime, endtime=self.endtime,
                                         unit='Count', period=self.period_seconds)
        avg = self.parseavarage(result)
        status_database_connections = {
            "aws.rds[database_connections]": avg,
            "timestamp": timestamp
        }
        logger.debug("{}: {}".format(__name__, avg))
        return status_database_connections


class AWSStats(object):
    def __init__(self):
        rds_name = dict_setup['aws_rds_host_name']
        aws_access_key_id = dict_setup['aws_access_key_id']
        aws_secret_access_key = dict_setup['aws_secret_access_key']
        aws_region = dict_setup['aws_region']

        self.aws = AWSInterface(rds_name=rds_name, aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key, aws_region=aws_region)

    def get_rds_cpu(self):
        result = dpower_tools.trapper(self.aws.getRDSCPU())
        return result

    def get_rds_read_iops(self):
        result = dpower_tools.trapper(self.aws.getRDSReadIOPS())
        return result

    def get_rds_write_iops(self):
        result = dpower_tools.trapper(self.aws.getRDSWriteIOPS())
        return result

    def get_rds_read_latency(self):
        result = dpower_tools.trapper(self.aws.getRDSReadLatency())
        return result

    def get_rds_write_latency(self):
        result = dpower_tools.trapper(self.aws.getRDSWriteLatency())
        return result

    def get_rds_read_throughput(self):
        result = dpower_tools.trapper(self.aws.getRDSReadThroughput())
        return result

    def get_rds_write_throughput(self):
        result = dpower_tools.trapper(self.aws.getRDSWriteThroughput())
        return result

    def get_rds_swap_usage(self):
        result = dpower_tools.trapper(self.aws.getRDSSwapUsage())
        return result

    def get_rds_freeable_memory(self):
        result = dpower_tools.trapper(self.aws.getRDSFreeableMemory())
        return result

    def get_rds_free_storage_space(self):
        result = dpower_tools.trapper(self.aws.getRDSFreeStorageSpace())
        return result

    def get_rds_disk_queue_depth(self):
        result = dpower_tools.trapper(self.aws.getRDSDiskQueueDepth())
        return result

    def get_rds_network_transmit_throughput(self):
        result = dpower_tools.trapper(self.aws.getRDSNetworkTransmitThroughput())
        return result

    def get_rds_network_receive_throughput(self):
        result = dpower_tools.trapper(self.aws.getRDSNetworkReceiveThroughput())
        return result

    def get_rds_database_connections(self):
        result = dpower_tools.trapper(self.aws.getRDSDatabaseConnections())
        return result

    def sentStats(self):
        logger.info("Sending cloudwatch rds events...")
        self.get_rds_cpu()
        self.get_rds_read_iops()
        self.get_rds_write_iops()
        self.get_rds_read_latency()
        self.get_rds_write_latency
        self.get_rds_read_throughput()
        self.get_rds_write_throughput()
        self.get_rds_swap_usage()
        self.get_rds_freeable_memory()
        self.get_rds_free_storage_space()
        self.get_rds_disk_queue_depth()
        self.get_rds_network_transmit_throughput()
        self.get_rds_network_receive_throughput()
        self.get_rds_database_connections()
