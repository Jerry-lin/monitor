# -*- coding: utf-8 -*-

from core import Service
from core import OpentsdbRecord

# HDFS
## hdfs - dn
class DatanodeService(Service):
    def __init__(self, ams_host, ams_port, ams_user, ams_password):
        super(DatanodeService, self).__init__(ams_host, ams_port, ams_user, ams_password)
        self.compoents_list_url = 'http://' + ams_host + ':' + str(ams_port) + \
            '/api/v1/clusters/fosun_bigdata/services/HDFS/components/DATANODE' + \
            '?fields=ServiceComponentInfo,host_components/'

    def metrics(self):
        '''jvm-rpc-service_compent
        '''
        return [ 
            # 'metrics/jvm/GcCountConcurrentMarkSweep', \
            # 'metrics/jvm/GcTimeMillisConcurrentMarkSweep', \
            'metrics/jvm/gcCount', 
            'metrics/jvm/gcTimeMillis', 
            'metrics/jvm/logError', 
            'metrics/jvm/logFatal', 
            'metrics/jvm/memHeapCommittedM', 
            'metrics/jvm/memHeapUsedM', 
            'metrics/jvm/memNonHeapCommittedM', 
            'metrics/jvm/memNonHeapUsedM', 
            'metrics/rpc/NumOpenConnections', 
            'metrics/rpc/RpcProcessingTime_avg_time', 
            'metrics/rpc/RpcProcessingTime_num_ops', 
            'metrics/rpc/RpcQueueTime_avg_time', 
            'metrics/rpc/RpcQueueTime_num_ops', 
            'metrics/rpc/callQueueLen', 
            'metrics/rpc/rpcAuthenticationFailures', 
            'metrics/rpc/rpcAuthorizationFailures', 
            'metrics/dfs/datanode/blocks_written', 
            'metrics/dfs/datanode/blocks_read',
            'metrics/dfs/datanode/FsyncNanosAvgTime',
            'metrics/dfs/datanode/FsyncNanosNumOps',
            'metrics/dfs/datanode/SendDataPacketBlockedOnNetworkNanosAvgTime',
            'metrics/dfs/datanode/SendDataPacketBlockedOnNetworkNanosNumOps',
            'metrics/dfs/datanode/SendDataPacketTransferNanosAvgTime', 
            'metrics/dfs/datanode/SendDataPacketTransferNanosNumOps', 
            'metrics/dfs/datanode/PacketAckRoundTripTimeNanosAvgTime', 
            'metrics/dfs/datanode/PacketAckRoundTripTimeNanosNumOps', 
            'metrics/dfs/datanode/DatanodeNetworkErrors' 
        ]
    def Service_component_info(self):
        return [ 
            'ServiceComponentInfo/started_count',
            'ServiceComponentInfo/total_count'
        ]

## hdfs - nn
class NamenodeService(Service):
    def __init__(self, ams_host, ams_port, ams_user, ams_password):
        super(NamenodeService, self).__init__(ams_host, ams_port, ams_user, ams_password)
        self.compoents_list_url = 'http://' + ams_host + ':' + str(ams_port) + \
            '/api/v1/clusters/fosun_bigdata/services/HDFS/components/NAMENODE' + \
            '?fields=ServiceComponentInfo,metrics,host_components/'                      
 
    def metrics(self):
        '''jvm-rpc-service_compent
        '''
        return [ 
            'metrics/jvm/gcCount', 
            'metrics/jvm/gcTimeMillis', 
            'metrics/jvm/JvmMetrics/GcCountParNew', 
            'metrics/jvm/JvmMetrics/GcTimeMillisParNew', 
            'metrics/jvm/JvmMetrics/GcTotalExtraSleepTime', 
            'metrics/jvm/JvmMetrics/GcNumWarnThresholdExceeded', 
            'metrics/rpc/client/RpcProcessingTime_avg_time', 
            'metrics/rpc/client/RpcProcessingTime_num_ops', 
            'metrics/rpc/client/RpcQueueTime_avg_time', 
            'metrics/rpc/client/RpcQueueTime_num_ops', 
            'metrics/rpc/client/callQueueLen', 
            'metrics/rpc/client/NumOpenConnections', 
            'metrics/rpcdetailed/client/addBlock_avg_time', 
            'metrics/rpcdetailed/client/addBlock_num_ops',
            'metrics/dfs/FSNamesystem/ExpiredHeartbeats', 
            'metrics/dfs/FSNamesystem/TransactionsSinceLastCheckpoint', 
            'metrics/dfs/FSNamesystem/TransactionsSinceLastLogRoll', 
            'metrics/dfs/FSNamesystem/TotalLoad' 
        ]
    def service_metrics(self):
        return [ 
            'metrics/dfs/FSNamesystem/BlockCapacity', 
            'metrics/dfs/FSNamesystem/BlocksTotal', 
            'metrics/dfs/FSNamesystem/CapacityTotalGB', 
            'metrics/dfs/FSNamesystem/CapacityUsedGB', 
            'metrics/dfs/namenode/TotalFiles', 
            'metrics/dfs/namenode/TotalBlocks', 
            'metrics/dfs/namenode/TotalFileOps' 
        ]
    
    def do_service_metrics_calc(self, service_metrics, timestamp, service_tags):
        metrics_dfs_FSNamesystem = service_metrics['dfs']['FSNamesystem']
        free_hdfs_per = (float(metrics_dfs_FSNamesystem['CapacityTotalGB']) - float(metrics_dfs_FSNamesystem['CapacityUsedGB'])) / float(metrics_dfs_FSNamesystem['CapacityTotalGB']) * 100
        free_blocks_per = (float(metrics_dfs_FSNamesystem['BlockCapacity']) - float(metrics_dfs_FSNamesystem['BlocksTotal'])) / float(metrics_dfs_FSNamesystem['BlockCapacity']) * 100

        records = [
            OpentsdbRecord('metrics.dfs.FSNamesystem.FreeHdfsCapacityPercentage', timestamp, round(free_hdfs_per, 2), service_tags), 
            OpentsdbRecord('metrics.dfs.FSNamesystem.FreeBlockCapacityPercentage', timestamp, round(free_blocks_per, 2), service_tags), 
        ]
        return records



# yarn
## yarn - rm
class ResourceManagerService(Service):
    '''Resource Manager Grafana
    '''
    def __init__(self, ams_host, ams_port, ams_user, ams_password):
        super(ResourceManagerService, self).__init__(ams_host, ams_port, ams_user, ams_password)
        self.compoents_list_url = 'http://' + ams_host + ':' + str(ams_port) + \
            '/api/v1/clusters/fosun_bigdata/services/YARN/components/RESOURCEMANAGER' + \
            '?fields=ServiceComponentInfo,metrics,host_components/'                      
 
    def metrics(self):
        return [ 
            'metrics/jvm/HeapMemoryMax', 
            'metrics/jvm/HeapMemoryUsed', 
            'metrics/jvm/NonHeapMemoryUsed', 
            'metrics/jvm/gcCount', 
            'metrics/jvm/gcTimeMillis', 
            'metrics/jvm/logError', 
            'metrics/jvm/logFatal', 
            'metrics/jvm/memHeapCommittedM', 
            'metrics/jvm/memHeapUsedM', 
            'metrics/jvm/memNonHeapCommittedM', 
            'metrics/jvm/memNonHeapUsedM', 
            'metrics/jvm/JvmMetrics/GcCountParNew', 
            'metrics/jvm/JvmMetrics/GcTimeMillisParNew', 
            'metrics/jvm/JvmMetrics/GcTotalExtraSleepTime', 
            'metrics/jvm/JvmMetrics/GcNumWarnThresholdExceeded', 
            'metrics/rpc/RpcProcessingTime_avg_time', 
            'metrics/rpc/RpcProcessingTime_num_ops', 
            'metrics/rpc/RpcQueueTime_avg_time', 
            'metrics/rpc/RpcQueueTime_num_ops', 
            'metrics/rpc/callQueueLen', 
            'metrics/rpc/NumOpenConnections', 
            'metrics/rpc/RpcAuthenticationFailures', 
            'metrics/rpc/RpcAuthorizationFailures' 
        ]

    def service_metrics(self):
        return [ 
            'metrics/jvm/HeapMemoryUsed', 
            'metrics/yarn/ClusterMetrics/NumActiveNMs', 
            'metrics/yarn/ClusterMetrics/NumLostNMs', 
            'metrics/yarn/ClusterMetrics/NumUnhealthyNMs',
            'metrics/yarn/Queue/root/AppsRunning',
            'metrics/yarn/Queue/root/AppsPending',
            'metrics/yarn/Queue/root/AppsFailed', 
            'metrics/yarn/Queue/root/AppsCompleted', 
            'metrics/yarn/Queue/root/AllocatedMB',
            'metrics/yarn/Queue/root/AllocatedVCores',
            'metrics/yarn/Queue/root/AvailableMB',
            'metrics/yarn/Queue/root/AvailableVCores'
        ]
    def Service_component_info(self):
        return [ 
            'ServiceComponentInfo/started_count',
            'ServiceComponentInfo/total_count',
            'ServiceComponentInfo/rm_metrics/cluster/activeNMcount'
        ]

## yarn - nm
class NodeManagerService(Service):
    def __init__(self, ams_host, ams_port, ams_user, ams_password):
        super(NodeManagerService, self).__init__(ams_host, ams_port, ams_user, ams_password)
        self.compoents_list_url = 'http://' + ams_host + ':' + str(ams_port) + \
            '/api/v1/clusters/fosun_bigdata/services/YARN/components/NODEMANAGER' + \
            '?fields=host_components/'                      
 
    def metrics(self):
        return [ 
                'metrics/jvm/gcCount', 
                'metrics/jvm/gcTimeMillis', 
                'metrics/jvm/logError', 
                'metrics/jvm/logFatal', 
                'metrics/jvm/memHeapCommittedM', 
                'metrics/jvm/memHeapUsedM', 
                'metrics/jvm/memNonHeapCommittedM', 
                'metrics/jvm/memNonHeapUsedM', 
                'metrics/jvm/JvmMetrics/MemHeapMaxM', 
                'metrics/jvm/JvmMetrics/MemMaxM', 
                'metrics/jvm/JvmMetrics/MemNonHeapMaxM', 
                'metrics/rpc/RpcProcessingTime_avg_time', 
                'metrics/rpc/RpcProcessingTime_num_ops', 
                'metrics/rpc/RpcQueueTime_avg_time', 
                'metrics/rpc/RpcQueueTime_num_ops', 
                'metrics/rpc/callQueueLen', 
                'metrics/rpc/NumOpenConnections',
                'metrics/rpc/rpcAuthenticationFailures',  
                'metrics/rpc/rpcAuthorizationFailures',  
                'metrics/yarn/ContainersRunning', 
                'metrics/yarn/ContainersFailed', 
                'metrics/yarn/ContainersKilled', 
                'metrics/yarn/ContainersCompleted', 
                'metrics/yarn/AvailableGB', 
                'metrics/yarn/AllocatedGB', 
                'metrics/yarn/AvailableVCores', 
                'metrics/yarn/AllocatedVCores', 
                'metrics/yarn/GoodLogDirsDiskUtilizationPerc', 
                'metrics/yarn/GoodLocalDirsDiskUtilizationPerc', 
                'metrics/yarn/BadLogDirs', 
                'metrics/yarn/BadLocalDirs', 
                'metrics/yarn/ContainerLaunchDurationAvgTime'
                ]             

## yarn - queue
class YarnQueueService(Service):
    pass
## yarn - app
class YarnAppService(Service):
    pass


class HBaseMasterService(Service):
    '''Resource Manager Grafana
    '''
    def __init__(self, ams_host, ams_port, ams_user, ams_password):
        super(HBaseMasterService, self).__init__(ams_host, ams_port, ams_user, ams_password)
        self.compoents_list_url = 'http://' + ams_host + ':' + str(ams_port) + \
            '/api/v1/clusters/fosun_bigdata/services/HBASE/components/HBASE_MASTER' + \
            '?fields=host_components/' 
            # '?fields=ServiceComponentInfo,metrics,host_components/'                      
 
    def metrics(self):
        return [ \
            'metrics/jvm/HeapMemoryMax', \
            'metrics/jvm/HeapMemoryUsed', \
            'metrics/jvm/NonHeapMemoryUsed', \
            'metrics/jvm/memMaxM', \
            'metrics/master/AssignmentManger/ritCount', \
            'metrics/master/AssignmentManger/ritCountOverThreshold', \
            'metrics/master/AssignmentManger/ritOldestAge', \
            'metrics/hbase/master/DeadRegionServers', \
            'metrics/hbase/master/RegionServers', \
            'metrics/hbase/master/AverageLoad', \
            'metrics/hbase/master/cluster_requests'
        ]

    # def service_metrics(self):
    #     return [ \
    #         'ServiceComponentInfo/rm_metrics/cluster/activeNMcount' , \
    #         'metrics/jvm/HeapMemoryUsed', \
    #         'metrics/yarn/ClusterMetrics/NumActiveNMs', \
    #         'metrics/yarn/ClusterMetrics/NumLostNMs', \
    #         'metrics/yarn/ClusterMetrics/NumUnhealthyNMs', \
    #         'metrics/yarn/Queue/root/AppsRunning', \
    #         'metrics/yarn/Queue/root/AppsPending', \
    #         'metrics/yarn/Queue/root/AppsFailed', \
    #         'metrics/yarn/Queue/root/AppsCompleted', \
    #         'metrics/yarn/Queue/root/AvailableMB' \
    #     ]

class HBaseRegionServerService(Service):
    '''Resource Manager Grafana
    '''
    def __init__(self, ams_host, ams_port, ams_user, ams_password):
        super(HBaseRegionServerService, self).__init__(ams_host, ams_port, ams_user, ams_password)
        self.compoents_list_url = 'http://' + ams_host + ':' + str(ams_port) + \
            '/api/v1/clusters/fosun_bigdata/services/HBASE/components/HBASE_REGIONSERVER' + \
            '?fields=host_components/'                      
 
    def metrics(self):
        return [ 
            'metrics/jvm/gcCount', 
            'metrics/jvm/gcTimeMillis', 
            'metrics/jvm/logError', 
            'metrics/jvm/logFatal', 
            'metrics/jvm/logInfo', 
            'metrics/jvm/logWarn', 
            'metrics/jvm/memHeapCommittedM', 
            'metrics/jvm/memHeapUsedM', 
            'metrics/jvm/memNonHeapCommittedM', 
            'metrics/jvm/memNonHeapUsedM', 
            'metrics/jvm/threadsBlocked', 
            'metrics/jvm/threadsNew', 
            'metrics/jvm/threadsRunnable', 
            'metrics/jvm/threadsTerminated', 
            'metrics/jvm/threadsTimedWaiting', 
            'metrics/jvm/threadsWaiting', 
            'metrics/rpc/ReceivedBytes', 
            'metrics/rpc/RpcProcessingTime_avg_time', 
            'metrics/rpc/RpcProcessingTime_num_ops', 
            'metrics/rpc/RpcQueueTime_avg_time', 
            'metrics/rpc/RpcQueueTime_num_ops', 
            'metrics/rpc/rpcAuthorizationFailures', 
            'metrics/rpc/rpcAuthorizationSuccesses', 
            'metrics/rpc/NumOpenConnections', 
            'metrics/rpc/callQueueLen', 
            'metrics/hbase/regionserver/Append_95th_percentile', 
            'metrics/hbase/regionserver/Append_num_ops', 
            'metrics/hbase/regionserver/Increment_95th_percentile', 
            'metrics/hbase/regionserver/Increment_num_ops', 
            'metrics/hbase/regionserver/blockCacheCount', 
            'metrics/hbase/regionserver/blockCacheEvictedCount', 
            'metrics/hbase/regionserver/blockCacheFree', 
            'metrics/hbase/regionserver/blockCacheHitCount', 
            'metrics/hbase/regionserver/blockCacheHitRatio', 
            'metrics/hbase/regionserver/blockCacheMissCount', 
            'metrics/hbase/regionserver/blockCacheSize', 
            'metrics/hbase/regionserver/compactionQueueSize', 
            'metrics/hbase/regionserver/deleteRequestLatency_75th_percentile', 
            'metrics/hbase/regionserver/deleteRequestLatency_95th_percentile', 
            'metrics/hbase/regionserver/deleteRequestLatency_99th_percentile', 
            'metrics/hbase/regionserver/deleteRequestLatency_max', 
            'metrics/hbase/regionserver/deleteRequestLatency_mean', 
            'metrics/hbase/regionserver/deleteRequestLatency_median', 
            'metrics/hbase/regionserver/deleteRequestLatency_min', 
            'metrics/hbase/regionserver/deleteRequestLatency_num_ops', 
            'metrics/hbase/regionserver/flushQueueSize', 
            'metrics/hbase/regionserver/flushTime_avg_time', 
            'metrics/hbase/regionserver/flushTime_num_ops', 
            'metrics/hbase/regionserver/getRequestLatency_75th_percentile', 
            'metrics/hbase/regionserver/getRequestLatency_95th_percentile', 
            'metrics/hbase/regionserver/getRequestLatency_99th_percentile', 
            'metrics/hbase/regionserver/getRequestLatency_max', 
            'metrics/hbase/regionserver/getRequestLatency_mean', 
            'metrics/hbase/regionserver/getRequestLatency_median', 
            'metrics/hbase/regionserver/getRequestLatency_min', 
            'metrics/hbase/regionserver/getRequestLatency_num_ops', 
            'metrics/hbase/regionserver/hlogFileCount', 
            'metrics/hbase/regionserver/memstoreSize', 
            'metrics/hbase/regionserver/mutationsWithoutWALCount', 
            'metrics/hbase/regionserver/mutationsWithoutWALSize', 
            'metrics/hbase/regionserver/percentFilesLocal', 
            'metrics/hbase/regionserver/putRequestLatency_75th_percentile', 
            'metrics/hbase/regionserver/putRequestLatency_95th_percentile', 
            'metrics/hbase/regionserver/putRequestLatency_99th_percentile', 
            'metrics/hbase/regionserver/putRequestLatency_max', 
            'metrics/hbase/regionserver/putRequestLatency_mean', 
            'metrics/hbase/regionserver/putRequestLatency_median', 
            'metrics/hbase/regionserver/putRequestLatency_min', 
            'metrics/hbase/regionserver/putRequestLatency_num_ops', 
            'metrics/hbase/regionserver/readRequestsCount', 
            'metrics/hbase/regionserver/regions', 
            'metrics/hbase/regionserver/requests', 
            'metrics/hbase/regionserver/slowAppendCount', 
            'metrics/hbase/regionserver/slowDeleteCount', 
            'metrics/hbase/regionserver/slowGetCount', 
            'metrics/hbase/regionserver/slowIncrementCount', 
            'metrics/hbase/regionserver/slowPutCount', 
            'metrics/hbase/regionserver/storefileIndexSizeMB', 
            'metrics/hbase/regionserver/storefiles', 
            'metrics/hbase/regionserver/stores', 
            'metrics/hbase/regionserver/totalStaticBloomSizeKB', 
            'metrics/hbase/regionserver/totalStaticIndexSizeKB', 
            'metrics/hbase/regionserver/updatesBlockedTime', 
            'metrics/hbase/regionserver/writeRequestsCount' 
        ]
