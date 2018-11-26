# -*- coding: utf-8 -*-


import json
import time
import requests
import logging
from functools import reduce

logger = logging.getLogger('monitor')

# Record
class Record(json.JSONEncoder): 
    def default(self, o):  # pylint: disable=E0202
            return o.__dict__ 

class OpentsdbRecord(Record):
    def __init__(self, metric, timestamp, value, tags):
        ''' tags: dictionary '''
        self.metric = metric
        self.timestamp = timestamp
        self.value = value
        self.tags = tags
    def update_tags(self, tags):
        self.tags = tags


# Rest API Based on Http Client
class HttpClient(object):
    '''The Http based Rest client
    '''
    def __init__(self, ambari_domain, ambari_port, user_id, user_pw):
        self.__update_url(ambari_domain, ambari_port)
        self.__update_user(user_id, user_pw)

    def __update_user(self, user_id, user_pw):
        self.auth = (user_id, user_pw)

    def __update_url(self, ambari_domain, ambari_port):
        self.base_url = "http://"+ ambari_domain +":"+ ambari_port

    def __get_rest_api(self, api):
        self.__check_api(api)
        return self.base_url + api
    
    def __check_api(self, api):
        '''check api
        '''
        if api is None :
            raise ValueError('api should not be None.')
        if (not isinstance(api, str)):
            raise TypeError('api should be an instance of str.')
        if (not api.startswith("/")):
            raise ValueError('api should start with \'/\'')

    def get(self, strapi, fullapi=True, isauth=True):

        # get method
        if not fullapi:
            url = self.__get_rest_api(strapi)
        else:
            url = strapi
        try:
            if isauth:
                data = requests.get(url, auth=self.auth)
            else:
                data = requests.get(url)
        except:
            logger.error("An exception occurred, when calling get method on: " + url)
            data = None
       
        return data


class Client(object):
    def __init__(self, httpclient):

        if httpclient is None :
            raise ValueError('httpclient should not be None.')
        if (not isinstance(httpclient, HttpClient)):
            raise TypeError('httpclient should be an instance of HttpClient.')
        self.httpclient = httpclient


class AmsClient(Client):
    def get_clusters(self, fullapi=False):
        cluster_url = '/api/v1/clusters/'
        resp = self.httpclient.get(cluster_url, fullapi)
        if resp is not None:
            json_resp = json.loads(resp.text)
            tuple_clusters = [(item.get('Clusters', None)['cluster_name'], item.get('href', None)) for item in json_resp['items']]
        else:
            tuple_clusters = [()]

        return tuple_clusters
    
    def get_services(self, cluster_url, cluster_name=None, fullapi=True):
        services_url = cluster_url + '/services/'
        resp = self.httpclient.get(services_url, fullapi)
        if resp is not None:
            json_resp = json.loads(resp.text)
            tuples = [(item.get('ServiceInfo', None)['service_name'], item.get('href', None)) for item in json_resp['items']]
        else:
            tuples = [()]
        return tuples
    
    def get_components(self, service_url, service_name=None, fullapi=True):
        resp = self.httpclient.get(service_url, fullapi)
        if resp is not None:
            json_resp = json.loads(resp.text)
            tuples = [(item.get('ServiceComponentInfo', None)['component_name'], item.get('href', None)) for item in json_resp['components']]
        else:
            tuples = [()]
        return tuples

    def get_component_intances(self, component_url, component_name=None, fullapi=True):
        resp = self.httpclient.get(component_url, fullapi)
        if resp is not None:
            json_resp = json.loads(resp.text)
            tuples = [(item.get('HostRoles', None)['component_name'], item.get('href', None)) for item in json_resp['host_components']]
            service_metrics = json_resp.get('metrics', None)
            Service_component_info = json_resp.get('ServiceComponentInfo', None)
        else:
            tuples = [()]
            json_resp = None
            service_metrics = None
            Service_component_info = None
        return tuples, json_resp, service_metrics, Service_component_info

    def get_host_component(self, host_component_url, component_name=None, fullapi=True):
        resp = self.httpclient.get(host_component_url, fullapi)
        if resp is not None:
            json_resp = json.loads(resp.text)
        else:
            json_resp = None
        return json_resp

# Ambari metrics to opentsdb recorder mapper
class FieldsToRecordMapper(object):
    @staticmethod
    def map(json_obj, metrics_path, timestamp, tags_path, prefix_metric=None):
        ''' 将json对象映射到OpenTsdb Record
        josnobj: 
        metrics_path: metric路径列表， 如['metrics/dfs/datanode/blocks_written', 'metrics/dfs/datanode/blocks_read', ...]
        tags_path:  tag路径列表， 如['HostRoles/component_name', 'HostRoles/host_name', 'HostRoles/cluster_name']
        prefix_metric: useless
        '''
        dict_tags = FieldsToRecordMapper.get_tags(json_obj, tags_path)
        list_record = FieldsToRecordMapper.get_metrics(json_obj, metrics_path, timestamp, dict_tags)

        return list_record

    @staticmethod
    def get_tags(josnobj, tags_path):
        dict_tags = {}
        for tag in tags_path:

            tagsub = tag.split('/')
            tag_name = tagsub[-1]
            tmp_josnobj = josnobj
            for tg in tagsub:
                tmp_josnobj = tmp_josnobj[tg]

            tag_value = tmp_josnobj
            dict_tags[tag_name] = tag_value

        return dict_tags

    @staticmethod
    def get_metrics(josnobj, metrics_path, timestamp, dict_tags=None):
        list_record = []
        for metric in metrics_path:
            metric_name = metric.replace('/', '.')
            metricsub = metric.split('/')
            tmp_josnobj = josnobj
            for mt in metricsub:
                tmp_josnobj = tmp_josnobj.get(mt, None)
                if tmp_josnobj is None:
                    break
            
            if tmp_josnobj is None:
                metric_value = 0
            else:
                metric_value = tmp_josnobj
            list_record.append(OpentsdbRecord(metric_name, timestamp,  metric_value, dict_tags))

        return list_record

# Get the monitor data of service that managered by Ambari server
class Service(object):
    def __init__(self, ams_host, ams_port, ams_user, ams_password):
        httpclient = HttpClient(ams_host, ams_port, ams_user, ams_password)
        self.amsclient = AmsClient(httpclient)
        self.clean_api_arg()

    def metrics(self):
        """
        'metrics/dfs/datanode/DatanodeNetworkErrors', \
        'metrics/jvm/gcCount', \
        'metrics/jvm/gcTimeMillis', \
        'metrics/jvm/JvmMetrics/GcTimeMillisParNew']
        """
        raise NotImplementedError("metrics")

    def tags(self):
        """
        ['HostRoles/component_name', \
        'HostRoles/host_name', \
        'HostRoles/cluster_name']
        """
        return ['HostRoles/component_name', \
                'HostRoles/host_name', \
                'HostRoles/cluster_name']
    
    def service_tags(self):
        return ['ServiceComponentInfo/component_name', \
                'ServiceComponentInfo/service_name', \
                'ServiceComponentInfo/cluster_name']
    def service_metrics(self):
        raise NotImplementedError("service_metrics")
    
    def Service_component_info(self):
        return []


    def compoents_list_url(self, timestamp):
        raise NotImplementedError
    
    def get_metrics_api_arg(self):
        self.metrics_api_arg = self.merge_api_arg(self.metrics_api_arg, self.metrics())
        return self.metrics_api_arg
    
    def get_tags_api_arg(self):
        self.tags_api_arg = self.merge_api_arg(self.tags_api_arg, self.tags())
        return self.tags_api_arg
    
    def merge_api_arg(self, api_arg, arg_list):
        if api_arg is None:
            api_arg = reduce((lambda x, y: x + ',' + y), arg_list)
        return api_arg
    
    def clean_api_arg(self):
        self.metrics_api_arg = None
        self.tags_api_arg = None

    def get_compoents_list_url(self, timestamp):
        return self.compoents_list_url + 'href&_=' + str(timestamp)
    
    def do_service_metrics_calc(self, service_metrics, timestamp, service_tags):
        return []

    def do_work(self):
        timestamp = int(time.time())

        # host_components list (component_name, href)
        tuple_host_components, json_resp, service_metrics, Service_component_info = self.amsclient.get_component_intances(self.get_compoents_list_url(timestamp))

        service_record_list = None
        if service_metrics is not None:
            service_record_list = FieldsToRecordMapper.map(json_resp, self.service_metrics(), timestamp, self.service_tags())
        
        if Service_component_info is not None:
            Service_component_info_list = FieldsToRecordMapper.map(json_resp, self.Service_component_info(), timestamp, self.service_tags())
        
        service_metrics_calc_records_list = self.do_service_metrics_calc(service_metrics, timestamp, FieldsToRecordMapper.get_tags(json_resp, self.service_tags()))

        # each component
        record_list = self.foreach_host_component(tuple_host_components, timestamp)

        if service_record_list is not None:
            record_list = record_list + service_record_list
        
        if Service_component_info_list is not None:
            record_list = record_list + Service_component_info_list
        
        record_list = record_list + service_metrics_calc_records_list

        self.do_send2_opentsdb(record_list)
        
    
    def foreach_host_component(self, tuple_host_components, timestamp):
        url_arg = '?fields=' + self.get_metrics_api_arg() + ',' + self.get_tags_api_arg() + '&_=' + str(timestamp)

        json_obj_list = [self.amsclient.get_host_component(item[1] + url_arg) for item in tuple_host_components]
        record_list = [FieldsToRecordMapper.map(json_obj, self.metrics(), timestamp, self.tags()) for json_obj in json_obj_list]
        record_list = reduce(lambda x, y: x + y, record_list)
        return record_list
    
    def do_send2_opentsdb(self, records, nstep=10, tsdburl="http://util02:4242/api/put?details"):

        data = json.dumps(records, ensure_ascii=False, cls=Record).encode('utf-8')
        # print(data)
        data  = json.loads(data)
        if len(data) < nstep:
            resp = requests.post(tsdburl, json=data)
            # print(resp.text)
        else:
            num = 0
            dt_list = []
            session = requests.Session()
            for data_item in data:
                dt_list.append(data_item)
                num = num + 1
                if num > nstep:
                    resp = session.post(tsdburl, json=dt_list)
                    num = 0
                    dt_list = []
                    # print(resp.text)
            
            if dt_list is not None and len(dt_list) > 0:
                resp = session.post(tsdburl, json=dt_list)
                # print(resp.text)
        return resp

# 告警
class AlertMsg(object):
    ''' 告警消息
        Usage of AlertMsg
        msg_item = AlertMsg.generate_alert_msg_item(0, "I am 测试消息-python", "I am A TEST APP", "I am A TEST EVENT", [ "group1"], [ { "host": "127.0.0.1", "name": "HOST_127.0.0.1" } ], 1541763328, 3, "", 1)
        # msg = AlertMsg.generate_msg(msg_item)
        # msg = AlertMsg.generate_msg(msg_item, msg)
        msg = AlertMsg.generate_msg(msg_item)

        alertmsgs = [
            AlertMsg(0, "I am 测试消息", "I am A 测试 APP", "I am A 测试 EVENT ID", [ "group1"], [ { "host": "127.0.0.1", "name": "HOST_127.0.0.1" } ], 1541763328, 3, "", 1), 
            AlertMsg(0, "I am 测试消息2", "I am A 测试 APP2", "I am A 测试 EVENT ID2", [ "group1"], [ { "host": "127.0.0.1", "name": "HOST_127.0.0.1" } ], 1541763328, 3, "", 0)
        ]
        data = AlertMsg.generate_msg_from_alertmsg_list(alertmsgs)
        data = json.dumps(data, ensure_ascii=False).encode('utf-8')
    '''
    def __init__(self, alertId, description, detail, eventid, groups, hosts, lastchange, priority, url, value):
        '''
        0   OK
        1   WARNING
        2   CRITICAL
        3   UNKNOWN
        4   NONE
        '''
        self.alertId = alertId
        self.description = description
        self.detail = detail
        self.eventid = eventid
        self.groups = groups
        self.hosts = hosts
        self.lastchange = lastchange
        self.priority = priority
        self.url = url
        self.value = value
    

    @staticmethod
    def generate_alert_msg_item(alertId, description, detail, eventid, groups, hosts, lastchange, priority, url, value):
        return { "alertId": alertId,
            "description": description,
            "detail": detail, 
            "eventid": eventid,
            "groups": groups,
            "hosts": hosts,
            "lastchange": lastchange,
            "priority": priority,
            "value": value
        }
    @staticmethod
    def generate_msg(msg_item=None, msg=None):
        if msg_item is not None:
            if msg is None:
                msg = {"ambariAlertList":[]}
            
            msg["ambariAlertList"].append(msg_item)
        else:
            if msg is None:
                msg = {"ambariAlertList":[]}
        return msg
    @staticmethod
    def generate_msg_from_alertmsg_list(alert_msg_list, msg=None):

        msg_item_list = [AlertMsg.generate_alert_msg_item(alertmsg.alertId, \
                                                        alertmsg.description, \
                                                        alertmsg.detail, \
                                                        alertmsg.eventid, \
                                                        alertmsg.groups, \
                                                        alertmsg.hosts, \
                                                        alertmsg.lastchange, \
                                                        alertmsg.priority, \
                                                        alertmsg.url, \
                                                        alertmsg.value) for alertmsg in alert_msg_list]
        if msg is None:
            msg = {"ambariAlertList":[]}

        for msg_item in msg_item_list:
            msg["ambariAlertList"].append(msg_item)
        return msg
    
    @staticmethod
    def send_msg(msg, alert_url):
        ''' send alert msg to eagle
            msg generated by generate_msg_from_alertmsg_list or generate_msg
            alert_url: default is a develop enviroment
        '''
        data = json.dumps(msg, ensure_ascii=False).encode('utf-8')
        headers = {'Content-Type': 'application/json'}
        data = requests.post(alert_url, headers=headers, data=data)
        return data
    
    @staticmethod
    def send_alertmsg_list(alert_msg_list, alert_url):

        msg = AlertMsg.generate_msg_from_alertmsg_list(alert_msg_list)
        return AlertMsg.send_msg(msg, alert_url)
