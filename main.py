# -*- coding: utf-8 -*-

from service import NamenodeService
from service import DatanodeService
from service import ResourceManagerService
from service import NodeManagerService
from service import HBaseMasterService
from service import HBaseRegionServerService

import sys
import getopt


class Usage(Exception):
    """ Usage
        -s | --services namenode[,datanode,resourcemanager,nodemanager,hbasemaster,hbaseregionserver]
    """
    def __init__(self, msg):
        self.msg = msg
def get_service_by_name(service_name, AMBARI_DOMAIN, AMBARI_PORT, AMBARI_USER_ID, AMBARI_USER_PW):
    if "namenode" == service_name:
        service = NamenodeService(AMBARI_DOMAIN, AMBARI_PORT, AMBARI_USER_ID, AMBARI_USER_PW)
    elif "datanode" == service_name:
        service = DatanodeService(AMBARI_DOMAIN, AMBARI_PORT, AMBARI_USER_ID, AMBARI_USER_PW)
    elif "resourcemanager" == service_name:
        service = ResourceManagerService(AMBARI_DOMAIN, AMBARI_PORT, AMBARI_USER_ID, AMBARI_USER_PW)
    elif "nodemanager" == service_name:
        service = NodeManagerService(AMBARI_DOMAIN, AMBARI_PORT, AMBARI_USER_ID, AMBARI_USER_PW)
    elif "hbasemaster" == service_name:
        service = HBaseMasterService(AMBARI_DOMAIN, AMBARI_PORT, AMBARI_USER_ID, AMBARI_USER_PW)
    elif "hbaseregionserver" == service_name:
        service = HBaseRegionServerService(AMBARI_DOMAIN, AMBARI_PORT, AMBARI_USER_ID, AMBARI_USER_PW)
    else:
        raise RuntimeError("Unkown service")
    return service

def do_components(components):

    AMBARI_DOMAIN = 'xx.xxxx.com'
    AMBARI_PORT = '8080'
    AMBARI_USER_ID = 'xxx'
    AMBARI_USER_PW = 'xxxx'

    component_list = components.split(',')
    for compoent in component_list:
        service = get_service_by_name(compoent, AMBARI_DOMAIN, AMBARI_PORT, AMBARI_USER_ID, AMBARI_USER_PW)
        service.do_work()


def main(argv=None):
    if argv is None:
        argv = sys.argv

    components = None
    try:
        try:
            opts, args = getopt.getopt(argv[1:],'hs:v',['help','services','version'])
        except getopt.GetoptError as err:
            raise Usage(err.msg)

        for o, a in opts:
            if o in ('-h','--help'):
                print(Usage.__doc__)
                return 0
            if o in ('-v','--version'):
                print("[*] Version is 0.01 ")
                return 0
            if o in ('-s','--services'):
                components = a
    except Usage as err:
        print(err.msg, file=sys.stderr)
        print("for help use --help", file=sys.stderr)
        return 2
    
    if components is None:
        print("for help use --help", file=sys.stderr)
        return 0
    return do_components(components)

if __name__ == '__main__':
    sys.exit(main())


