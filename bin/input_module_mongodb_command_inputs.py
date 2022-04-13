# encoding = utf-8

import os
import sys
import time
import datetime
import json
from pymongo import mongo_client

'''
    IMPORTANT
    Edit only the validate_input and collect_events functions.
    Do not edit any other part in this file.
    This file is generated only once when creating the modular input.
'''
'''
# For advanced users, if you want to create single instance mod input, uncomment this method.
def use_single_instance_mode():
    return True
'''


def validate_input(helper, definition):
    """Implement your own validation logic to validate the input stanza configurations"""
    # This example accesses the modular input variable
    ip = definition.parameters.get('ip', None)
    port = definition.parameters.get('port', None)
    username = definition.parameters.get('username', None)
    password = definition.parameters.get('password', None)
    database = definition.parameters.get('database', None)
    command = definition.parameters.get('command', None)


def collect_events(helper, ew):
    results = None
    opt_ip = helper.get_arg('ip')
    opt_port = helper.get_arg('port')
    opt_username = helper.get_arg('username')
    opt_password = helper.get_arg('password')
    opt_database = helper.get_arg('database')
    opt_command = helper.get_arg('command')

    try:
        # 链接mongodb
        client = mongo_client.MongoClient(opt_ip, int(opt_port))
        helper.log_info("<<<<创建数据库会话>>>>")
        db = client[opt_database]
        db.authenticate(opt_username, opt_password)

        for command in opt_command:
            sourcetype = "mongo:command:" + command
            try:
                results = db.command(command)
            except Exception as auth:
                helper.log_error(str(command) + "命令执行异常：" + str(auth))
            # 数据处理
            res = get_data(results, command)
            helper.log_info("命令:" + str(command) + " | 数据内容:" + str(res))
            # 数据写入
            write_data(helper, ew, res, sourcetype)
        # 关闭数据链接
        client.close()
        helper.log_info(">>>>关闭数据库会话<<<<")

    except Exception as connect:
        helper.log_error("Mongodb 链接异常:"+str(connect))


def get_data(results_data, command):
    if command == "serverStatus":
        json_data = {'command': "serverStatus", 'mem': results_data['mem'], 'network': results_data['network'],
                       'pid': results_data['pid'], 'uptime': results_data['uptime'], 'version': results_data['version'],
                       'connections': results_data['connections'], 'extra_info': results_data['extra_info'],
                       'flowControl': results_data['flowControl'], 'opcounters': results_data['opcounters'],
                       'asserts': results_data['asserts']}
        res = json.dumps(json_data, default=str, ensure_ascii=False)
    else:
        res = json.dumps(results_data, default=str, ensure_ascii=False)
    return res


def write_data(helper, ew, data, sourcetype):
    opt_ip = helper.get_arg('ip')
    event = helper.new_event(host=opt_ip, source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=sourcetype, data=data)
    ew.write_event(event)
    """
    Implement your data collection logic here

    # The following examples get the arguments of this input. 多实例
    # Note, for single instance mod input, args will be returned as a dict.
    # For multi instance mod input, args will be returned as a single value.
    opt_ip = helper.get_arg('ip')
    opt_username = helper.get_arg('username')
    opt_password = helper.get_arg('password')
    opt_database = helper.get_arg('database')
    opt_command = helper.get_arg('command')
    
    # In single instance mode, to get arguments of a particular input, use 单实例
    opt_ip = helper.get_arg('ip', stanza_name)
    opt_username = helper.get_arg('username', stanza_name)
    opt_password = helper.get_arg('password', stanza_name)
    opt_database = helper.get_arg('database', stanza_name)
    opt_command = helper.get_arg('command', stanza_name)

    # get input type
    helper.get_input_type()

    # The following examples get input stanzas.
    # get all detailed input stanzas
    helper.get_input_stanza()
    # get specific input stanza with stanza name
    helper.get_input_stanza(stanza_name)
    # get all stanza names
    helper.get_input_stanza_names()

    # The following examples get options from setup page configuration.
    # get the loglevel from the setup page
    loglevel = helper.get_log_level()
    # get proxy setting configuration
    proxy_settings = helper.get_proxy()
    # get account credentials as dictionary
    account = helper.get_user_credential_by_username("username")
    account = helper.get_user_credential_by_id("account id")
    # get global variable configuration
    global_userdefined_global_var = helper.get_global_setting("userdefined_global_var")

    # The following examples show usage of logging related helper functions.
    # write to the log for this modular input using configured global log level or INFO as default
    helper.log("log message")
    # write to the log using specified log level
    helper.log_debug("log message")
    helper.log_info("log message")
    helper.log_warning("log message")
    helper.log_error("log message")
    helper.log_critical("log message")
    # set the log level for this modular input
    # (log_level can be "debug", "info", "warning", "error" or "critical", case insensitive)
    helper.set_log_level(log_level)

    # The following examples send rest requests to some endpoint.
    response = helper.send_http_request(url, method, parameters=None, payload=None,
                                        headers=None, cookies=None, verify=True, cert=None,
                                        timeout=None, use_proxy=True)
    # get the response headers
    r_headers = response.headers
    # get the response body as text
    r_text = response.text
    # get response body as json. If the body text is not a json string, raise a ValueError
    r_json = response.json()
    # get response cookies
    r_cookies = response.cookies
    # get redirect history
    historical_responses = response.history
    # get response status code
    r_status = response.status_code
    # check the response status, if the status is not sucessful, raise requests.HTTPError
    response.raise_for_status()

    # The following examples show usage of check pointing related helper functions.
    # save checkpoint
    helper.save_check_point(key, state)
    # delete checkpoint
    helper.delete_check_point(key)
    # get checkpoint
    state = helper.get_check_point(key)

    # To create a splunk event
    helper.new_event(data, time=None, host=None, index=None, source=None, sourcetype=None, done=True, unbroken=True)
    """

    '''
    # The following example writes a random number as an event. (Multi Instance Mode)
    # Use this code template by default.
    import random
    data = str(random.randint(0,100))
    event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=data)
    ew.write_event(event)
    '''

    '''
    # The following example writes a random number as an event for each input config. (Single Instance Mode)
    # For advanced users, if you want to create single instance mod input, please use this code template.
    # Also, you need to uncomment use_single_instance_mode() above.
    import random
    input_type = helper.get_input_type()
    for stanza_name in helper.get_input_stanza_names():
        data = str(random.randint(0,100))
        event = helper.new_event(source=input_type, index=helper.get_output_index(stanza_name), sourcetype=helper.get_sourcetype(stanza_name), data=data)
        ew.write_event(event)
    '''
