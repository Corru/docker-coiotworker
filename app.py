#!/usr/bin/env python2
# -*- coding: utf-8 -*-
######################################################################
#  ###  ###  ###  ###  #   # ###  ##### #### ####    ###  ###  ###   #
# #    #   # #  # #  # #   # #  #   #   #___ #   #   #  # #  #   #   #
# #    #   # ###  ###  #   # ###    #   #    #   #   ###  ###    #   #
#  ###  ###  #  # #  #  ###  #      #   #### ####    #    #  # ##  # #
######################################################################
#   Corrupted Prj.                                                   #
#   Copyright (c) 2017 <corruptedproject@yandex.ru>                  #
#   Contributors: Mathtin, Plaguedo                                  #
#   This file is released under the MIT license.                     #
######################################################################

__author__ = "Mathtin, Plaguedo"
__date__ = "$5.07.2018 03:11:27$"

from ConfigParser import RawConfigParser
import paho.mqtt.client as mqtt
import mysql.connector as mysql
import thread, re, sys

def read_hosts():
    with open('/etc/hosts') as f:
        hosts = f.readlines()
    hosts = [x.split() for x in hosts]
    for host in hosts:
        if len(host) == 2 and re.match('10\.\d+\.\d+\.\d+', host[0]):
            return host[0], host[1]

def make_config(path='/app/main.cnf'):
    configfile = open(path, 'wb')
    config = RawConfigParser()
    config.add_section('Main')
    config.set('Main', 'key', 'secret')
    config.set('Main', 'mqtt_host', '127.0.0.1')
    config.set('Main', 'mqtt_port', 1883)
    config.set('Main', 'mysql_host', '127.0.0.1')
    config.set('Main', 'mysql_port', 3306)
    config.set('Main', 'mysql_username', 'root')
    config.set('Main', 'mysql_password', 'secret')
    config.set('Main', 'mysql_database', 'coiot')
    config.write(configfile)
    configfile.close()

def read_config(path='/app/main.cnf'):
    try:
        configfile = open(path, 'rb')
    except IOError:
        make_config(path)
        configfile = open(path, 'rb')
    if configfile.closed:
        raise Exception('Can not open or create config file')
    config = RawConfigParser()
    config.readfp(configfile)
    cnf = {}
    cnf['key'] = config.get('Main', 'key')
    cnf['mqtt_host'] = config.get('Main', 'mqtt_host')
    cnf['mqtt_port'] = config.getint('Main', 'mqtt_port')
    cnf['mysql_host'] = config.get('Main', 'mysql_host')
    cnf['mysql_port'] = config.getint('Main', 'mysql_port')
    cnf['mysql_username'] = config.get('Main', 'mysql_username')
    cnf['mysql_password'] = config.get('Main', 'mysql_password')
    cnf['mysql_database'] = config.get('Main', 'mysql_database')
    configfile.close()
    return cnf

def get_userlist(cnx, cnf):
    cursor = cnx.cursor()
    query = ('SELECT username FROM users u JOIN worker_ownership o ON o.user_id = u.id WHERE o.worker_id = %s')
    cursor.execute(query, (cnf['db_id'],))
    users = [x[0] for x in list(cursor)]
    cursor.close()
    return users

def incstat(cnx, cnf, usr, dev):
    cursor = cnx.cursor()
    query = (' SELECT s.id FROM statistics s'
             ' JOIN device_ownership o ON o.device_id = s.device_id'
             ' JOIN devices d ON d.id = o.device_id'
             ' JOIN users u ON u.id = o.user_id'
             ' WHERE u.username = %s AND d.device = %s')
    cursor.execute(query, (usr, dev))
    row = list(cursor)
    if len(row) > 0:
        query = ('UPDATE statistics SET value = value + 1 WHERE id = %s')
        cursor.execute(query, (row[0][0],))
    else:
        query = (' INSERT INTO `statistics` (`device_id`, `type`, `value`) '
                 ' VALUES ((SELECT d.id FROM device_ownership o'
                            ' JOIN devices d ON d.id = o.device_id'
                            ' JOIN users u ON u.id = o.user_id'
                            ' WHERE u.username = %s AND d.device = %s), \'CNT\', \'1\')')
        cursor.execute(query, (usr, dev))
    cnx.commit()

class MQTTClient(object):
    def __init__(self, cnf, db):
        self.host = cnf['mqtt_host']
        self.port = cnf['mqtt_port']
        self.id = cnf['id']
        self.key = cnf['key']
        self.cnf = cnf
        self.db = db
        self.client = mqtt.Client()
        self.client.username_pw_set('wrk-' + cnf['id'], password=cnf['key'])
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
    
    def subscribe(self):
        self.client.unsubscribe('coiot/#')
        self.client.subscribe('coiot/workers/' + self.id)
        self.client.subscribe('coiot/workers/' + self.id + '/#')
        users = get_userlist(self.db, self.cnf)
        for user in users:
            self.client.subscribe('coiot/users/' + user + '/devices/+/events')
    
    def _on_connect(self, client, userdata, flags, rc):
        print ('[.] Connected to mqtt')
        self.subscribe()
        
    def _on_message(self, client, userdata, msg):
        topic = msg.topic.split('/')
        if topic[1] == 'workers' and msg.payload == 'subscribe':
            self.subscribe()
            return
        elif topic[1] == 'users' and len(topic) > 5 and topic[5] == 'events':
            username = topic[2]
            device = topic[4]
            incstat(self.db, self.cnf, username, device)
        
    def run(self):
        self.client.connect_async(self.host, port=self.port)
        self.client.loop_forever()

def main(argv):
    print ('[*] Reading config')
    if len(argv) > 1:
        cnf = read_config(argv[1])
    else:
        cnf = read_config()
    cnf['ip'], cnf['id'] = read_hosts()
    print ('[.] Host: ' + cnf['id'] + ' ' + cnf['ip'])
    print ('[*] Connecting to database')
    try:
        cnx = mysql.connect(user=cnf['mysql_username'], 
                            password=cnf['mysql_password'],
                            host=cnf['mysql_host'],
                            port=cnf['mysql_port'],
                            database=cnf['mysql_database'],
                            autocommit=True)
    except mysql.Error as e:
        print e
        return
    cursor = cnx.cursor()
    query = ('SELECT id FROM workers WHERE `ip` = %s')
    print ('[*] Updating self in database')
    cursor.execute(query, (cnf['ip'],))
    row = list(cursor)
    if len(row) > 0:
        query = ('UPDATE `workers` SET `ip` = %s, `key` = %s WHERE `name` = %s')
        cursor.execute(query, (cnf['ip'], cnf['key'], cnf['id']))
        cnf['db_id'] = row[0][0]
    else:
        query = ('INSERT INTO `workers` (`ip`, `key`, `name`) VALUES (%s, %s, %s)')
        cursor.execute(query, (cnf['ip'], cnf['key'], cnf['id']))
        cnf['db_id'] = cursor.lastrowid
    cursor.close()
    app = MQTTClient(cnf, cnx)
    print ('[*] Connecting to mqtt')
    app.run()

if __name__ == "__main__":
    main(sys.argv)
