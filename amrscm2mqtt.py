#!/usr/bin/env python3

'''
Runs rtlamr to watch for IDM broadcasts from power meter. If meter id
is in the list, usage is sent to 'readings/{meter id}/meter_reading'
topic on the MQTT broker specified in settings.

WATCHED_METERS = A Python list indicating those meter IDs to record and post.
MQTT_HOST = String containing the MQTT server address.
MQTT_PORT = An int containing the port the MQTT server is active on.

'''

import subprocess
import signal
import sys
import time
import configparser
import logging
import ssl
import json
from paho.mqtt import client as mqtt_client

# uses signal to shutdown and hard kill opened processes and self
def shutdown( signum, frame ):
    subprocess.call( '/usr/bin/pkill -9 rtlamr', shell=True )
    subprocess.call( '/usr/bin/pkill -9 rtl_tcp', shell=True )
    sys.exit( 0 )

def on_connected( client, userdata, flags, rc ):
    logger = logging.getLogger( 'mqtt' )
    logger.info( 'mqtt connected' )

def stop( client ):
    logger = logging.getLogger( 'mqtt' )
    logger.info( 'mqtt shutting down...' )
    client.disconnect()
    client.loop_stop()
    
def send_mqtt( client, topic, payload ):
    logger = logging.getLogger( 'mqtt' )
    logger.debug( 'publishing %s to %s...', payload, topic )
    try:
        client.publish( topic, payload )
    except Exception as ex:
        logger.exception( ex )

def client_connect( client, config ):
    logger = logging.getLogger( 'mqtt' )
    client.loop_start()
    client.enable_logger()
    client.tls_set( config['mqtt']['ca'], tls_version=ssl.PROTOCOL_TLSv1_2 )
    client.on_connect = on_connected
    logger.info( 'connecting to MQTT at %s:%d...',
        config['mqtt']['host'], config.getint( 'mqtt', 'port' ) )
    client.username_pw_set(
        config['mqtt']['user'], config['mqtt']['password'] )
    client.connect( config['mqtt']['host'], config.getint( 'mqtt', 'port' ) )

def main():

    logging.basicConfig( level=logging.INFO )
    logger = logging.getLogger( 'main' )

    config = configparser.RawConfigParser()
    config.read( '/etc/amrscm2mqtt.ini' )
    watched_meters = config['meter']['ids'].split( ',' )
    
    signal.signal( signal.SIGTERM, shutdown )
    signal.signal( signal.SIGINT, shutdown )
    
    client = mqtt_client.Client(
        config['mqtt']['uid'], True, None, mqtt_client.MQTTv31 )
    client_connect( client, config )
    
    # start the rtl_tcp program
    rtltcp = subprocess.Popen(
        ["/usr/bin/rtl_tcp > /dev/null 2>&1 &"], shell=True,
        stdin=None, stdout=None, stderr=None, close_fds=True )
    
    time.sleep( 2 )
    
    # start the rtlamr program.
    rtlamr = subprocess.Popen(
        ['/usr/local/bin/rtlamr', '-msgtype=scm', '-format=json'],
        stdout=subprocess.PIPE )
    
    while True:
        try:
            # rtlamr's readline returns byte list,
            # remove whitespace and convert to string
            amrline = rtlamr.stdout.readline().strip().decode()

            # make sure the meter id is one we want
            flds = json.loads( amrline )
            if len( watched_meters ) and \
            str( flds['Message']['ID'] ) not in watched_meters:
                continue

            logger.info( 'found reading from meter: %d', flds['Message']['ID'] )
    
            # get some required info: current meter reading, 
            # current interval id, most recent interval
            read_cur = flds['Message']['Consumption']
    
            current_reading_in_kwh = \
                (read_cur * config.getint( 'meter', 'multiplier' ) / 1000)
    
            send_mqtt(
                client,
                'amrscm/{}/meter_reading'.format( flds['Message']['ID'] ),
                '{}'.format( current_reading_in_kwh ) )
    
        except Exception as e:
            logger.exception( e )
            time.sleep( 2 )

if '__main__' == __name__:
    main()
