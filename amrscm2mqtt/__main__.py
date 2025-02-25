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
import argparse
from paho.mqtt import client as mqtt_client
from datetime import datetime

class AMRSCM2MQTT( object ):

    def __init__( self, config_path ):
        self.logger = logging.getLogger( 'amrscm2mqtt' )
        self.prev_flds_incr = None
        self.prev_reading = 0
        self.rate_updated_once = False # Protect against bad rate data.
        self.meter_rate = 0
        self.config = configparser.RawConfigParser()
        self.config_path = config_path
        self.config.read( self.config_path )
        self.watched_meters = self.config['meter']['ids'].split( ',' )
        self.client = mqtt_client.Client(
            self.config['mqtt']['uid'], True, None, mqtt_client.MQTTv31 )
        self.client_connect()

        # start the rtl_tcp program
        self.rtltcp = subprocess.Popen(
            ["/usr/bin/rtl_tcp > /dev/null 2>&1 &"], shell=True,
            stdin=None, stdout=None, stderr=None, close_fds=True )

        time.sleep( 2 )

        # start the rtlamr program.
        self.rtlamr = subprocess.Popen(
            ['/usr/local/bin/rtlamr', '-msgtype=scm', '-format=json'],
            stdout=subprocess.PIPE )

    # uses signal to shutdown and hard kill opened processes and self
    def shutdown( self, signum, frame ):
        subprocess.call( '/usr/bin/pkill -9 rtlamr', shell=True )
        subprocess.call( '/usr/bin/pkill -9 rtl_tcp', shell=True )
        sys.exit( 0 )

    def on_connected( self, client, userdata, flags, rc ):
        self.logger.info( 'mqtt connected' )

    #def on_message(client, userdata, msg):
    #    logger = logging.getLogger( 'mqtt' )
    #    logger.debug( 'message: %s: %s', msg.topic, msg.payload )

    def stop( self, client ):
        self.logger.info( 'mqtt shutting down...' )
        client.disconnect()
        client.loop_stop()

    def send_mqtt( self, topic, payload, retain=False ):
        self.logger.debug( 'publishing %s to %s...', payload, topic )
        try:
            self.client.publish( topic, payload, retain=retain )
        except Exception as ex:
            self.logger.exception( ex )

    def client_connect( self, verbose=False ):
        self.client.loop_start()
        if verbose:
            self.client.enable_logger()
        self.client.tls_set( self.config['mqtt']['ca'], tls_version=ssl.PROTOCOL_TLSv1_2 )
        self.client.on_connect = self.on_connected
        self.logger.info( 'connecting to MQTT at %s:%d...',
            self.config['mqtt']['host'], self.config.getint( 'mqtt', 'port' ) )
        self.client.username_pw_set(
            self.config['mqtt']['user'], self.config['mqtt']['password'] )
        self.client.connect( self.config['mqtt']['host'], self.config.getint( 'mqtt', 'port' ) )

    def save_last_reading_change( self, flds ):
        self.prev_flds_incr = flds
        self.config['persist']['reading'] = '{}'.format( flds['Message']['Consumption'] )
        self.logger.debug( 'saving last reading to config...' )
        try:
            with open( self.config_path, 'w' ) as config_file:
                self.config.write( config_file )
        except Exception as ex:
            self.logger.exception( ex )

    def get_line( self ):
        # rtlamr's readline returns byte list,
        # remove whitespace and convert to string
        return self.rtlamr.stdout.readline().strip().decode()

    def process_reading( self, line ):

        # Make sure the meter id is one we want.
        flds = json.loads( line )
        if len( self.watched_meters ) and \
        str( flds['Message']['ID'] ) not in self.watched_meters:
            return

        self.logger.debug( 'found reading from meter: %s', flds )

        # Convert timestamp to native object for calculations later.
        flds['Timestamp'] = datetime.strptime(
            flds['Time'].split( '.' )[0], '%Y-%m-%dT%H:%M:%S' )

        # Check for counter bug.
        if 0 < self.prev_reading and \
        0 < flds['Message']['Consumption'] > self.prev_reading:
            if flds['Message']['Consumption'] > self.prev_reading + 10 or \
            flds['Message']['Consumption'] < self.prev_reading:
                self.logger.error(
                    'erroneous count (%d) detected; dropped', flds['Message']['Consumption'] )
                return

        # Check for counter reset.
        if self.config.getint( 'persist', 'reading' ) > flds['Message']['Consumption']:
            self.logger.info( 'counter was reset' )
            self.send_mqtt(
                '{}/{}/meter_reading_reset'.format(
                    self.config['mqtt']['topic'], flds['Message']['ID'] ),
                datetime.now().isoformat(),
                retain=True )

        if self.prev_flds_incr and \
        self.prev_flds_incr['Message']['Consumption'] < flds['Message']['Consumption'] and \
        self.rate_updated_once:
            kwh_diff = flds['Message']['Consumption'] - \
                self.prev_flds_incr['Message']['Consumption']
            time_diff = flds['Timestamp'] - self.prev_flds_incr['Timestamp']
            hours_diff = time_diff.total_seconds() / 3600
            meter_rate = kwh_diff / hours_diff

            # Store last reading for later.
            self.save_last_reading_change( flds )

        if self.prev_flds_incr and \
        self.prev_flds_incr['Message']['Consumption'] < flds['Message']['Consumption'] and \
        not self.rate_updated_once:

            self.rate_updated_once = True

            # Store last reading for later.
            self.save_last_reading_change( flds )

        elif not self.prev_flds_incr:
            # Store last reading for later.
            self.save_last_reading_change( flds )

        self.send_mqtt(
            '{}/{}/meter_reading'.format( self.config['mqtt']['topic'], flds['Message']['ID'] ),
            '{}'.format( flds['Message']['Consumption'] ) )
        self.send_mqtt(
            '{}/{}/meter_reading_updated'.format(
                self.config['mqtt']['topic'], flds['Message']['ID'] ),
            datetime.now().isoformat() )

        if 0 < meter_rate:
            self.send_mqtt(
                '{}/{}/meter_rate'.format( self.config['mqtt']['topic'], flds['Message']['ID'] ),
                '{}'.format( meter_rate ) )
            self.send_mqtt(
                '{}/{}/meter_rate_updated'.format(
                    self.config['mqtt']['topic'], flds['Message']['ID'] ),
                datetime.now().isoformat() )

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument( '-c', '--config-file', default='/etc/amrscm2mqtt.ini' )

    parser.add_argument( '-v', '--verbose', action='store_true' )

    args = parser.parse_args()

    logging.basicConfig( level=logging.DEBUG if args.verbose else logging.INFO )
    logger = logging.getLogger( 'main' )

    amrscm2mqtt = AMRSCM2MQTT( args.config_file )
    amrscm2mqtt.client_connect()

    signal.signal( signal.SIGTERM, amrscm2mqtt.shutdown )
    signal.signal( signal.SIGINT, amrscm2mqtt.shutdown )

    while True:
        try:
            line = amrscm2mqtt.get_line()
            amrscm2mqtt.process_reading( line )

        except Exception as ex:
            logger.exception( ex )
            time.sleep( 2 )

if '__main__' == __name__:
    main()
