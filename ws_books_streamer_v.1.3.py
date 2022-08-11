#! /usr/bin/python3
# -*- coding: utf-8 -*-
"""
Websocket Books: Created on Tue Apr  5 11:25:58 2022

@author: D. Gently


Program 1 of 2: - Must be used with ws_books_level2 to create the actual Level2 order books on the local system.
                - Use of a RAM disk is advised to achieve optimal performance


CONTACT FOR ASSISTANCE: anon35828167@protonmail.com


           ------------------------------------------------------------------------------------------------------------
           --                                                                                                        --
           -- >> Copy and use and/or otherwise implement this code or portions thereof in exchange for a donation << --
           --                                                                                                        --
           ------------------------------------------------------------------------------------------------------------


!!! THIS SOFTWARE IS DONATIONWARE -- USE AND BENEFIT FROM THIS CODE ... BE SURE TO MAKE A DONATION and DO NOT REMOVE THIS NOTICE !!!
!!! THIS SOFTWARE IS DONATIONWARE -- USE AND BENEFIT FROM THIS CODE ... BE SURE TO MAKE A DONATION and DO NOT REMOVE THIS NOTICE !!!
!!! THIS SOFTWARE IS DONATIONWARE -- USE AND BENEFIT FROM THIS CODE ... BE SURE TO MAKE A DONATION and DO NOT REMOVE THIS NOTICE !!!

                                 __DONATION ADDRESSES__

               Bitcoin (SEGWIT): 32oZeP5PsJcmULownqGnRxHap5DR3obiFL
                   Bitcoin Cash: qzdun0du6ant53entvqgjukgqdzsulz8tccx26c7uy
                       Ethereum: 0x1f0708e36ea404b8e1cdf17de44675ad8155c956
                          Zcash: zs1vye5p8qyzhszgup48a40xmkehtmwnvrl9smz0vg48xm4x83w7d6wg2zh9nngch7zyhgt5n047ya
                       Litecoin: MHXMQBrUU7Hs1ti4cc1g2xB1rarBdFKiEb
                           Dash: Xiii9PxbzbHaLsHVqvKQMWjRqJzhpBbMAQ
                   Pirate Chain: zs1650z06lmkre5zr2m59rhtffp9e0xmx5ql0n3mupphf9spj8ek4e0zcjk9af9s3z5n64vjqj3hpz
            Bitcoin (NO SEGWIT): 15iAUTFzfnyP579vTg1h7XBAQLEg6qBWs9

!!! THIS SOFTWARE IS DONATIONWARE -- USE AND BENEFIT FROM THIS CODE ... BE SURE TO MAKE A DONATION and DO NOT REMOVE THIS NOTICE !!!
!!! THIS SOFTWARE IS DONATIONWARE -- USE AND BENEFIT FROM THIS CODE ... BE SURE TO MAKE A DONATION and DO NOT REMOVE THIS NOTICE !!!
!!! THIS SOFTWARE IS DONATIONWARE -- USE AND BENEFIT FROM THIS CODE ... BE SURE TO MAKE A DONATION and DO NOT REMOVE THIS NOTICE !!!




Retrieve and maintain the Kucoin full depth order books associated with a set of five PeTRA instruments.  Using a multiplex tunnel approach -- that is, subscribing to all five
data feeds under one connection.  Testing has revealed that a standalone program approach that maintains a set of local books for all PeTRA instances is best given the rate
of messages coming down the multplex tunnel.


GLOBAL VARIABLES DISCLAIMER: Globals are used intentionally to simplify and improve performance.


General steps:

    1. REST API is used to apply for the public websocket token: POST /api/v1/bullet-public
    2. Create a websockets application connection to an endpoint such as "wss://push1-v2.kucoin.com/endpoint?token=xxx&[connectId=xxxxx]"
    3. From the registered on_message handler continuously ping the server every pingInterval seconds
    4. Subscribe to the five Level 2 market data feeds in a single multiplex tunnel
    5. Process incoming messages into a global variable FIFO queue named BooksFeed
    6. Take a snapshot of the entire Level2 dataset for all five instruments at the start into the Books global
    7. Process the BooksFeed FIFO queue against the Books global to bring each set of books up to date
    8. On interval, perhaps every 5 seconds, write one of the instrument's current books to disk with a timestamp extension -- delete oldest of each instrument & keep two copies
    9. On interval, perhaps every 10 minutes, refresh one of the Level2 static books entirely from the Kucoin REST API


"""
# 2022.04.15: version 1.1: Improved error detection and recovery for production use with PeTRA v.2.1
# 2022.08.05: version 1.2: "MULTIPLE BOOK UPDATES FOUND IN A SINGLE WEBSOCKET MESSAGE: UPDATE THE CODE TO HANDLE THIS CONDITION!" being posted -- updated the code to handle.
# 2022.08.08: version 1.3: This new version splits the functionality of providing local Level2 books into this code, the streamer, and the ws_books_level2 program that creates the books.



############################################################################################################################################
##  config section (no separate .config file is used)
############################################################################################################################################


global FQPubK
global FQPrvK
global FQPass
global Market_API_Delay
global API_Retry_Delay
global InstrumentsList
global InstrumentDict
global PersistenceCounter
global BusyFilespec
global StreamFilesListSpec
global MaxStreamFiles

FQPubK = "./API_Public_Key.kucoin"
FQPrvK = "./API_Private_Key.kucoin"
FQPass = "./API_Passphrase.kucoin"
Market_API_Delay = 3.5
API_Retry_Delay = 20
InstrumentsList = [ "BTC-USDT", "ATOM-BTC", "DOT-BTC", "XMR-USDT", "ZEC-USDT" ]
InstrumentDict = { "BTC": 0, "ATO": 1, "DOT": 2, "XMR": 3, "ZEC": 4 }
PersistenceCounter = 4000
WebsocketBooksFolder = "./books/"
StreamBusyFileSpec = "BUSY.STREAM"
StreamFilesListSpec = "FILESLIST.STREAM"
MaxStreamFiles = 50



############################################################################################################################################
##  imports
############################################################################################################################################


import requests
import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta
import pytz
import websocket
import os, glob



############################################################################################################################################
##  ws_books_v.1.0 code
############################################################################################################################################


#-------------------------------------------------------------------------------------------------------------------------------------------
#  secondary websockets functions
#-------------------------------------------------------------------------------------------------------------------------------------------


def SubscribeBooks( instrumentslist ):

    global ConnectId
    global PubWebsocket

    print( "TOP OF SubscribeBooks()" )

    try:
        # top-tier instrument

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"openTunnel", "newTunnelId":"' + instrumentslist[ 0 ] + '_books", "response": "true" }' )
        ConnectId += 1

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"subscribe", "topic":"/market/level2:' + instrumentslist[ 0 ] + '", "tunnelId":"' + instrumentslist[ 0 ] + '_books", "response": "true" }' )
        ConnectId += 1

        # triad a instrument 1

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"openTunnel", "newTunnelId":"' + instrumentslist[ 1 ] + '_books", "response": "true" }' )
        ConnectId += 1

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"subscribe", "topic":"/market/level2:' + instrumentslist[ 1 ] + '", "tunnelId":"' + instrumentslist[ 1 ] + '_books", "response": "true" }' )
        ConnectId += 1

        # triad a instrument 2

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"openTunnel", "newTunnelId":"' + instrumentslist[ 2 ] + '_books", "response": "true" }' )
        ConnectId += 1

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"subscribe", "topic":"/market/level2:' + instrumentslist[ 2 ] + '", "tunnelId":"' + instrumentslist[ 2 ] + '_books", "response": "true" }' )
        ConnectId += 1

        # triad b instrument 1

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"openTunnel", "newTunnelId":"' + instrumentslist[ 3 ] + '_books", "response": "true" }' )
        ConnectId += 1

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"subscribe", "topic":"/market/level2:' + instrumentslist[ 3 ] + '", "tunnelId":"' + instrumentslist[ 3 ] + '_books", "response": "true" }' )
        ConnectId += 1

        # triad b instrument 2

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"openTunnel", "newTunnelId":"' + instrumentslist[ 4 ] + '_books", "response": "true" }' )
        ConnectId += 1

        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"subscribe", "topic":"/market/level2:' + instrumentslist[ 4 ] + '", "tunnelId":"' + instrumentslist[ 4 ] + '_books", "response": "true" }' )
        ConnectId += 1

        print( "RETURNING TRUE FROM SubscribeBooks()" )
        return( True )

    except:

        print( "RETURNING FALSE FROM SubscribeBooks()" )
        return( False )



def GetPublicWebsocketToken():

    thisAPIRetryDelay = API_Retry_Delay

    api_key = open( FQPubK ).read().strip()
    encoded_api_secret = bytes( ( open( FQPrvK ).read().strip() )[ 2:-1], 'utf-8' )
    decoded_api_secret = base64.b64decode( encoded_api_secret )
    api_secret = str(decoded_api_secret , 'utf-8')
    encoded_api_passphrase = bytes( ( open( FQPass ).read().strip() )[ 2:-1], 'utf-8' )
    decoded_api_passphrase = base64.b64decode( encoded_api_passphrase )
    api_passphrase = str( decoded_api_passphrase, 'utf-8' )

    http_method = "POST"
    request_path = "/api/v1/bullet-public"
    url = "https://api.kucoin.com" + request_path
    passphrase = base64.b64encode( hmac.new( api_secret.encode( 'utf-8' ), api_passphrase.encode( 'utf-8' ), hashlib.sha256 ).digest() )

    parameters = { }
    parameters_json = json.dumps( parameters )

    while True:

        nonce = str( int( round( time.time() * 1000 ) ) )
        message = nonce + http_method + request_path + parameters_json
        signature = base64.b64encode( hmac.new( api_secret.encode( 'utf-8' ), message.encode( 'utf-8' ), hashlib.sha256 ).digest() )
        headers = {
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": nonce,
            "KC-API-KEY": api_key,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }

        try:
            response = json.loads( ( requests.request( http_method, url, headers = headers, data = parameters_json ) ).content )
            responseStr = str( response )
            time.sleep( Market_API_Delay )

        except Exception as error:

            print( "API call exception in GetPublicWebsocketToken() (%s)" % error )
            time.sleep( thisAPIRetryDelay )
            thisAPIRetryDelay *= 1.1

        if "'code': '200000'" in responseStr:
            return( response )
        else:
            print( "API call failed in GetPublicWebsocketToken(): " + responseStr )
            time.sleep( thisAPIRetryDelay )
            thisAPIRetryDelay *= 1.1



def PingWebsocket():

    global ConnectId
    global PubWebsocket
    global DisconnectedFlg

    try:
        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        print( "Pinging websocket with ConnectId # " + connectIdStr )
        PubWebsocket.send( '{ "id":"' + connectIdStr + '", "type":"ping" }' )
        ConnectId += 1
    except:
        DisconnectedFlg = True



#-------------------------------------------------------------------------------------------------------------------------------------------
#  websockets connect & event handlers
#-------------------------------------------------------------------------------------------------------------------------------------------


def WebsocketOnError( wsapp, error ):

    global BooksFeed
    global LastPing
    global NextPing
    global DisconnectedFlg
    global PingMessages
    global StreamFiles

    print( "WebsocketOnError ERROR: ", error )
    BooksFeed = []
    LastPing = datetime.now( pytz.utc )
    NextPing = LastPing + timedelta( 0, 10 )
    DisconnectedFlg = True
    PingMessages = 0
    StreamFiles = []
    wsapp.close()



def WebsocketOnMessage( wsapp, msg ):

    global InstrumentDict
    global PersistenceCounter
    global StreamBusyFileSpec
    global StreamFilesListSpec
    global BooksFeed
    global LastPing
    global NextPing
    global TotalMessages
    global PingMessages
    global StreamFiles

    # STREAMER VERSION 1.3 -- start with some msg preprocessing to lighten the load on ws_books_level2_v.1.3.py

    try:
        instrNdx = InstrumentDict[ msg[ 30:33 ] ]

        ndx0 = msg.find( "sequenceStart" ) + 15
        ndx1 = msg[ ndx0: ].find( "," )
        ndx2 = msg.find( "sequenceEnd" ) + 13
        ndx3 = msg[ ndx2: ].find( "," )
        if ndx3 == -1:
            ndx3 = msg[ ndx2: ].find( "}" )

        seqStart = msg[ ndx0 : ndx0 + ndx1 ]
        seqEnd = msg[ ndx2 : ndx2 + ndx3 ]
        seqStartInt = int( seqStart )
        seqEndInt = int( seqEnd )

        ndx4 = msg.find( '"asks":[]' )
        if ndx4 == -1:
            ndx4 = msg.find( '"asks":[[' )
            ndx5 = msg[ ndx4: ].find( "]]" )
            asksStr = msg[ ndx4: ndx4 + ndx5 + 2 ]
        else:
            asksStr = '"asks":[]'

        ndx6 = msg.find( '"bids":[]' )
        if ndx6 == -1:
            ndx6 = msg.find( '"bids":[[' )
            ndx7 = msg[ ndx6: ].find( "]]" )
            bidsStr = msg[ ndx6: ndx6 + ndx7 + 2 ]
        else:
            bidsStr = '"bids":[]'

        for seqInt in range( seqStartInt, seqEndInt + 1 ):
            seq = str( seqInt )
            if seq in asksStr:
                ndx8 = asksStr.find( seq )
                ndx9 = asksStr.rfind( "[", 0, ndx8 )
                updateRec = json.loads( asksStr[ ndx9: ndx9 + asksStr[ ndx9: ].find( "]" ) + 1 ] )
                BooksFeed = BooksFeed + [ [ instrNdx, 2, updateRec, float( updateRec[ 0 ] ) ] ]          # second position: bid == 1; ask == 2
            else:
                ndx8 = bidsStr.find( seq )
                ndx9 = bidsStr.rfind( "[", 0, ndx8 )
                updateRec = json.loads( bidsStr[ ndx9: ndx9 + bidsStr[ ndx9: ].find( "]" ) + 1 ] )
                BooksFeed = BooksFeed + [ [ instrNdx, 1, updateRec, float( updateRec[ 0 ] ) ] ]

        TotalMessages += 1
        PingMessages += 1

    except:
        return

    now = datetime.now( pytz.utc )

    if NextPing < now:
        print( "    PING DELTA: " +  str( now - LastPing ) + "  PING MESSAGES: " + str( PingMessages ) )
        LastPing = now
        NextPing = now + timedelta( 0, PingInterval )
        PingMessages = 0
        PingWebsocket()

    if ( TotalMessages % PersistenceCounter ) <= ( seqEndInt - seqStartInt ):                           # (possibly a few more than PersistenceCounter preprocessed messages per file)

        ts = str( datetime.now( pytz.utc ) ).replace( "+00:00", '' ).replace( ' ', '-' ).replace( ':', '-' ).replace( '.', '-' )
        streamFileName = "books/stream-" + ts + ".STREAM"
        print( "TotalMessages: " + str( TotalMessages ) + "   ---   Persisting stream to: " + streamFileName )

        StreamFiles.insert( 0, streamFileName )
        with open( WebsocketBooksFolder + StreamBusyFileSpec, 'w' ) as flagFile:
            json.dump( "WAIT!", flagFile )
        with open( streamFileName, 'w' ) as streamFile:
            json.dump( BooksFeed, streamFile, indent = 4 )
        if len( StreamFiles ) > MaxStreamFiles:
            os.remove( StreamFiles[ MaxStreamFiles ] )
            del StreamFiles[ MaxStreamFiles ]
        with open( WebsocketBooksFolder + StreamFilesListSpec, 'w' ) as streamFilesListFile:
            json.dump( StreamFiles, streamFilesListFile, indent = 4 )

        os.remove( WebsocketBooksFolder + StreamBusyFileSpec )

        BooksFeed = []

    return



def WebsocketOnOpen( wsapp ):

    global InstrumentsList
    global ConnectId

    print( "ENTERING WebsocketOnOpen()" )
    print( wsapp )
    time.sleep( 5 )

    connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
    print( "Pinging websocket..." )
    wsapp.send( '{ "id":"' + connectIdStr + '", "type":"ping" }' )
    time.sleep( 5 )
    ConnectId += 1

    SubscribeBooks( InstrumentsList )



def PubWebsocketConnect():

    global Token
    global Endpoint
    global PingInterval
    global ConnectId
    global PubWebsocket
    global BooksFeed
    global LastPing
    global NextPing
    global TotalMessages
    global DisconnectedFlg
    global PingMessages
    global StreamFiles

    while True:
        try:

            for file in glob.glob( "books/*.STREAM" ):
                os.remove( file )

            BooksFeed = []
            LastPing = datetime.now( pytz.utc )
            NextPing = LastPing + timedelta( 0, 10 )
            DisconnectedFlg = False
            PingMessages = 0
            StreamFiles = []

            pubWebsocketTkn = GetPublicWebsocketToken()

            Token = pubWebsocketTkn[ "data" ][ "token" ]
            Endpoint = pubWebsocketTkn[ "data" ][ "instanceServers" ][ 0 ][ "endpoint" ]
            PingInterval = float( pubWebsocketTkn[ "data" ][ "instanceServers" ][ 0 ][ "pingInterval" ] ) / 1000

            connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]

            websocket.setdefaulttimeout( 5 )
            PubWebsocket = websocket.WebSocketApp( Endpoint + "?token=" + Token + "&connectId=" + connectIdStr,
                                                  on_open = WebsocketOnOpen,
                                                  on_message = WebsocketOnMessage,
                                                  on_error = WebsocketOnError )
            ConnectId += 1
            PubWebsocket.run_forever()
            time.sleep( 5 )
            return
        except:
            time.sleep( 5 )
            continue



############################################################################################################################################
##  MAINLINE
############################################################################################################################################

def main():

    global ConnectId
    global TotalMessages

    ConnectId = 0
    TotalMessages = 0

    PubWebsocketConnect()

    seconds = 0
    while True:
        if DisconnectedFlg:
            PubWebsocketConnect()                                   # (this call triggered here turns-off the DisconnectedFlg)
        time.sleep( 10 )
        seconds += 10
        print( "Main loop wait seconds: " + str( seconds ) )



global Token
global Endpoint
global PingInterval
global ConnectId
global PubWebsocket
global BooksFeed
global LastPing
global NextPing
global TotalMessages
global DisconnectedFlg
global PingMessages
global StreamFiles

main()
