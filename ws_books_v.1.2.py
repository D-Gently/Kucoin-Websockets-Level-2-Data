#! /usr/bin/python3
# -*- coding: utf-8 -*-
"""
Websocket Books: Created on Tue Apr  5 11:25:58 2022

@author: D. Gently



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



############################################################################################################################################
##  config section (no separate .config file is used)
############################################################################################################################################


global VERBOSE_ON
global SILENT_ON
global SPECIAL_DEBUG_ON
global FQPubK
global FQPrvK
global FQPass
global Market_API_Delay
global API_Retry_Delay
global InstrumentsList
global InitPreloadBuffer
global PreloadBuffer
global MessagesPerVerify
global PersistenceCounter
global BusyFilespec

VERBOSE_ON = False
SILENT_ON = False
SPECIAL_DEBUG_ON = False
FQPubK = "./API_Public_Key.kucoin"
FQPrvK = "./API_Private_Key.kucoin"
FQPass = "./API_Passphrase.kucoin"
Market_API_Delay = 3.5
API_Retry_Delay = 20
InstrumentsList = [ "BTC-USDT", "ATOM-BTC", "DOT-BTC", "XMR-USDT", "ZEC-USDT" ]
InstrumentDict = { "BTC": 0, "ATO": 1, "DOT": 2, "XMR": 3, "ZEC": 4 }
InitPreloadBuffer = 50000
PreloadBuffer = 8000
MessagesPerVerify = 92000
PersistenceCounter = 4000
BusyFilespec = "books/BUSY.FLG"



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
#  general functions
#-------------------------------------------------------------------------------------------------------------------------------------------


def PersistBooks( ndx, name, sequence, bids, asks ):

    global BooksFileNames

    booksDict = { name: [ { 'sequence': sequence },
                          { 'bids': bids },
                          { 'asks': asks } ] }

    if ndx < 5:
        ext = ".BOOKS"
    else:
        ext = ".VERIFY"
    ts = str( datetime.now( pytz.utc ) ).replace( "+00:00", '' ).replace( ' ', '-' ).replace( ':', '-' ).replace( '.', '-' )
    booksFileName = "books/" + name + '-' + ts + ext
    if len( BooksFileNames[ ndx ] ) == 2:
        os.remove( BooksFileNames[ ndx ][ 1 ] )
        del BooksFileNames[ ndx ][ 1 ]
    BooksFileNames[ ndx ].insert( 0, booksFileName )

    with open( booksFileName, 'w' ) as booksFile:
        json.dump( booksDict, booksFile, indent = 4 )



def SyncToFeed():       # process a websocket message from the FIFO queue per on_message call (return 0 == normal; -1 == verification failure; -2 == missing seq. number)

    # Sample BooksFeed books update line:
    #
    # IPdb [129]: BooksFeed[ 12 ][ 0 ]
    # Out  [129]: '{"type":"message","tunnelId":"XMR-USDT_books","topic":"/market/level2:XMR-USDT","subject":"trade.l2update","data":{"sequenceStart":1617361515152,"symbol":"XMR-USDT","changes":{"asks":[],"bids":
    # [["0","0","1617361515152"]]},"sequenceEnd":1617361515152}}'

    # Ver. 1.2: Here's a sample BooksFeed FIFO queue message with more than one update:
    #
    # BooksFeed[ 0 ][ 0 ]: {"type":"message","tunnelId":"DOT-BTC_books","topic":"/market/level2:DOT-BTC","subject":"trade.l2update","data":{"changes":{"asks":[["0.00037526","50","11501462"],
    #                      ["0.00037563","150","11501461"]],"bids":[["0.00037339","150","11501460"]]},"sequenceEnd":11501462,"sequenceStart":11501460,"symbol":"DOT-BTC","time":1659738043576}}

    global SILENT_ON
    global SPECIAL_DEBUG_ON
    global BooksFeed
    global Books
    global VerifyBooks
    #global MessageDump

    # THIS IS THE FIRST WEBSOCKET MESSAGE THAT WE TRY TO PROCESS:
    #
    try:
        msg = BooksFeed[ 0 ][ 0 ]
        instrNdx = InstrumentDict[ msg[ 30:33 ] ]

        # NOTE: This check for the sequenceStart being the same as the sequenceEnd is precautionary: if this sys.exit( 0 ) ever fires then the code needs to be updated to handle potentially multiple
        #       books updates per single message.   UPDATE: Ver. 1.2 code processes the multiple change records per message if necessary
        #
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

        #if SPECIAL_DEBUG_ON:
        #    print( "SyncToFeed(): checkpoint #1A" )
        #    print( "msg: " + msg )
        #    print( "msg[ 30:33 ]: " + msg[ 30:33 ] )
        #    print( "instrNdx: " + str( instrNdx ) + "\n" )

        changes = []
        for seqInt in range( seqStartInt, seqEndInt + 1 ):
            seq = str( seqInt )
            if seq in asksStr:
                ndx8 = asksStr.find( seq )
                ndx9 = asksStr.rfind( "[", 0, ndx8 )
                updateRec = json.loads( asksStr[ ndx9: ndx9 + asksStr[ ndx9: ].find( "]" ) + 1 ] )
                changes = changes + [ [ 2, updateRec, float( updateRec[ 0 ] ) ] ]
            else:
                ndx8 = bidsStr.find( seq )
                ndx9 = bidsStr.rfind( "[", 0, ndx8 )
                updateRec = json.loads( bidsStr[ ndx9: ndx9 + bidsStr[ ndx9: ].find( "]" ) + 1 ] )
                changes = changes + [ [ 1, updateRec, float( updateRec[ 0 ] ) ] ]

        if SPECIAL_DEBUG_ON:
            #print( "SyncToFeed(): checkpoint #2A" )
            if len( changes ) > 1:
                print( "SyncToFeed(): checkpoint #2A:   Instrument: " + InstrumentsList[ instrNdx ] + ":   changes: " + str( changes ) + "\n" )

        #if SPECIAL_DEBUG_ON and instrNdx == 2:
        #    MessageDump = MessageDump + [ [ msg ] ]
        #    MessageDump = MessageDump + [ [ changes ] ]

        for changeRec in changes:

            #if SPECIAL_DEBUG_ON and instrNdx == 2:
            #    MessageDump = MessageDump + [ [ changeRec ] ]

            if changeRec[ 1 ][ 2 ] <= Books[ instrNdx ][ 0 ]:
                continue

            updateBook = changeRec[ 0 ]                                                 # bid == 1; ask == 2
            updateRec = changeRec[ 1 ]
            thisRecPrice = changeRec[ 2 ]

            # here's the update logic as per the Kucoin api docs at https://docs.kucoin.com/?lang=en_US#market-snapshot
            #
            if ( updateRec[ 0 ] != '0' ) and ( updateRec[ 1 ] == '0' ):                 # when there's a price but the size is 0, remove the corresponding price record
                i = 0
                while Books[ instrNdx ][ updateBook ][ i ][ 0 ] != updateRec[ 0 ]:
                    i += 1
                try:
                    del Books[ instrNdx ][ updateBook ][ i ]
                except:
                    print( "==>>  DELETE FAILED!  <<==")
            elif ( updateRec[ 0 ] != '0' ) and ( updateRec[ 1 ] != '0' ):               # when there's a price and a non-zero size, update or add as necessary the price record
                i = 0
                thisBooksPrice = float( Books[ instrNdx ][ updateBook ][ i ][ 0 ] )
                if updateBook == 1:                                                     # updating new size onto existing price or new record into bid book
                    while thisRecPrice < thisBooksPrice:
                        i += 1
                        thisBooksPrice = float( Books[ instrNdx ][ updateBook ][ i ][ 0 ] )
                else:                                                                   # updating new size onto existing price or new record into ask book
                    while thisRecPrice > thisBooksPrice:
                        i += 1
                        thisBooksPrice = float( Books[ instrNdx ][ updateBook ][ i ][ 0 ] )
                if thisRecPrice == thisBooksPrice:
                    Books[ instrNdx ][ updateBook ][ i ][ 1 ] = updateRec[ 1 ]
                else:
                    Books[ instrNdx ][ updateBook ].insert( i, [ updateRec[ 0 ], updateRec[ 1 ] ] )

                    # Ver.1.2 NOTE: Here's additional logic required but not documented by Kucoin: When a *new* order is created it *may* automatically modify the
                    #               alternate order book *if* there is a/are standing order/s at more favourable pricing in the alternate book...
                    #
                    # (Working on the assumption that the new record is the "settled result" of the actual limit order(s) that was/were placed -- that is to say, that the
                    #  limit order(s) has/have already removed all standing orders at the same or more favourable pricing in the opposite side book.)
                    #
                    if i == 0:                                                          # situation only arises when new order is at the top of the book
                        if updateBook == 1:                                             # a new bid was just added
                            altBook = 2
                            while float( Books[ instrNdx ][ altBook ][ 0 ][ 0 ] ) <= thisRecPrice:
                                del Books[ instrNdx ][ altBook ][ 0 ]
                        else:
                            altBook = 1
                            while float( Books[ instrNdx ][ altBook ][ 0 ][ 0 ] ) >= thisRecPrice:
                                del Books[ instrNdx ][ altBook ][ 0 ]

            #if SPECIAL_DEBUG_ON:
            #    print( "SyncToFeed(): checkpoint #3A" )

            Books[ instrNdx ][ 0 ] = updateRec[ 2 ]                                     # in all cases, including neither price nor size, make sure to update the sequence number

            #if SPECIAL_DEBUG_ON:
            #    print( "SyncToFeed(): checkpoint #4A" )

            if VerifyBooks[ 0 ]:                                                        # index 0: is set True when there's a REST API full depth set of books available for verification
                if InstrumentDict[ VerifyBooks[ 1 ][ 0 : 3 ] ] == instrNdx:             # index 1: instrument name for the books that need to be verified
                    #if SPECIAL_DEBUG_ON:
                    #    print( "msg: " + msg )
                    #    print( "changeRec: " + str( changeRec ) )
                    #    print( "VerifyBooks sequencenumber: " + VerifyBooks[ 2 ] )
                    #    print( "      Books sequencenumber: " + Books[ instrNdx ][ 0 ] )
                    #elif ( int( Books[ instrNdx ][ 0 ] ) % 1000 ) == 0:
                    #    print( "VerifyBooks sequencenumber: " + VerifyBooks[ 2 ] )
                    #    print( "      Books sequencenumber: " + Books[ instrNdx ][ 0 ] )
                    if VerifyBooks[ 2 ] == Books[ instrNdx ][ 0 ]:                      # index 2: sequence number of the verify books set
                        if SPECIAL_DEBUG_ON:
                            print( "VerifyBooks REQUESTED and instNdx MATCHED and sequencenumber MATCHED" )
                            print( "VerifyBooks sequencenumber: " + VerifyBooks[ 2 ] )
                            print( "      Books sequencenumber: " + Books[ instrNdx ][ 0 ] )
                            #SILENT_ON = True
                            #SPECIAL_DEBUG_ON = False
                        if VerifyBooks[ 3 ] != Books[ instrNdx ][ 1 ]:
                            if SPECIAL_DEBUG_ON:
                                print( "VerifyBooks A -- BID BOOKS DON'T MATCH!" )
                                print( "msg: " + msg )
                                #with open( "books/MessageDump.txt", 'w' ) as messageDumpFile:
                                #    json.dump( MessageDump, messageDumpFile, indent = 4 )
                            PersistBooks( instrNdx, VerifyBooks[ 1 ], Books[ instrNdx ][ 0 ], Books[ instrNdx ][ 1 ], Books[ instrNdx ][ 2 ] )
                            PersistBooks( instrNdx + 5, VerifyBooks[ 1 ], VerifyBooks[ 2 ], VerifyBooks[ 3 ], VerifyBooks[ 4 ] )
                            return( -1 )
                        if VerifyBooks[ 4 ] != Books[ instrNdx ][ 2 ]:
                            if SPECIAL_DEBUG_ON:
                                print( "VerifyBooks A -- ASK BOOKS DON'T MATCH!" )
                                print( "msg: " + msg )
                                #with open( "books/MessageDump.txt", 'w' ) as messageDumpFile:
                                #    json.dump( MessageDump, messageDumpFile, indent = 4 )
                            PersistBooks( instrNdx, VerifyBooks[ 1 ], Books[ instrNdx ][ 0 ], Books[ instrNdx ][ 1 ], Books[ instrNdx ][ 2 ] )
                            PersistBooks( instrNdx + 5, VerifyBooks[ 1 ], VerifyBooks[ 2 ], VerifyBooks[ 3 ], VerifyBooks[ 4 ] )
                            return( -1 )
                        if VERBOSE_ON or not SILENT_ON:
                            print( "==>> 100% MATCH CONFIRMED BETWEEN LOCAL WEBSOCKET BOOKS AND STATIC SERVER REST BOOKS FOR INSTRUMENT: " +  VerifyBooks[ 1 ] )
                        VerifyBooks[ 0 ] = False
                    elif VerifyBooks[ 2 ] < Books[ instrNdx ][ 0 ]:
                        if SPECIAL_DEBUG_ON:
                            print( " ### MISSING SEQ. NUMBER BETWEEN LOCAL WEBSOCKET BOOKS AND STATIC SERVER REST BOOKS FOR INSTRUMENT: " +  VerifyBooks[ 1 ] + " ###" )
                        VerifyBooks[ 0 ] = False
                        return( -2 )

                #if SPECIAL_DEBUG_ON:
                #    print( "SyncToFeed(): checkpoint #5A" )

            #if SPECIAL_DEBUG_ON:
            #    print( "SyncToFeed(): checkpoint #6A" )

        #if SPECIAL_DEBUG_ON:
        #    print( "SyncToFeed(): checkpoint #7A" )

        del BooksFeed[ 0 ]

        #if SPECIAL_DEBUG_ON:
        #    print( "SyncToFeed(): checkpoint #8A" )

        return( 0 )

    except:
        #if VerifyBooks[ 0 ]:
            #if SPECIAL_DEBUG_ON:
            #    print( "VerifyBooks REQUESTED BUT EXCEPTION THROWN" )
            #    SILENT_ON = True
            #    SPECIAL_DEBUG_ON = False
        if VERBOSE_ON:
            print( "Processing EXCEPTION 1 of 2 in SyncToFeed()" )
        try:
            del BooksFeed[ 0 ]
        except:
            return( 0 )



#-------------------------------------------------------------------------------------------------------------------------------------------
#  rest api related functions
#-------------------------------------------------------------------------------------------------------------------------------------------


def GetFullOrderBook( instrument ):

    api_key = open( FQPubK ).read().strip()
    encoded_api_secret = bytes( ( open( FQPrvK ).read().strip() )[ 2:-1], 'utf-8' )
    decoded_api_secret = base64.b64decode( encoded_api_secret )
    api_secret = str(decoded_api_secret , 'utf-8')
    encoded_api_passphrase = bytes( ( open( FQPass ).read().strip() )[ 2:-1], 'utf-8' )
    decoded_api_passphrase = base64.b64decode( encoded_api_passphrase )
    api_passphrase = str( decoded_api_passphrase, 'utf-8' )

    http_method = "GET"
    request_path = "/api/v3/market/orderbook/level2?symbol=" + instrument
    url = "https://api.kucoin.com" + request_path
    passphrase = base64.b64encode( hmac.new( api_secret.encode( 'utf-8' ), api_passphrase.encode( 'utf-8' ), hashlib.sha256 ).digest() )

    thisAPIRetryDelay = API_Retry_Delay

    while ( True ):
        nonce = str( int( round( time.time() * 1000 ) ) )
        message = nonce + http_method + request_path
        signature = base64.b64encode( hmac.new( api_secret.encode( 'utf-8' ), message.encode( 'utf-8' ), hashlib.sha256 ).digest() )
        headers = {
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": nonce,
            "KC-API-KEY": api_key,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2"
        }

        try:
            response = json.loads( ( requests.request( http_method, url, headers = headers ) ).content )
            responseStr = str( response )
            time.sleep( Market_API_Delay )
            #time.sleep( 10 )

        except Exception as error:
            print( "API call exception in GetFullOrderBook_kcn( instrument ) (%s)" % error )
            time.sleep( thisAPIRetryDelay )
            thisAPIRetryDelay *= 1.1
            continue

        if "'code': '200000'" in responseStr:
            return( response )

        else:
            print( "API call failed in GetFullOrderBook_kcn(): " + responseStr )
            time.sleep( thisAPIRetryDelay )
            thisAPIRetryDelay *= 1.1
            continue



def LoadLevel2( instrumentslist, load = True ):

    # Ver.1.2: return False when single-instrument verify REST data is older than current feed data

    global SILENT_ON
    global SPECIAL_DEBUG_ON
    global Books
    global VerifyBooks
    global LastPing

    if VERBOSE_ON or not SILENT_ON:
        print( "TOP OF LoadLevel2()" )

    if load:
        Books = []

    count = 0
    for instrument in instrumentslist:
        if VERBOSE_ON or not SILENT_ON:
            print( "...getting next instrument order book..." )
        response = GetFullOrderBook( instrument )
        if load:
            Books = Books + [ [ response[ "data" ][ "sequence" ], response[ "data" ][ "bids" ], response[ "data" ][ "asks" ] ] ]
        else:
            if response[ "data" ][ "sequence" ] > Books[ InstrumentDict[ instrument[ 0:3 ] ] ][ 0 ]:
                VerifyBooks = [ True, instrument, response[ "data" ][ "sequence" ], response[ "data" ][ "bids" ], response[ "data" ][ "asks" ] ]
                if SPECIAL_DEBUG_ON:
                    print( "VerifyBooks[ 0 ] set True in LoadLevel2()" )
                    print( "VerifyBooks[ 1 ] (instrument): " + instrument )
                    print( "VerifyBooks[ 2 ] (sequence): " + response[ "data" ][ "sequence" ] )
                    print( "VerifyBooks[ 3 ][ 0 ] (top bid): " + json.dumps( response[ "data" ][ "bids" ][ 0 ] ) )
                    print( "VerifyBooks[ 4 ][ 0 ] (top ask): " + json.dumps( response[ "data" ][ "asks" ][ 0 ] ) )
            else:
                return( False )
        count += 1
        if ( count % 2 ) == 0:
        #if ( count % 1 ) == 0:
            PingWebsocket()
            LastPing = datetime.now( pytz.utc )

    return( True )



#-------------------------------------------------------------------------------------------------------------------------------------------
#  secondary websockets functions
#-------------------------------------------------------------------------------------------------------------------------------------------


def SubscribeBooks( instrumentslist ):

    global ConnectId
    global PubWebsocket

    if VERBOSE_ON or not SILENT_ON:
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

        if VERBOSE_ON or not SILENT_ON:
            print( "RETURNING TRUE FROM SubscribeBooks()" )
        return( True )

    except:

        if VERBOSE_ON or not SILENT_ON:
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

            if VERBOSE_ON:
                print( "API call exception in GetPublicWebsocketToken() (%s)" % error )
            time.sleep( thisAPIRetryDelay )
            thisAPIRetryDelay *= 1.1

        if "'code': '200000'" in responseStr:
            return( response )
        else:
            if VERBOSE_ON:
                print( "API call failed in GetPublicWebsocketToken(): " + responseStr )
            time.sleep( thisAPIRetryDelay )
            thisAPIRetryDelay *= 1.1



def PingWebsocket():

    global ConnectId
    global PubWebsocket
    global DisconnectedFlg

    try:
        connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
        if VERBOSE_ON or not SILENT_ON:
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
    global Books
    global VerifyBooks
    global LastPing
    global DisconnectedFlg
    global ProcessingOnMessage
    global StopPinging
    global InstrumentReloadPending
    global BooksFileNames
    global PersistenceCounterDelay
    #global MessageDump

    print( "WebsocketOnError ERROR: ", error )
    BooksFeed = []
    Books = []
    VerifyBooks = [ False, '', '', [], [] ]
    LastPing = datetime.now( pytz.utc )
    DisconnectedFlg = True
    ProcessingOnMessage = False
    StopPinging = False
    InstrumentReloadPending = -2
    BooksFileNames = [ [], [], [], [], [], [], [], [], [], [] ]
    PersistenceCounterDelay = TotalMessages + ( InitPreloadBuffer * 2 )
    #MessageDump = []
    wsapp.close()



def WebsocketOnMessage( wsapp, msg ):

    global BooksFeed
    global LastPing
    global TotalMessages
    global SubscriptionCounter
    global ProcessingOnMessage
    global StopPinging
    global InstrumentReloadPending

    if VERBOSE_ON:
        print( msg )

    BooksFeed = BooksFeed + [ [ msg ] ]
    TotalMessages += 1

    if ProcessingOnMessage:
        if ( VERBOSE_ON or not SILENT_ON ) and ( ( TotalMessages % 5000 ) == 0 ):
            print( "TOTAL MESSAGES: " + str( TotalMessages ) )
            print( "PRE SyncToFeed() BooksFeed Length: " + str( len( BooksFeed ) ) )
        return

    if StopPinging:
        return                                                      # this is how a SyncToFeed verification failure causes a reset

    ProcessingOnMessage = True

    lastPingPlusDelta = LastPing + timedelta( 0, PingInterval )
    now = datetime.now( pytz.utc )

    if lastPingPlusDelta < now:
        PingWebsocket()
        LastPing = now
        ProcessingOnMessage = False
        return

    SubscriptionCounter += 1

    if ( InstrumentReloadPending == -2 ) and ( SubscriptionCounter == 0 ):

        if VERBOSE_ON or not SILENT_ON:
            print( "Init-loading Level2 books for ALL instruments: " + InstrumentsList[ 0 ] + ' ' + InstrumentsList[ 1 ] + ' ' + InstrumentsList[ 2 ] + ' ' \
                   + InstrumentsList[ 3 ] + ' ' + InstrumentsList[ 4 ] )
        LoadLevel2( InstrumentsList, True )
        InstrumentReloadPending = -1

    elif SubscriptionCounter > 0:

        if ( SubscriptionCounter % MessagesPerVerify ) == 0:

            # Ver. 1.2: if already running a VerifyBooks operation abort this request and delay for PreloadBuffer messages
            #
            if VerifyBooks[ 0 ]:
                print( "!!! PERFORMANCE!  FALLING BEHIND FEED: VerifyBooks operation DEFERRED as already running !!!" )
            else:
                InstrumentReloadPending += 1
                if InstrumentReloadPending == 5:
                    InstrumentReloadPending = 0
                if VERBOSE_ON or not SILENT_ON:
                    print( "Verify-loading Level2 books for instrument: " + InstrumentsList[ InstrumentReloadPending ] )
                SubscriptionCounter = -PreloadBuffer
        else:
            syncToFeedResult = SyncToFeed()                     # process a message out of the FIFO queue
            if syncToFeedResult == -1:
                if ( VERBOSE_ON or not SILENT_ON ):
                    print( " ### SyncToFeed() VERIFICATION FAILURE! ###" )
                ProcessingOnMessage = False
                StopPinging = True
                return
            elif syncToFeedResult == -2:
                if ( VERBOSE_ON or not SILENT_ON ):
                    print( " ### SyncToFeed() MISSING SEQUENCE NUMBER! ###" )
                ProcessingOnMessage = False
                StopPinging = True
                return
            if len( BooksFeed ) > 0:                            # while the BooksFeed FIFO queue has messages process two queued messages per OnMessage call
                syncToFeedResult = SyncToFeed()
                if syncToFeedResult == -1:
                    if ( VERBOSE_ON or not SILENT_ON ):
                        print( " ### SyncToFeed() VERIFICATION FAILURE! ###" )
                    ProcessingOnMessage = False
                    StopPinging = True
                    return
                elif syncToFeedResult == -2:
                    if ( VERBOSE_ON or not SILENT_ON ):
                        print( " ### SyncToFeed() MISSING SEQUENCE NUMBER! ###" )
                    ProcessingOnMessage = False
                    StopPinging = True
                    return
            if ( VERBOSE_ON or not SILENT_ON ):
                booksFeedLen = len( BooksFeed )
                if ( booksFeedLen > 0 ) and ( ( booksFeedLen % 1000 ) == 0 ):
                    print( "POST SyncToFeed() BooksFeed Length: " + str( booksFeedLen ) )

    elif SubscriptionCounter == 0:

        if not LoadLevel2( [ InstrumentsList[ InstrumentReloadPending ] ], False ):     # Ver.1.2: Keep loading the FIFO queue when verifying until at least one message has come in
            SubscriptionCounter = -PreloadBuffer                                        # (the VerifyBooks global is set by calling LoadLevel2 with False in the second parameter)

    if ( VERBOSE_ON or not SILENT_ON ) and ( ( TotalMessages % 1000 ) == 0 ):
        print( "TOTAL MESSAGES: " + str( TotalMessages ) )
        print( "BooksFeed Length: " + str( len( BooksFeed ) ) )

    if ( TotalMessages % PersistenceCounter ) == 0 and ( TotalMessages > PersistenceCounterDelay ):
        with open( BusyFilespec, 'w' ) as flagFile:
            json.dump( "WAIT!", flagFile )
        for i in range( 0, 5 ):
            PersistBooks( i, InstrumentsList[ i ], Books[ i ][ 0 ], Books[ i ][ 1 ], Books[ i ][ 2 ] )
        os.remove( BusyFilespec )

    ProcessingOnMessage = False



def WebsocketOnOpen( wsapp ):

    global InstrumentsList
    global ConnectId
    global SubscriptionCounter
    global StopPinging

    if VERBOSE_ON or not SILENT_ON:
        print( "ENTERING WebsocketOnOpen()" )
        print( wsapp )
    time.sleep( 5 )

    connectIdStr = ( "000000000" + str( ConnectId ) )[ -10 : ]
    if VERBOSE_ON or not SILENT_ON:
        print( "Pinging websocket..." )
    wsapp.send( '{ "id":"' + connectIdStr + '", "type":"ping" }' )
    time.sleep( 5 )
    ConnectId += 1

    SubscribeBooks( InstrumentsList )
    SubscriptionCounter = -InitPreloadBuffer
    StopPinging = False



def PubWebsocketConnect():

    global Token
    global Endpoint
    global PingInterval
    global ConnectId
    global PubWebsocket
    global BooksFeed
    global Books
    global VerifyBooks
    global LastPing
    global TotalMessages
    global DisconnectedFlg
    global ProcessingOnMessage
    global StopPinging
    global InstrumentReloadPending
    global BooksFileNames
    global PersistenceCounterDelay
    #global MessageDump

    for file in glob.glob( "books/*" ):
        os.remove( file )

    while True:
        try:

            BooksFeed = []
            Books = []
            VerifyBooks = [ False, '', '', [], [] ]
            LastPing = datetime.now( pytz.utc )
            DisconnectedFlg = False
            ProcessingOnMessage = False
            StopPinging = False
            InstrumentReloadPending = -2
            BooksFileNames = [ [], [], [], [], [], [], [], [], [], [] ]
            PersistenceCounterDelay = TotalMessages + ( InitPreloadBuffer * 2 )
            #MessageDump = []

            pubWebsocketTkn = GetPublicWebsocketToken()

            Token = pubWebsocketTkn[ "data" ][ "token" ]
            Endpoint = pubWebsocketTkn[ "data" ][ "instanceServers" ][ 0 ][ "endpoint" ]
            PingInterval = float( pubWebsocketTkn[ "data" ][ "instanceServers" ][ 0 ][ "pingInterval" ] ) / 1000
            #PingInterval = 10

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
    global PersistenceCounterDelay

    ConnectId = 0
    TotalMessages = 0
    PersistenceCounterDelay = InitPreloadBuffer * 2

    PubWebsocketConnect()

    seconds = 0
    while True:
        if DisconnectedFlg:
            PubWebsocketConnect()                                   # (this call triggered here turns-off the DisconnectedFlg)
        time.sleep( 10 )
        seconds += 10
        if VERBOSE_ON or not SILENT_ON:
            print( "Main loop wait seconds: " + str( seconds ) )



global Token
global Endpoint
global PingInterval
global ConnectId
global PubWebsocket
global BooksFeed
global Books
global VerifyBooks
global LastPing
global TotalMessages
global DisconnectedFlg
global SubscriptionCounter
global ProcessingOnMessage
global StopPinging
global InstrumentReloadPending
global BooksFileNames
global PersistenceCounterDelay
#global MessageDump

main()
