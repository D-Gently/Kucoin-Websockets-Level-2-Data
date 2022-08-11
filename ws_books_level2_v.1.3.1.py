#! /usr/bin/python3
# -*- coding: utf-8 -*-
"""
Websocket Books: Created on Tue Apr  5 11:25:58 2022

@author: D. Gently


Program 2 of 2: - Must be used with ws_books_streamer to create the actual Level2 order books on the local system.
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
# 2022.08.08: version 1.3: Split the functionality into ws_books_streamer_v.1.3.py and ws_books_level2_v.1.3.py to keep up with the feed.
# 2022.08.10: version 1.3.1: Single books verification failure recovery without full re-init.



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
global StreamCounter
global PersistenceSeconds
global InitPreloadBuffer
global PreloadBuffer
global MessagesPerVerify
global WebsocketBooksFolder
global StreamBusyFileSpec
global Level2BusyFileSpec
global StreamFilesListSpec

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
WebsocketBooksFolder = "./books/"
StreamBusyFileSpec = "BUSY.STREAM"
Level2BusyFileSpec = "BUSY.LEVEL2"
StreamFilesListSpec = "FILESLIST.STREAM"

# local system settings:
#
#StreamCounter = 4000
#PersistenceSeconds = 90
#InitPreloadBuffer = 10
#PreloadBuffer = 5
#StreamChunksPerVerify = 15
#
# oracle host with RAM disk:
#
StreamCounter = 2000
PersistenceSeconds = 7
InitPreloadBuffer = 20
PreloadBuffer = 6
StreamChunksPerVerify = 30




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
import os, glob
import copy



############################################################################################################################################
##  ws_books_v.1.0 code
############################################################################################################################################


#-------------------------------------------------------------------------------------------------------------------------------------------
#  general functions
#-------------------------------------------------------------------------------------------------------------------------------------------


def LoadStreamFiles( thisPreloadBuffer, verificationMode = False, lastFileLoaded = "n/a" ):

    global API_Retry_Delay
    global WebsocketBooksFolder
    global StreamBusyFileSpec
    global StreamFilesListSpec

    global BooksFeed
    global VerifyFeed
    global StreamFiles

    while True:
        streamFiles = os.listdir( WebsocketBooksFolder )
        while True:
            if StreamBusyFileSpec in streamFiles:
                time.sleep( 0.05 )
                streamFiles = os.listdir( WebsocketBooksFolder )
            else:
                break
        try:
            with open( WebsocketBooksFolder + StreamFilesListSpec, 'r' ) as streamFilesListFile:
                StreamFiles = json.load( streamFilesListFile )
            if thisPreloadBuffer > 0 and len( StreamFiles ) < thisPreloadBuffer:
                print( "Missing Stream Files in Stream Files Folder ... waiting & retrying" )
                time.sleep( API_Retry_Delay )
                continue
            break
        except Exception as error:
            print( "File open exception: %s" % error )
            time.sleep( API_Retry_Delay )
            continue

    if thisPreloadBuffer > 0:
        if not verificationMode:
            BooksFeed = []
        else:
            VerifyFeed = []
        while True:
            streamFiles = os.listdir( WebsocketBooksFolder )
            while True:
                if StreamBusyFileSpec in streamFiles:
                    time.sleep( 0.05 )
                    streamFiles = os.listdir( WebsocketBooksFolder )
                else:
                    break
            for i in range( thisPreloadBuffer - 1, -1, -1 ):
                try:
                    with open( StreamFiles[ i ], 'r' ) as streamFile:
                        if not verificationMode:
                            BooksFeed = BooksFeed + json.load( streamFile )
                        else:
                            VerifyFeed = VerifyFeed + json.load( streamFile )
                except Exception as error:
                    print( "File access/open issue: %s" % error )
                    time.sleep( API_Retry_Delay )
                    if not verificationMode:
                        BooksFeed = []
                    else:
                        VerifyFeed = []
                    break
            if i == 0:
                break
        return( StreamFiles[ 0 ] )

    elif thisPreloadBuffer == -1:                               # -1 means get a single file, the next one after lastfileloaded which must have been passed
        try:
            nextNdx = StreamFiles.index( lastFileLoaded ) - 1
        except:
            print( "WS_BOOKS_LEVEL2 IS FALLING BEHIND WS_BOOKS_STREAMER ... RE-INITIALIZING" )
            return( "STREAM_FILE_NOT_PRESENT" )
        if nextNdx >= 0:
            nextStreamFile = StreamFiles[ nextNdx ]
        else:
            return( lastFileLoaded )
        with open( nextStreamFile, 'r' ) as streamFile:
            if not verificationMode:
                BooksFeed = BooksFeed + json.load( streamFile )
            else:
                VerifyFeed = VerifyFeed + json.load( streamFile )
        return( nextStreamFile )



def PersistBooks( ndx, name, sequence, bids, asks ):

    global BooksFileNames

    booksDict = { name: [ { 'sequence': sequence },
                          { 'bids': bids },
                          { 'asks': asks } ] }

    ext = ".BOOKS"
    ts = str( datetime.now( pytz.utc ) ).replace( "+00:00", '' ).replace( ' ', '-' ).replace( ':', '-' ).replace( '.', '-' )
    booksFileName = "books/" + name + '-' + ts + ext
    if len( BooksFileNames[ ndx ] ) == 2:
        os.remove( BooksFileNames[ ndx ][ 1 ] )
        del BooksFileNames[ ndx ][ 1 ]
    BooksFileNames[ ndx ].insert( 0, booksFileName )

    with open( booksFileName, 'w' ) as booksFile:
        json.dump( booksDict, booksFile, indent = 4 )



def SyncToFeed():       # process a preprocessed-by-ws_books_streamer_v.1.3.py message from the FIFO queue

    global BooksFeed
    global VerifyFeed
    global Books
    global SaveBooks
    global VerifyBooks
    global Verifying
    global VerifyInstrNdx

    instrNdx = BooksFeed[ 0 ][ 0 ]
    updateBook = BooksFeed[ 0 ][ 1 ]
    priceStr = BooksFeed[ 0 ][ 2 ][ 0 ]
    sizeStr = BooksFeed[ 0 ][ 2 ][ 1 ]
    seqStr = BooksFeed[ 0 ][ 2 ][ 2 ]
    priceFlt = BooksFeed[ 0 ][ 3 ]

    if Verifying < 3:                                                                   # pause BooksFeed processing during the repair phase of a REST / Websockets mismatch
        if seqStr > Books[ instrNdx ][ 0 ]:

            # here's the update logic as per the Kucoin api docs at https://docs.kucoin.com/?lang=en_US#market-snapshot
            #
            bookLen = len( Books[ instrNdx ][ updateBook ] )
            if priceStr != '0' and sizeStr == '0':                                      # when there's a price but the size is 0, remove the corresponding price record
                for i in range( 0, bookLen ):
                    if Books[ instrNdx ][ updateBook ][ i ][ 0 ] == priceStr:
                        break
                if i < bookLen:
                    del Books[ instrNdx ][ updateBook ][ i ]
            elif priceStr != '0' and sizeStr != '0':                                    # when there's a price and a non-zero size, update or add as necessary the price record
                i = 0
                thisBooksPrice = float( Books[ instrNdx ][ updateBook ][ i ][ 0 ] )
                if updateBook == 1:                                                     # updating new size onto existing price or new record into bid book
                    if priceFlt < thisBooksPrice:
                        while priceFlt < thisBooksPrice:
                            i += 1
                            if i < bookLen:
                                thisBooksPrice = float( Books[ instrNdx ][ updateBook ][ i ][ 0 ] )
                            else:
                                break
                else:                                                                   # updating new size onto existing price or new record into ask book
                    if priceFlt > thisBooksPrice:
                        while priceFlt > thisBooksPrice:
                            i += 1
                            if i < bookLen:
                                thisBooksPrice = float( Books[ instrNdx ][ updateBook ][ i ][ 0 ] )
                            else:
                                break
                if priceFlt == thisBooksPrice:
                    Books[ instrNdx ][ updateBook ][ i ][ 1 ] = sizeStr
                else:
                    Books[ instrNdx ][ updateBook ].insert( i, [ priceStr, sizeStr ] )

                    if i == 0:                                                          # situation only arises when new order is at the top of the book
                        if updateBook == 1:                                             # a new bid was just added
                            altBook = 2
                            while float( Books[ instrNdx ][ altBook ][ 0 ][ 0 ] ) <= priceFlt:
                                del Books[ instrNdx ][ altBook ][ 0 ]
                        else:
                            altBook = 1
                            while float( Books[ instrNdx ][ altBook ][ 0 ][ 0 ] ) >= priceFlt:
                                del Books[ instrNdx ][ altBook ][ 0 ]

            Books[ instrNdx ][ 0 ] = seqStr                                             # in all cases, including neither price nor size, make sure to update the sequence number

        del BooksFeed[ 0 ]

    if len( VerifyFeed ) > 0 and Verifying == 2:
        instrNdx = VerifyFeed[ 0 ][ 0 ]

        if instrNdx == VerifyInstrNdx:
            updateBook = VerifyFeed[ 0 ][ 1 ]
            priceStr = VerifyFeed[ 0 ][ 2 ][ 0 ]
            sizeStr = VerifyFeed[ 0 ][ 2 ][ 1 ]
            seqStr = VerifyFeed[ 0 ][ 2 ][ 2 ]
            priceFlt = VerifyFeed[ 0 ][ 3 ]

            if seqStr > SaveBooks[ instrNdx ][ 0 ]:

                bookLen = len( SaveBooks[ instrNdx ][ updateBook ] )
                if priceStr != '0' and sizeStr == '0':                              # when there's a price but the size is 0, remove the corresponding price record
                    for i in range( 0, bookLen ):
                        if SaveBooks[ instrNdx ][ updateBook ][ i ][ 0 ] == priceStr:
                            break
                    if i < bookLen:
                        del SaveBooks[ instrNdx ][ updateBook ][ i ]
                elif priceStr != '0' and sizeStr != '0':                            # when there's a price and a non-zero size, update or add as necessary the price record
                    i = 0
                    thisBooksPrice = float( SaveBooks[ instrNdx ][ updateBook ][ i ][ 0 ] )
                    if updateBook == 1:                                             # updating new size onto existing price or new record into bid book
                        if priceFlt < thisBooksPrice:
                            while priceFlt < thisBooksPrice:
                                i += 1
                                if i < bookLen:
                                    thisBooksPrice = float( SaveBooks[ instrNdx ][ updateBook ][ i ][ 0 ] )
                                else:
                                    break
                    else:                                                           # updating new size onto existing price or new record into ask book
                        if priceFlt > thisBooksPrice:
                            while priceFlt > thisBooksPrice:
                                i += 1
                                if i < bookLen:
                                    thisBooksPrice = float( SaveBooks[ instrNdx ][ updateBook ][ i ][ 0 ] )
                                else:
                                    break
                    if priceFlt == thisBooksPrice:
                        SaveBooks[ instrNdx ][ updateBook ][ i ][ 1 ] = sizeStr
                    else:
                        SaveBooks[ instrNdx ][ updateBook ].insert( i, [ priceStr, sizeStr ] )

                        if i == 0:                                                  # situation only arises when new order is at the top of the book
                            if updateBook == 1:                                     # a new bid was just added
                                altBook = 2
                                while float( SaveBooks[ instrNdx ][ altBook ][ 0 ][ 0 ] ) <= priceFlt:
                                    del SaveBooks[ instrNdx ][ altBook ][ 0 ]
                            else:
                                altBook = 1
                                while float( SaveBooks[ instrNdx ][ altBook ][ 0 ][ 0 ] ) >= priceFlt:
                                    del SaveBooks[ instrNdx ][ altBook ][ 0 ]

                SaveBooks[ instrNdx ][ 0 ] = seqStr                                 # in all cases, including neither price nor size, make sure to update the sequence number

        del VerifyFeed[ 0 ]

    if len( VerifyFeed ) > 0 and Verifying == 3:
        for j in range( 0, 3 ):
            instrNdx = VerifyFeed[ 0 ][ 0 ]
            if instrNdx == VerifyInstrNdx:
                updateBook = VerifyFeed[ 0 ][ 1 ]
                priceStr = VerifyFeed[ 0 ][ 2 ][ 0 ]
                sizeStr = VerifyFeed[ 0 ][ 2 ][ 1 ]
                seqStr = VerifyFeed[ 0 ][ 2 ][ 2 ]
                priceFlt = VerifyFeed[ 0 ][ 3 ]

                if seqStr > SaveBooks[ instrNdx ][ 0 ]:

                    bookLen = len( SaveBooks[ instrNdx ][ updateBook ] )
                    if priceStr != '0' and sizeStr == '0':                              # when there's a price but the size is 0, remove the corresponding price record
                        for i in range( 0, bookLen ):
                            if SaveBooks[ instrNdx ][ updateBook ][ i ][ 0 ] == priceStr:
                                break
                        if i < bookLen:
                            del SaveBooks[ instrNdx ][ updateBook ][ i ]
                    elif priceStr != '0' and sizeStr != '0':                            # when there's a price and a non-zero size, update or add as necessary the price record
                        i = 0
                        thisBooksPrice = float( SaveBooks[ instrNdx ][ updateBook ][ i ][ 0 ] )
                        if updateBook == 1:                                             # updating new size onto existing price or new record into bid book
                            if priceFlt < thisBooksPrice:
                                while priceFlt < thisBooksPrice:
                                    i += 1
                                    if i < bookLen:
                                        thisBooksPrice = float( SaveBooks[ instrNdx ][ updateBook ][ i ][ 0 ] )
                                    else:
                                        break
                        else:                                                           # updating new size onto existing price or new record into ask book
                            if priceFlt > thisBooksPrice:
                                while priceFlt > thisBooksPrice:
                                    i += 1
                                    if i < bookLen:
                                        thisBooksPrice = float( SaveBooks[ instrNdx ][ updateBook ][ i ][ 0 ] )
                                    else:
                                        break
                        if priceFlt == thisBooksPrice:
                            SaveBooks[ instrNdx ][ updateBook ][ i ][ 1 ] = sizeStr
                        else:
                            SaveBooks[ instrNdx ][ updateBook ].insert( i, [ priceStr, sizeStr ] )

                            if i == 0:                                                  # situation only arises when new order is at the top of the book
                                if updateBook == 1:                                     # a new bid was just added
                                    altBook = 2
                                    while float( SaveBooks[ instrNdx ][ altBook ][ 0 ][ 0 ] ) <= priceFlt:
                                        del SaveBooks[ instrNdx ][ altBook ][ 0 ]
                                else:
                                    altBook = 1
                                    while float( SaveBooks[ instrNdx ][ altBook ][ 0 ][ 0 ] ) >= priceFlt:
                                        del SaveBooks[ instrNdx ][ altBook ][ 0 ]

                    SaveBooks[ instrNdx ][ 0 ] = seqStr                                 # in all cases, including neither price nor size, make sure to update the sequence number

            del VerifyFeed[ 0 ]
            verifyFeedLen = len( VerifyFeed )
            if verifyFeedLen == 0:
                break
        if verifyFeedLen == 0:
            Verifying = 4                                                               # message back to main() that the verification books have been brought to current



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

    global Books
    global SaveBooks
    global VerifyBooks

    if load:
        Books = []
        SaveBooks = [ [], [], [], [], [] ]
        VerifyBooks = [ [], [], [], [], [] ]

    for instrument in instrumentslist:
        print( "        Getting next instrument order book..." )
        response = GetFullOrderBook( instrument )
        if load:
            Books = Books + [ [ response[ "data" ][ "sequence" ], response[ "data" ][ "bids" ], response[ "data" ][ "asks" ] ] ]
        else:
            instrNdx = InstrumentDict[ instrument[ 0:3 ] ]
            if response[ "data" ][ "sequence" ] > SaveBooks[ instrNdx ][ 0 ]:
                VerifyBooks[ instrNdx ] = [ response[ "data" ][ "sequence" ], response[ "data" ][ "bids" ], response[ "data" ][ "asks" ] ]
            else:
                return( False )

    return( True )



############################################################################################################################################
##  MAINLINE
############################################################################################################################################

def main():

    global InstrumentsList
    global StreamCounter
    global PersistenceSeconds
    global InitPreloadBuffer
    global PreloadBuffer
    global StreamChunksPerVerify

    global BooksFeed
    global VerifyFeed
    global Books
    global SaveBooks
    global VerifyBooks
    global TotalMessages
    global BooksFileNames
    global Verifying
    global VerifyInstrNdx

    BooksFeed = [ ]
    VerifyFeed = [ ]
    TotalMessages = 0
    BooksFileNames = [ [], [], [], [], [], [], [], [], [], [] ]

    for file in glob.glob( "books/*.BOOKS" ):
        os.remove( file )
    for file in glob.glob( "books/*.VERIFY" ):
        os.remove( file )

    latestStreamFileLoaded = LoadStreamFiles( InitPreloadBuffer )                   # pre-load specified number of stream files
    nextStreamLoadTime = datetime.now( pytz.utc ) + timedelta( 0, 1 )
    print( "latestStreamFileLoaded: " + latestStreamFileLoaded + "\n" )
    LoadLevel2( InstrumentsList, True )
    print( "\nREST Books init-loaded" )
    count = 0
    verifyCount = 0
    verifyStreamChunks = 0
    VerifyInstrNdx = 0
    Verifying = 0
    preloadCounter = 0
    nextPersistenceTime = datetime.now( pytz.utc ) + timedelta( 0, 60 )

    while True:
        previousStreamFileLoaded = latestStreamFileLoaded
        if datetime.now( pytz.utc ) > nextStreamLoadTime:
            latestStreamFileLoaded = LoadStreamFiles( -1, False, previousStreamFileLoaded )

            if latestStreamFileLoaded == "STREAM_FILE_NOT_PRESENT":                 # re-init required ... re-run copy of the init code

                BooksFeed = [ ]
                VerifyFeed = [ ]
                TotalMessages = 0
                BooksFileNames = [ [], [], [], [], [], [], [], [], [], [] ]

                for file in glob.glob( "books/*.BOOKS" ):
                    os.remove( file )
                for file in glob.glob( "books/*.VERIFY" ):
                    os.remove( file )

                latestStreamFileLoaded = LoadStreamFiles( InitPreloadBuffer )       # pre-load specified number of stream files
                nextStreamLoadTime = datetime.now( pytz.utc ) + timedelta( 0, 1 )
                print( "latestStreamFileLoaded: " + latestStreamFileLoaded  + "\n")
                LoadLevel2( InstrumentsList, True )
                print( "\nREST Books init-loaded" )
                count = 0
                verifyCount = 0
                verifyStreamChunks = 0
                VerifyInstrNdx = 0
                Verifying = 0
                preloadCounter = 0
                nextPersistenceTime = datetime.now( pytz.utc ) + timedelta( 0, 60 )
                continue

            nextStreamLoadTime = datetime.now( pytz.utc ) + timedelta( 0, 1 )
            if latestStreamFileLoaded != previousStreamFileLoaded:
                print( "latestStreamFileLoaded: " + latestStreamFileLoaded )
                verifyStreamChunks += 1
                preloadCounter -= 1

        while len( BooksFeed ) > 0:
            SyncToFeed()
            if Verifying < 3:                                                       # normal BooksFeed is paused during mismatch repair
                count += 1
            else:
                if len( VerifyFeed ) == 0:
                    break
                verifyCount += 1
                if verifyCount % StreamCounter == 0:
                    print( "         Message count: " + str( count ) )
                    print( "           Level2 FIFO: " + str( len( BooksFeed ) ) )
                    print( "     Verification FIFO: " + str( len( VerifyFeed ) ) )
                    break
            if Verifying == 2 and \
               len( SaveBooks[ VerifyInstrNdx ] ) > 0 and len( VerifyBooks[ VerifyInstrNdx ] ) > 0 and \
               SaveBooks[ VerifyInstrNdx ][ 0 ] == VerifyBooks[ VerifyInstrNdx ][ 0 ]:
                break
            if count % StreamCounter == 0:
                print( "         Message count: " + str( count ) )
                print( "           Level2 FIFO: " + str( len( BooksFeed ) ) )
                if Verifying > 0:
                    print( "     Verification FIFO: " + str( len( VerifyFeed ) ) )
                if Verifying == 2:
                    print( "             VERIFYING " + InstrumentsList[ VerifyInstrNdx ]  + ": VerifyBooks SEQ # " + VerifyBooks[ VerifyInstrNdx ][ 0 ] )
                    print( "             VERIFYING " + InstrumentsList[ VerifyInstrNdx ]  + ":   SaveBooks SEQ # " + SaveBooks[ VerifyInstrNdx ][ 0 ] )
                break

        if Verifying == 0 and verifyStreamChunks == StreamChunksPerVerify:
            if len( BooksFeed ) > StreamCounter:                                    # defer verification if not caught up
                verifyStreamChunks = 0
            else:
                Verifying = 1

                # besides saving the books to fast forward to verification data there must be an independent books feed starting far enough back
                #
                latestVerifyFileLoaded = LoadStreamFiles( 2, True )
                nextVerifyLoadTime = datetime.now( pytz.utc ) + timedelta( 0, 1 )

                SaveBooks[ VerifyInstrNdx ] = copy.deepcopy( Books[ VerifyInstrNdx ] )
                preloadCounter = PreloadBuffer

        if Verifying > 0 and preloadCounter != PreloadBuffer:                       # (second condition is to keep out of this code if the block above just set to Verifying == 1)

            # need to keep the verification feed current as per the books feed until done with it
            #
            previousVerifyFileLoaded = latestVerifyFileLoaded
            if datetime.now( pytz.utc ) > nextVerifyLoadTime:
                latestVerifyFileLoaded = LoadStreamFiles( -1, True, previousVerifyFileLoaded )

                if latestStreamFileLoaded == "STREAM_FILE_NOT_PRESENT":             # re-init required ... re-run copy of the init code

                    BooksFeed = [ ]
                    VerifyFeed = [ ]
                    TotalMessages = 0
                    BooksFileNames = [ [], [], [], [], [], [], [], [], [], [] ]

                    for file in glob.glob( "books/*.BOOKS" ):
                        os.remove( file )
                    for file in glob.glob( "books/*.VERIFY" ):
                        os.remove( file )

                    latestStreamFileLoaded = LoadStreamFiles( InitPreloadBuffer )   # pre-load specified number of stream files
                    nextStreamLoadTime = datetime.now( pytz.utc ) + timedelta( 0, 1 )
                    print( "latestStreamFileLoaded: " + latestStreamFileLoaded  + "\n")
                    LoadLevel2( InstrumentsList, True )
                    print( "\nREST Books init-loaded" )
                    count = 0
                    verifyCount = 0
                    verifyStreamChunks = 0
                    VerifyInstrNdx = 0
                    Verifying = 0
                    preloadCounter = 0
                    nextPersistenceTime = datetime.now( pytz.utc ) + timedelta( 0, 60 )
                    continue

                nextVerifyLoadTime = datetime.now( pytz.utc ) + timedelta( 0, 1 )
                if latestVerifyFileLoaded != previousVerifyFileLoaded:
                    print( "latestVerifyFileLoaded: " + latestVerifyFileLoaded )

        if Verifying == 1 and preloadCounter == 0:
            print( "\n" )
            if not LoadLevel2( [ InstrumentsList[ VerifyInstrNdx ] ], False ):
                preloadCounter = PreloadBuffer
                print( "                        ...VerifyBooks for " + InstrumentsList[ VerifyInstrNdx ] + " are too old.  RETRYING...\n")
            else:
                Verifying = 2
                print( "                        ...REST Books verify-loaded for: " + InstrumentsList[ VerifyInstrNdx ] + "\n" )

        if Verifying == 2 and \
           len( SaveBooks[ VerifyInstrNdx ] ) > 0 and len( VerifyBooks[ VerifyInstrNdx ] ) > 0 and \
           SaveBooks[ VerifyInstrNdx ][ 0 ] == VerifyBooks[ VerifyInstrNdx ][ 0 ]:
            if SaveBooks[ VerifyInstrNdx ] == VerifyBooks[ VerifyInstrNdx ]:
                print( "\n   ===>>>   CONFIRMED MATCH between REST & Websockets: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===" )
                print( "   ===>>>   CONFIRMED MATCH between REST & Websockets: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===" )
                print( "   ===>>>   CONFIRMED MATCH between REST & Websockets: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===\n" )
                verifyStreamChunks = 0
                VerifyInstrNdx += 1
                if VerifyInstrNdx == 5:
                    VerifyInstrNdx = 0
                Verifying = 0
                SaveBooks = [ [], [], [], [], [] ]
                VerifyBooks = [ [], [], [], [], [] ]
            else:
                print( "\n   ===>>>   mismatch!!! mismatch!!! REST & Websockets: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===" )
                print( "   ===>>>   mismatch!!! mismatch!!! REST & Websockets: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===" )
                print( "   ===>>>   mismatch!!! mismatch!!! REST & Websockets: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===\n" )
                print( "WS_BOOKS_LEVEL2 WEBSOCKETS DATA DOES NOT MATCH REST DATA... RE-INIT " + InstrumentsList[ VerifyInstrNdx ] + "\n" )
                SaveBooks[ VerifyInstrNdx ] = copy.deepcopy( VerifyBooks[ VerifyInstrNdx ] )
                Verifying = 3
                verifyCount = 0

        if Verifying == 4:                                                          # this is how the repair mode of SyncToFeed() signals the VerifyBooks have been brought to current
            Books[ VerifyInstrNdx ] = copy.deepcopy( SaveBooks[ VerifyInstrNdx ] )
            print( "\n   ===>>>   Replaced Level2 Books with latest Verified copy: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===" )
            print( "   ===>>>   Replaced Level2 Books with latest Verified copy: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===" )
            print( "   ===>>>   Replaced Level2 Books with latest Verified copy: " + InstrumentsList[ VerifyInstrNdx ] + "   <<<===\n" )
            verifyStreamChunks = 0
            VerifyInstrNdx += 1
            if VerifyInstrNdx == 5:
                VerifyInstrNdx = 0
            Verifying = 0
            SaveBooks = [ [], [], [], [], [] ]
            VerifyBooks = [ [], [], [], [], [] ]

        if datetime.now( pytz.utc ) > nextPersistenceTime:                          # defer level2 data persistence until caught up
            if Verifying < 3:
                print( "\n    PERSISTING LEVEL2 BOOKS TO DISK.  START..." )
                with open( Level2BusyFileSpec, 'w' ) as flagFile:
                    json.dump( "WAIT!", flagFile )
                for i in range( 0, 5 ):
                    PersistBooks( i, InstrumentsList[ i ], Books[ i ][ 0 ], Books[ i ][ 1 ], Books[ i ][ 2 ] )
                os.remove( Level2BusyFileSpec )
                print( "                                                   ...DONE\n" )
            nextPersistenceTime = datetime.now( pytz.utc ) + timedelta( 0, PersistenceSeconds - 1.5 )   # (-1.5 seconds it takes to get down here on average per loop)



global BooksFeed
global VerifyFeed
global Books
global SaveBooks
global VerifyBooks
global TotalMessages
global StreamFiles
global BooksFileNames
global Verifying
global VerifyInstrNdx

main()
