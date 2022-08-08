#
# Here are some sample functions that work with the ws_books_v.1.2.py WebSockets Books feed software.  Enjoy.
#



#
# in a <trading_software>.config file there's a setting like this that we read to locate the WS books data
#
#               "WEBSOCKET_BOOKS": "../ws_books_v.1.2/books/",
#
# (if we find a value associated with the WS data as above then the UseWSB flag will get set)



#
# this is just a wrapper function that redirects to the "WS" (WebSockets) version of the function in case of support found for ws_books
#
def GetFullOrderBook( instrument ):             # this is the V3 full depth function used by the V3 versions of the GetBaseVolume and GetBaseValue functions

    if ( EXCHANGE == "KUCOIN" ):
        if ( UseWSB ):
            result = GetFullOrderWSBook_kcn( instrument )
            if ( result == False ):                                 # some problem with websockets books use; revert to REST books
                return( [ False, GetFullOrderBook_kcn( instrument ) ] )
            else:
                return( [ True, result ] )
        else:
            return( [ False, GetFullOrderBook_kcn( instrument ) ] )


          
#
# here's a WebSockets version of the order books request
#
def GetFullOrderWSBook_kcn( instrument ):

    # this function checks the global variable path WebsocketBooks to see if a full book is available for the instrument and uses it if found

    instrLen = len( instrument )
    while True:
        while True:
            wsbooksFiles = os.listdir( WebsocketBooks )
            for file in wsbooksFiles:
                if ( file == "BUSY.FLG" ):
                    if ( len( wsbooksFiles ) > 1 ):
                        time.sleep( 1 )
                        break
                    else:
                        return( False )
            try:
                if ( file != "BUSY.FLG" ):
                    break
            except:
                return( False )

        booksFileName1 = ""
        booksFileName2 = ""
        booksFileName = ""
        for file in wsbooksFiles:                       # restart the files check having confirmed there's no ws_books file-writing conflict in the current file set names
            if ( file[ 0 : instrLen ] == instrument ):
                if ( booksFileName1 == "" ):
                    booksFileName1 = file
                else:
                    booksFileName2 = file
                    break
        if ( booksFileName1 == "" ):
            return( False )
        else:
            if ( booksFileName2 == "" ):
                booksFileName = booksFileName1
            else:
                if ( booksFileName2 > booksFileName1 ):
                    booksFileName = booksFileName2
                else:
                    booksFileName = booksFileName1

        fileTimeStr = booksFileName[ ( instrLen + 1 ) : booksFileName.find( ".BOOKS" ) ]
        fileTime = datetime.fromisoformat( fileTimeStr[ 0 : 10 ] + 'T' + fileTimeStr[ 11 : 13 ] + ':' + fileTimeStr[ 14 : 16 ] + ':' + fileTimeStr[ 17 : 19 ] + '.' + fileTimeStr[ 20 : 26 ] + "+00:00" )
        now = datetime.now( pytz.utc )

        if ( now - fileTime ).seconds > 120:
            return( False )                             # if websockets books feed is frozen for over 2 minutes revert to REST full books
        else:
            try:
                with open( WebsocketBooks + booksFileName, 'r' ) as booksFile:
                    booksFileContentStr = str( json.load( booksFile ) )
                    return( booksFileContentStr )
            except:
                time.sleep( 12 )


                
#
# here's a REST API version of the Level 2 data that we'll revert to if the WS books get a bit old
#
def GetFullOrderBook_kcn( instrument ):

    # Get Full Order Book(aggregated):  GET /api/v3/market/orderbook/level2?symbol=BTC-USDT
    #
    # This version 3 Kucoin API function is called by the GetBaseValue_kcn() and GetBaseVolume_kcn version 1 API functions
    #
    # NOTE: The Market_API_Delay user .config setting is over-ridden to use 5.5 seconds in this function

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
            response = str( ( requests.request( http_method, url, headers = headers ) ).content )
            #time.sleep( Market_API_Delay )
            time.sleep( 5.5 )

        except Exception as error:
            print( "API call exception in GetFullOrderBook_kcn( instrument ) (%s)" % error )
            time.sleep( thisAPIRetryDelay )
            thisAPIRetryDelay *= 1.1
            continue

        if ( '"code":"200000"' in response ):
            return( response )

        else:
            print( "API call failed in GetFullOrderBook_kcn(): " + response )
            time.sleep( thisAPIRetryDelay )
            thisAPIRetryDelay *= 1.1
            continue

