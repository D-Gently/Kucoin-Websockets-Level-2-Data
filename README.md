# Kucoin-Websockets-Level-2-Data


VERSION 1.3.x -- THIS VERSION REPLACES 1.2 WHICH HAS PERFORMANCE ISSUES WHEN THE TOTAL MESSAGE FEED EXCEEDS ABOUT 700 MESSAGES PER SECOND.  THIS NEW VERSION CAN RUN ON A STANDARD FREE ORACLE-HOSTED LINUX SERVER AND KEEP UP WITH FEEDS EXCEEDING 2,000 MPS.  THE FUNCTIONALITY IS SPLIT INTO TWO PROGRAMS WHICH SHOULD BE RUN ON THE SAME PLATFORM IN THE SAME FOLDER ON DIFFERENT TERMINALS (USING screen FOR EXAMPLE):

           ws_books_streamer_v.1.3.py       This program subscribes to the Kucoin feed and pre-processes the incoming messages into stream files
           ws_books_level2_v.1.3.1.py       This program reads the stream files and produces level 2 books at a rate of up to around 1 copy of each
                                            of five books (both sides, bid and ask) per about 7 seconds.  The disk on the free Oracle server is apparently
                                            in RAM as the rate of file creation is too high on regular storage.
                                            


Provides real-time websockets-based Level 2 Order Book data from Kucoin on the local system for high performance trading automation.

"DonationWare": If you benefit from this software make a donation.


Websocket Books: Created on Tue Apr  5 11:25:58 2022
@author: D. Gently


CONTACT FOR ASSISTANCE: anon35828167@protonmail.com
(Sample Python code is available on request to assist with use of the local Level 2 data feed.)



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
