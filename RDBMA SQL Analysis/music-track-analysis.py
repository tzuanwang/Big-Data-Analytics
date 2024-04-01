#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
#   python lab1_part1.py music_small.db

import sys
import sqlite3


# The database file should be given as the first argument on the command line
# Please do not hard code the database file!
db_file = sys.argv[1]

# We connect to the database using 
with sqlite3.connect(db_file) as conn:
    # This query counts the number of artists who became active in 1990
    year = (1990,)
    for row in conn.execute('SELECT count(*) FROM artist WHERE artist_active_year_begin=?', year):
        # Since there is no grouping here, the aggregation is over all rows
        # and there will only be one output row from the query, which we can
        # print as follows:
        print('Tracks from {}: {}'.format(year[0], row[0]))
        
        # The [0] bits here tell us to pull the first column out of the 'year' tuple
        # and query results, respectively.

    # ADD YOUR CODE STARTING HERE
    
    # Question 1
    print('Question 1:')
    
    # implement your solution to q1
    char = ("W",)
    for row in conn.execute('SELECT id, track_title, track_lyricist FROM track WHERE substr(track_lyricist, 1, 1)=?', char):
        print('Tracks whose lyricist name begins with {} - ID: {}; Name: {}'.format(char[0], row[0], row[1]))
    
    print('---')
    
    # Question 2
    print('Question 2:')
    
    # implement your solution to q2
    for row in conn.execute('SELECT DISTINCT(track_explicit) FROM track'):
        print('The field can take values from: {}'.format(row[0]))
    
    print('---')
    
    # Question 3
    print('Question 3:')
    
    # implement your solution to q3
    for row in conn.execute('SELECT id, track_title FROM track ORDER BY track_listens DESC LIMIT 1'):
        print('Track with the most listens - ID: {}; Name: {}'.format(row[0], row[1]))
    
    print('---')
    
    # Question 4
    print('Question 4:')
    
    # implement your solution to q4
    for row in conn.execute('SELECT COUNT(artist_related_projects) FROM artist'):
        print('Number of artists that have related projects: {}'.format(row[0]))
    
    print('---')
    
    # Question 5
    print('Question 5:')
    
    # implement your solution to q5
    num = (4,)
    for row in conn.execute('SELECT track_language_code \
                             FROM track \
                             WHERE track_language_code IS NOT NULL \
                             GROUP BY track_language_code \
                             HAVING COUNT(id)=?', num):
        print('Non-null language codes that have exactly {} tracks: {}'.format(num[0], row[0]))
    
    print('---')
    
    # Question 6
    print('Question 6:')
    
    # implement your solution to q6
    for row in conn.execute('SELECT COUNT(*) \
                             FROM track t \
                             INNER JOIN artist a ON t.artist_id = a.id \
                             WHERE a.artist_active_year_begin BETWEEN 1990 AND 1999 \
                               AND a.artist_active_year_end BETWEEN 1990 AND 1999'):
        print('Number of tracks by artists only active within {}s: {}'.format(year[0], row[0]))
    
    print('---')
    
    # Question 7
    print('Question 7:')
    
    # implement your solution to q7
    top = (3,)
    for row in conn.execute('SELECT artist.artist_name, COUNT(DISTINCT album.album_producer) AS count\
                             FROM artist \
                             INNER JOIN track ON artist.id = track.artist_id \
                             INNER JOIN album ON track.album_id = album.id \
                             GROUP BY artist.artist_name \
                             ORDER BY count DESC \
                             LIMIT ?', top):
        print('Top {} artists with largest number of distinct album producers: {}'.format(top[0], row[0]))
    
    print('---')
    
    # Question 8
    print('Question 8:')
    
    # implement your solution to q8
    for row in conn.execute('SELECT track.id, track.track_title, artist.artist_name, \
                             (album.album_listens - track.track_listens) as diff \
                             FROM artist \
                             INNER JOIN track ON artist.id = track.artist_id \
                             INNER JOIN album ON track.album_id = album.id \
                             ORDER BY diff DESC \
                             LIMIT 1'):
        print('Track with largest difference - ID: {}; TITLE: {}; NAME: {}'.format(row[0], row[1], row[2]))
    
    print('---')
