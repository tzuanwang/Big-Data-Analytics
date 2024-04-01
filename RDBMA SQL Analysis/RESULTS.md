# Homework 1 results

Your name: Tzu-An Wang

## Part 1
Paste the results of your queries for each question given in the README below:

1. Tracks whose lyricist name begins with W - ID: 22344; Name: Outburst
   Tracks whose lyricist name begins with W - ID: 66084; Name: Got My Modem Working
   Tracks whose lyricist name begins with W - ID: 66096; Name: Mistress Song

2. The field can take values from: None
   The field can take values from: Radio-Safe
   The field can take values from: Radio-Unsafe
   The field can take values from: Adults-Only

3. Track with the most listens - ID: 62460; Name: Siesta

4. Number of artists that have related projects: 453

5. Non-null language codes that have exactly 4 tracks: de
   Non-null language codes that have exactly 4 tracks: ru

6. Number of tracks by artists only active within 1990s: 34

7. Top 3 artists with largest number of distinct album producers: U Can Unlearn Guitar
   Top 3 artists with largest number of distinct album producers: Ars Sonor
   Top 3 artists with largest number of distinct album producers: Disco Missile

8. Track with largest difference - ID: 76008; TITLE: JessicaBD; NAME: Cody Goss

## Part 2

- Execution time before optimization: 
  Mean time: 0.061 [seconds/query]
  Best time: 0.012 [seconds/query]
- Execution time after optimization:
  Mean time: 0.047 [seconds/query]
  Best time   : 0.009 [seconds/query]
- Briefly describe how you optimized for this query:
  Create indices on artist.id, album.id, and track(artist_id, album_id)
- Did you try anything other approaches?  How did they compare to your final answer?
  1. I create index only on album.id and the result turns out to be the same as the result before optimization.
  2. I create index on artist.id and album.id and the result is slower than the final one