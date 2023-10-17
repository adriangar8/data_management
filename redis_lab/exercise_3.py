import redis

# Connect redis and python
r = redis.StrictRedis(host="localhost", port=6379, db=0)

r.flushall()
"""
---------------------------------------------------------------
"""

# Create sets for <user>:wishlist
r.lpush('Pol:wishlist', 'web:yt', 'web:google', 'web:yah', 'web:tv')
r.lpush('Adrian:wishlist', 'web:google', 'web:snap', 'web:yt')
r.lpush('Antonio:wishlist', 'web:acer', 'web:hp', 'web:uab')
r.lpush('Alex:wishlist', 'web:J&J', 'web:google', 'web:apple')
r.lpush('Jack:wishlist', 'web:insta')
r.lpush('Roger:wishlist', 'web:pais', 'web:microsoft', 'web:nike')
r.lpush('Joan:wishlist', 'web:yt', 'web:hp', 'web:apple', 'web:levis')
r.lpush('Xavier:wishlist', 'web:uab', 'web:eastpack', 'web:sony')
r.lpush('Marc:wishlist', 'web:insta', 'web:meta', 'web:J&J')
r.lpush('Paula:wishlist', 'web:yt', 'web:nike', 'web:apple', 'web:gmail', 'web:pytorch')
r.lpush('Lucia:wishlist', 'web:acer', 'web:apple')
r.lpush('Patricia:wishlist', 'web:mit', 'web:vanguardia', 'web:leetcode')

# Create sorted sets for <user>:visited_list
r.lpush('Pol:visited_list', 'web:insta', 'web:amazon', 'web:hp', 'web:tiktok', 'web:twitter')
r.lpush('Adrian:visited_list', 'web:uab', 'web:insta', 'web:hp' )
r.lpush('Antonio:visited_list', 'web:cv', 'web:linkedin', 'web:gmaps')
r.lpush('Alex:visited_list', 'web:bershka', 'web:zara', 'web:insta')
r.lpush('Jack:visited_list', 'web:samsung', 'web:apple')
r.lpush('Roger:visited_list', 'web:amazon', 'web:elmundo')
r.lpush('Joan:visited_list', 'web:recetas', 'web:stack', 'web:boss')
r.lpush('Xavier:visited_list', 'web:acer', 'web:google', 'web:h&m', 'web:twitter')
r.lpush('Marc:visited_list', 'web:apple', 'web:yt', 'web:playstation')
r.lpush('Paula:visited_list', 'web:acer', 'web:google', 'web:keras', 'web:wikipedia')
r.lpush('Lucia:visited_list', 'web:gmaps', 'web:amazon', 'web:vanguardia')
r.lpush('Patricia:visited_list', 'web:elmundo', 'web:pais', 'web:eastpack', 'web:nike')

import json
# Create sorted sets for target user --> Jorge
r.lpush('Jorge:wishlist', 'web:eastpack', 'web:insta', 'web:yah', 'web:amazon', 'web:apple')
r.lpush('Jorge:visited_list', 'web:acer', 'web:hp', 'web:gmaps', 'web:twitter', 'web:wikipedia')

# Create merged list for Jorge
wishlist = r.lrange('Jorge:wishlist', 0, -1)
visited_list = r.lrange('Jorge:visited_list', 0, -1)

merge_list = []; 
merge_list.extend(wishlist); merge_list.extend(visited_list)

for item in merge_list:
    r.rpush('Jorge:merge', item)
    r.sadd('Jorge:merged_set', item)


# Merge all the users wishlist with visited_list and compare with jorge
users = ['Pol', 'Adrian', 'Antonio', 'Alex', 'Jack', 
         'Roger', 'Joan', 'Xavier', 'Marc', 'Paula', 
         'Lucia', 'Patricia']

for user in users:
    merge_list = []
    wishlist = r.lrange(user+':wishlist', 0, -1)
    visited_list = r.lrange(user+':visited_list', 0, -1)
    
    merge_list.extend(wishlist); merge_list.extend(visited_list)

    for item in merge_list:
        r.rpush(user+':merge', item)
        r.sadd(user+':merged_set', item)
    

    matches = r.sinter('Jorge:merged_set', user+':merged_set')
    
    for element in list(matches):
        r.sadd('Jorge_'+user+':common_webs', element)
    
    nMatches = r.scard('Jorge_'+user+':common_webs')    
    r.zadd('jorge:user_compatibility', {user:nMatches})


nMatches = r.scard('Jorge_'+user+':common_webs')    
r.zadd('jorge:user_compatibility', {user:nMatches})

top_user = r.zrevrange('jorge:user_compatibility', 0, 0, withscores=True)


print("\n\nThe most similar user with respect to Jorge:")
user = (top_user[0][0]).decode('utf-8')
print(str(user) + ", with a compatibility score of", round(int(top_user[0][1])), 'points.\n')

# Look for non shared webs on all users w.r.t Jorge
for user in r.zrevrange('jorge:user_compatibility', 0, -1, withscores=True):
    name = user[0].decode('utf-8')
    recommended_webs = list(r.sdiff(user[0].decode('utf-8')+':merged_set', 'Jorge:merged_set'))
    recommended_webs = [web.decode('utf-8') for web in recommended_webs]
    if len(recommended_webs) >= 1: 
        break

import random
website = random.choice(recommended_webs).replace('web:', '')
print(f"Website recommended for user is {website}.\n\n" )