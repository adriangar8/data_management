import redis

# Connect redis and python
r = redis.StrictRedis(host="localhost", port=6379, db=0)

"""
---------------------------------------------------------------
"""

# Create sets for <user>:voted_list
r.sadd('Pol:voted_list', 'yt', 'google', 'yah', 'tv')
r.sadd('Adrian:voted_list', 'google', 'snap', 'yt')
r.sadd('Antonio:voted_list', 'acer', 'hp', 'uab')
r.sadd('Alex:voted_list', 'J&J', 'google', 'apple')
r.sadd('Jack:voted_list', 'insta')
r.sadd('Roger:voted_list', 'pais', 'microsoft', 'nike')
r.sadd('Joan:voted_list', 'yt', 'hp', 'apple', 'levis')
r.sadd('Xavier:voted_list', 'uab', 'eastpack', 'sony')
r.sadd('Marc:voted_list', 'insta', 'meta', 'J&J')
r.sadd('Paula:voted_list', 'yt', 'nike', 'apple', 'gmail', 'pytorch')
r.sadd('Lucia:voted_list', 'acer', 'apple')
r.sadd('Patricia:voted_list', 'mit', 'vanguardia', 'leetcode')

# Create sorted sets for <user>:time_spent
r.zadd('Pol:time_spent', {'yt': 120, 'google': 85, 'yah': 45, 'tv': 15})
r.zadd('Adrian:time_spent', {'google': 60, 'snap': 110, 'yt': 30})
r.zadd('Antonio:time_spent', {'acer': 50, 'hp': 70, 'uab': 25})
r.zadd('Alex:time_spent', {'J&J': 95, 'google': 105, 'apple': 40})
r.zadd('Jack:time_spent', {'insta': 295})
r.zadd('Roger:time_spent', {'pais': 90, 'microsoft': 55, 'nike': 35})
r.zadd('Joan:time_spent', {'yt': 100, 'hp': 95, 'apple': 60, 'levis': 100})
r.zadd('Xavier:time_spent', {'uab': 85, 'eastpack': 70, 'sony': 45})
r.zadd('Marc:time_spent', {'insta': 120, 'meta': 65, 'J&J': 40})
r.zadd('Paula:time_spent', {'yt': 90, 'nike': 80, 'apple': 110, 'gmail': 5, 'pytorch': 30})
r.zadd('Lucia:time_spent', {'acer': 50, 'apple': 105, 'linux': 65})
r.zadd('Patricia:time_spent', {'mit': 75, 'vanguardia': 65, 'leetcode': 55})

# Create a sorted set in which each user has the times he/she has posted
r.zadd('posted', {
    'user:Pol': 12,
    'user:Adrian': 53,
    'user:Antonio': 70,
    'user:Alex': 10,
    'user:Jack': 45,
    'user:Roger': 26,
    'user:Joan': 19,
    'user:Xavier': 11,
    'user:Marc': 14,
    'user:Paula': 36,
    'user:Lucia': 3,
    'user:Patricia': 52,
})

"""
---------------------------------------------------------------
"""

# List of all users
users = ['Pol', 'Adrian', 'Antonio', 'Alex', 'Jack', 'Roger', 'Joan', 'Xavier', 'Marc', 'Paula', 'Lucia', 'Patricia']

# Loop through each user to compute the activity score
for user in users:
    
    # 1. Get the score from the "posted" sorted set
    posted_score = r.zscore('posted', 'user:' + user) or 0

    # 2. Get the length of the corresponding "<user>:voted_list"
    voted_length = r.scard(user + ':voted_list')

    # 3. Get the total score from the corresponding "<user>:time_spent" sorted set
    time_spent_scores = [score for _, score in r.zrangebyscore(user + ':time_spent', '-inf', 'inf', withscores=True)]
    time_spent_score = sum(time_spent_scores)

    # Compute the activity score using the given weighted sum formula
    activity_score = (posted_score * 0.6) + (voted_length * 0.3) + (time_spent_score * 0.1)
    
    # Store the computed activity score in the "activity" sorted set
    r.zadd('activity', {'user:' + user: activity_score})

"""
---------------------------------------------------------------
"""

top_10_users = r.zrevrange('activity', 0, 9, withscores=True)

print("\nTop 10 users based on activity:\n")

for tuple in top_10_users:
    user = (tuple[0]).decode('utf-8')
    print(user, "with an activity score of", round(int(tuple[1])))

print()

"""
---------------------------------------------------------------
"""