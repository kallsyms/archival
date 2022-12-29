#!/bin/sh
ct="45019604ef2d"

echo "users"
docker exec $ct mongo parler --eval "db.user.count()" --quiet

echo "posts"
docker exec $ct mongo parler --eval "db.post.count()" --quiet

echo "comments"
docker exec $ct mongo parler --eval "db.comment.count()" --quiet

# echo "stats"
# docker exec $ct mongo parler --eval "db.runCommand({dbStats: 1})" --quiet
