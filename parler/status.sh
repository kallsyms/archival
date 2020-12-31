#!/bin/sh
echo "users"
docker exec parler mongo parler --eval "db.users.count()" --quiet

echo "posts"
docker exec parler mongo parler --eval "db.posts.count()" --quiet

echo "comments"
docker exec parler mongo parler --eval "db.comments.count()" --quiet
