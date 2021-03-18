from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import base64
from datetime import datetime
from operator import add
import math

import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", "-ip", type=str, default=None)
args = vars(parser.parse_args())

dataset_path = args['input_path']

# import sys # TODO: read directory path from command line args

conf = SparkConf().setAppName("bigdata-prosjekt").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Task 1

# Read csv files into RDDs
posts_file_unfiltered = sc.textFile(dataset_path + "/posts.csv")
posts_file_header = posts_file_unfiltered.first()
posts_file = posts_file_unfiltered.filter(lambda row: row != posts_file_header)

comments_file_unfiltered = sc.textFile(dataset_path + "/comments.csv")
comments_file_header = comments_file_unfiltered.first()
comments_file = comments_file_unfiltered.filter(lambda row: row != comments_file_header)

users_file_unfiltered = sc.textFile(dataset_path + "/users.csv")
users_file_header = users_file_unfiltered.first()
users_file = users_file_unfiltered.filter(lambda row: row != users_file_header)

badges_file_unfiltered = sc.textFile(dataset_path + "/badges.csv")
badges_file_header = badges_file_unfiltered.first()
badges_file = badges_file_unfiltered.filter(lambda row: row != badges_file_header)

print('Task 1:')
print('Number of rows in posts.csv    :', posts_file.count())
print('Number of rows in comments.csv :', comments_file.count())
print('Number of rows in users.csv    :', users_file.count())
print('Number of rows in badges.csv   :', badges_file.count())


# Task 2.1
print('\nTask 2.1:')
# Map rows to questions, answers and comments
posts = posts_file.map(lambda line: line.split("\t"))
questions = posts.filter(lambda p: p[1] == "1")
answers = posts.filter(lambda p: p[1] == "2")
comments = comments_file.map(lambda line: line.split("\t"))

# Calculate the average text length
avg_len_questions = questions.map(lambda q: len(base64.b64decode(q[5]))).mean()
avg_len_answers = answers.map(lambda a: len(base64.b64decode(a[5]))).mean()
avg_len_comments = comments.map(lambda c: len(base64.b64decode(c[2]))).mean()

print('Average length of questions :', avg_len_questions)
print('Average length of answers   :', avg_len_answers)
print('Average length of comments  :', avg_len_comments)

# Task 2.2
print('\nTask 2.2:')
# Map questions to owner id and creation date
question_name_date = questions.map(lambda q: (q[6], datetime.strptime(q[2], "%Y-%m-%d %H:%M:%S")))

# Find first and last creation question 
question_name_date_min = question_name_date.min(key=lambda x: x[1])
question_name_date_max = question_name_date.max(key=lambda x: x[1])

# Find question owner names
users = users_file.map(lambda line: line.split("\t"))
name_min = users.filter(lambda u: u[0] == question_name_date_min[0]).first()
name_max = users.filter(lambda u: u[0] == question_name_date_max[0]).first()

print("First created post: date = " + str(question_name_date_min[1]) + ", name = " + name_min[3])
print("Last  created post: date = " + str(question_name_date_max[1]) + ", name = " + name_max[3])

# Task 2.3
print('\nTask 2.3:')
# Do we need to find more than one?
not_null = lambda p: p[6] != "NULL" # "null" = -1

user_max_answers = answers.filter(not_null).map(lambda p: (p[6], 1)).reduceByKey(add).max(key=lambda p: p[1])
user_max_questions = questions.filter(not_null).map(lambda p: (p[6], 1)).reduceByKey(add).max(key=lambda p: p[1])

print("User id of user with most answers  :", user_max_answers)
print("User id of user with most questions:", user_max_questions)

# Task 2.4
print('\nTask 2.4:')
badges = badges_file.map(lambda line: line.split("\t"))
less_than_three_badges = badges.map(lambda b: (b[0], 1)).reduceByKey(add).filter(lambda b: b[1] < 3).count()
print("Number of users who received less than three badges:", less_than_three_badges)


# Task 2.5
print('\nTask 2.5:')
X = users.map(lambda u: int(u[7]))  # user_upvotes
Y = users.map(lambda u: int(u[8]))  # user_downvotes

# find mean values
_X = X.mean()
_Y = Y.mean()

# zip the RDDs so each row is the format (x, y)
zipped_X_Y = X.zip(Y)

# x = v[0], y = v[1]
numerator = zipped_X_Y.map(lambda v: (v[0] - _X) * (v[1] - _Y)).sum()

denominator_X = math.sqrt(X.map(lambda x: (x - _X)**2).sum())
denominator_Y = math.sqrt(Y.map(lambda y: (y - _Y)**2).sum())

print("Pearson correlation coefficient:", numerator / (denominator_X * denominator_Y))


# Task 2.6
print('\nTask 2.6:')
# users with one or more comment
# user: (id, num_comments)
users_with_comments = comments.map(lambda c: (c[4], 1)).reduceByKey(add)

num_comments = comments.count()

P = users_with_comments.map(lambda u: u[1]/num_comments)

H = -1*P.map(lambda p_x: p_x*math.log2(p_x)).sum()
print("Entropy of id of users whom commented:", H)


# Task 3.1
print('\nTask 3.1:')
# (post_id, commenter_id)
c_postid_userid = comments.map(lambda c: (c[0], c[4]))

# (post_id, poster_id)
p_postid_userid = posts.map(lambda p: (p[0], p[6]))

# Structure:
# after join        : (post_id, (commenter_id, poster_id))
# after map         : ((commenter_id, poster_id), 1)
# after reduceByKey : ((commenter_id, poster_id), weight)
# after map         : (commenter_id, poster_id, weight)
edges = c_postid_userid.join(p_postid_userid).map(lambda x: (x[1], 1)).reduceByKey(add).map(lambda x: (x[0][0], x[0][1], x[1]))

print('Done creating graph!')
# Test:
# print("test first edge:", edges.first())
# print("test max weight edge:", edges.max(key=lambda e: e[2]))
# max weigh is from a user to the same user, which make sense as one usually ansewer to comments
# on your own post. 


# Task 3.2
print('\nTask 3.2:')
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(edges, ["commenter_id", "poster_id", "weight"])
df.show()

# Task 3.3 - Find the user ids of top 10 users who wrote the most comments
print('\nTask 3.3:')
df.groupBy("commenter_id").sum("weight").sort("sum(weight)", ascending=False).show(10)

# Task 3.4
print('\nTask 3.4:')
top_ten_recievers_list = df.groupBy("poster_id").sum("weight").sort("sum(weight)", ascending=False).take(10)
top_ten_recievers = sqlContext.createDataFrame(top_ten_recievers_list)

user_df = sqlContext.createDataFrame(users.map(lambda u: (u[0], u[3])), ["user_id", "name"])

# Make the names shorter
ta = top_ten_recievers.alias("ta")
tb = user_df.alias("tb")

ta.join(tb, ta.poster_id==tb.user_id).select("name", "sum(weight)").sort("sum(weight)", ascending=False).show()


# Task 3.5
print('\nTask 3.5:')
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(dataset_path + '/graph_of_posts_and_comments.csv')
print('Done saving file!')