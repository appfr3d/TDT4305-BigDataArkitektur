from pyspark import SparkContext, SparkConf
import base64
from datetime import datetime
from operator import add
import math

conf = SparkConf().setAppName("bigdata-prosjekt").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Task 1

# Read csv files into RDDs
posts_file_unfiltered = sc.textFile("data/posts.csv")
posts_file_header = posts_file_unfiltered.first()
posts_file = posts_file_unfiltered.filter(lambda row: row != posts_file_header)

comments_file_unfiltered = sc.textFile("data/comments.csv")
comments_file_header = comments_file_unfiltered.first()
comments_file = comments_file_unfiltered.filter(lambda row: row != comments_file_header)

users_file_unfiltered = sc.textFile("data/users.csv")
users_file_header = users_file_unfiltered.first()
users_file = users_file_unfiltered.filter(lambda row: row != users_file_header)

badges_file_unfiltered = sc.textFile("data/badges.csv")
badges_file_header = badges_file_unfiltered.first()
badges_file = badges_file_unfiltered.filter(lambda row: row != badges_file_header)

# print('Number of rows in posts.csv    :', posts_file.count())
# print('Number of rows in comments.csv :', comments_file.count())
# print('Number of rows in users.csv    :', users_file.count())
# print('Number of rows in badges.csv   :', badges_file.count())


# Task 2.1
# Map rows to questions, answers and comments
# posts = posts_file.map(lambda line: line.split("\t"))
# questions = posts.filter(lambda p: p[1] == "1")
# answers = posts.filter(lambda p: p[1] == "2")
# comments = comments_file.map(lambda line: line.split("\t"))

# Calculate the average text length
# avg_len_questions = questions.map(lambda q: len(base64.b64decode(p[5]))).mean()
# avg_len_answers = answers.map(lambda a: len(base64.b64decode(a[5]))).mean()
# avg_len_comments = comments.map(lambda c: len(base64.b64decode(c[2]))).mean()

# print('Average length of questions :', avg_len_questions)
# print('Average length of answers   :', avg_len_answers)
# print('Average length of comments  :', avg_len_comments)

# Task 2.2
# Map questions to owner id and creation date
# question_name_date = questions.map(lambda q: (q[6], datetime.strptime(q[2], "%Y-%m-%d %H:%M:%S")))

# Find first and last creation question 
# question_name_date_min = question_name_date.min(key=lambda x: x[1])
# question_name_date_max = question_name_date.max(key=lambda x: x[1])

# Find question owner names
users = users_file.map(lambda line: line.split("\t"))
# name_min = users.filter(lambda u: u[0] == question_name_date_min[0]).first()
# name_max = users.filter(lambda u: u[0] == question_name_date_max[0]).first()

# print("First created post: date = " + str(question_name_date_min[1]) + ", name = " + name_min[3])
# print("Last  created post: date = " + str(question_name_date_max[1]) + ", name = " + name_max[3])

# Task 2.3
# Do we need to find more than one?
# print(posts.filter(lambda p: p[6] != "-1").map(lambda p: (p[6], 1)).reduceByKey(add).max(key=lambda p: p[1]))

# Task 2.4
# badges = badges_file.map(lambda line: line.split("\t"))
# print(badges.map(lambda b: (b[0], 1)).reduceByKey(add).filter(lambda b: b[1] < 3).count())

# Task 2.5
def PCC_RDD(X, Y):
  """
  Function to find linear correlation between X and Y
  :param X
      RDD with 1 column of either int or float
  :param Y
      RDD with 1 column of either int or float
  :return
      correlation between -1 and 1. 0 means no linear correlation.
  """

  # find mean values
  _X = X.mean()
  _Y = Y.mean()

  # zip the RDDs so each row is the format (x, y)
  zipped_X_Y = X.zip(Y)
  # x = v[0], y = v[1]
  numerator = zipped_X_Y.map(lambda v: (v[0] - _X) * (v[1] - _Y)).sum()

  denominator_X = math.sqrt(X.map(lambda x: (x - _X)**2).sum())
  denominator_Y = math.sqrt(Y.map(lambda y: (y - _Y)**2).sum())

  return numerator / (denominator_X * denominator_Y)

user_upvotes = users.map(lambda u:  int(u[7]))
user_downvotes = users.map(lambda u: int(u[8]))

print(PCC_RDD(user_upvotes, user_downvotes))


# Task 2.6
