from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import base64
from operator import add
from graphframes import *
import argparse

# Read the input arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", "-ip", type=str, default=None)
parser.add_argument("--post_id", "-pi", type=str, default=None)
args = vars(parser.parse_args())

# Save arguments in variables 
dataset_path = args['input_path'] # data
id_of_post = args['post_id']      # 14

# Stopwords from https://gist.github.com/habedi/c7229ee5bd50bf49f5b2bc404366344d
stopwords = ["a","about","above","after","again","against","ain","all","am","an","and",
"any","are","aren","aren't","as","at","be","because","been","before","being",
"below","between","both","but","by","can","couldn","couldn't","d","did","didn",
"didn't","do","does","doesn","doesn't","doing","don","don't","down","during",
"each","few","for","from","further","had","hadn","hadn't","has","hasn","hasn't",
"have","haven","haven't","having","he","her","here","hers","herself","him",
"himself","his","how","i","if","in","into","is","isn","isn't","it","it's","its",
"itself","just","ll","m","ma","me","mightn","mightn't","more","most","mustn",
"mustn't","my","myself","needn","needn't","no","nor","not","now","o","of","off",
"on","once","only","or","other","our","ours","ourselves","out","over","own","re"
,"s","same","shan","shan't","she","she's","should","should've","shouldn",
"shouldn't","so","some","such","t","than","that","that'll","the","their",
"theirs","them","themselves","then","there","these","they","this","those",
"through","to","too","under","until","up","ve","very","was","wasn","wasn't","we"
,"were","weren","weren't","what","when","where","which","while","who","whom",
"why","will","with","won","won't","wouldn","wouldn't","y","you","you'd","you'll"
,"you're","you've","your","yours","yourself","yourselves","could","he'd","he'll"
,"he's","here's","how's","i'd","i'll","i'm","i've","let's","ought","she'd",
"she'll","that's","there's","they'd","they'll","they're","they've","we'd",
"we'll","we're","we've","what's","when's","where's","who's","why's","would",
"able","abst","accordance","according","accordingly","across","act","actually",
"added","adj","affected","affecting","affects","afterwards","ah","almost",
"alone","along","already","also","although","always","among","amongst",
"announce","another","anybody","anyhow","anymore","anyone","anything","anyway",
"anyways","anywhere","apparently","approximately","arent","arise","around",
"aside","ask","asking","auth","available","away","awfully","b","back","became",
"become","becomes","becoming","beforehand","begin","beginning","beginnings",
"begins","behind","believe","beside","besides","beyond","biol","brief","briefly"
,"c","ca","came","cannot","can't","cause","causes","certain","certainly","co",
"com","come","comes","contain","containing","contains","couldnt","date",
"different","done","downwards","due","e","ed","edu","effect","eg","eight",
"eighty","either","else","elsewhere","end","ending","enough","especially","et",
"etc","even","ever","every","everybody","everyone","everything","everywhere",
"ex","except","f","far","ff","fifth","first","five","fix","followed","following"
,"follows","former","formerly","forth","found","four","furthermore","g","gave",
"get","gets","getting","give","given","gives","giving","go","goes","gone","got",
"gotten","h","happens","hardly","hed","hence","hereafter","hereby","herein",
"heres","hereupon","hes","hi","hid","hither","home","howbeit","however",
"hundred","id","ie","im","immediate","immediately","importance","important",
"inc","indeed","index","information","instead","invention","inward","itd",
"it'll","j","k","keep","keeps","kept","kg","km","know","known","knows","l",
"largely","last","lately","later","latter","latterly","least","less","lest",
"let","lets","like","liked","likely","line","little","'ll","look","looking",
"looks","ltd","made","mainly","make","makes","many","may","maybe","mean","means"
,"meantime","meanwhile","merely","mg","might","million","miss","ml","moreover",
"mostly","mr","mrs","much","mug","must","n","na","name","namely","nay","nd",
"near","nearly","necessarily","necessary","need","needs","neither","never",
"nevertheless","new","next","nine","ninety","nobody","non","none","nonetheless",
"noone","normally","nos","noted","nothing","nowhere","obtain","obtained",
"obviously","often","oh","ok","okay","old","omitted","one","ones","onto","ord",
"others","otherwise","outside","overall","owing","p","page","pages","part",
"particular","particularly","past","per","perhaps","placed","please","plus",
"poorly","possible","possibly","potentially","pp","predominantly","present",
"previously","primarily","probably","promptly","proud","provides","put","q",
"que","quickly","quite","qv","r","ran","rather","rd","readily","really","recent"
,"recently","ref","refs","regarding","regardless","regards","related",
"relatively","research","respectively","resulted","resulting","results","right",
"run","said","saw","say","saying","says","sec","section","see","seeing","seem",
"seemed","seeming","seems","seen","self","selves","sent","seven","several",
"shall","shed","shes","show","showed","shown","showns","shows","significant",
"significantly","similar","similarly","since","six","slightly","somebody",
"somehow","someone","somethan","something","sometime","sometimes","somewhat",
"somewhere","soon","sorry","specifically","specified","specify","specifying",
"still","stop","strongly","sub","substantially","successfully","sufficiently",
"suggest","sup","sure","take","taken","taking","tell","tends","th","thank",
"thanks","thanx","thats","that've","thence","thereafter","thereby","thered",
"therefore","therein","there'll","thereof","therere","theres","thereto",
"thereupon","there've","theyd","theyre","think","thou","though","thoughh",
"thousand","throug","throughout","thru","thus","til","tip","together","took",
"toward","towards","tried","tries","truly","try","trying","ts","twice","two","u"
,"un","unfortunately","unless","unlike","unlikely","unto","upon","ups","us",
"use","used","useful","usefully","usefulness","uses","using","usually","v",
"value","various","'ve","via","viz","vol","vols","vs","w","want","wants","wasnt"
,"way","wed","welcome","went","werent","whatever","what'll","whats","whence",
"whenever","whereafter","whereas","whereby","wherein","wheres","whereupon",
"wherever","whether","whim","whither","whod","whoever","whole","who'll",
"whomever","whos","whose","widely","willing","wish","within","without","wont",
"words","world","wouldnt","www","x","yes","yet","youd","youre","z","zero","a's",
"ain't","allow","allows","apart","appear","appreciate","appropriate",
"associated","best","better","c'mon","c's","cant","changes","clearly",
"concerning","consequently","consider","considering","corresponding","course",
"currently","definitely","described","despite","entirely","exactly","example",
"going","greetings","hello","help","hopefully","ignored","inasmuch","indicate",
"indicated","indicates","inner","insofar","it'd","keep","keeps","novel",
"presumably","reasonably","second","secondly","sensible","serious","seriously",
"sure","t's","third","thorough","thoroughly","three","well","wonder"]

# Start Spark enviornment
conf = SparkConf().setAppName("bigdata-prosjekt").setMaster("local[*]")
sc = SparkContext(conf=conf)

### Stage one

# Parse datafile
posts_file_unfiltered = sc.textFile(dataset_path + "/posts.csv.gz")
posts_file_header = posts_file_unfiltered.first()
posts_file = posts_file_unfiltered.filter(lambda row: row != posts_file_header)
posts = posts_file.map(lambda line: line.split("\t"))

# Select one post based on input argument
given_post = posts.filter(lambda post: post[0] == id_of_post)

# 1. and 2.: Decode post body and turn characters to lower case
punctuation = '!"#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~\t'
post_body = given_post.map(lambda p: base64.b64decode(p[5]).decode('utf-8').lower())

# 2 and 3: Remove punktuations and symbols except DOT
def remove_punctuation(text):
  for p in punctuation:
    text = text.replace(p, " ")
  return text

post_no_punctuation = post_body.map(remove_punctuation)

# 4, 5 and 6: Tokenize based on whitespace, remove short tokens and remove DOT characters at beginning and end og tokens
tokens_with_stopwords = post_no_punctuation.map(lambda p: [word.strip(".") for word in p.split() if len(word) >=3])

# 7: Remove stopwords from list of tokens
tokens = tokens_with_stopwords.map(lambda t: [word for word in t if not word in stopwords])

def window(seq):
  num_chunks = ((len(seq) - 5)) + 1
  windows = []
  for i in range(num_chunks):
    windows.append(seq[i:i+5])
  return windows

def make_edges(windows):
  edges = []
  for window in windows:
    for i in range(len(window)):
      for j in range(len(window)):
        if not i == j:
          edges.append((window[i], window[j]))
  return edges

# Create vertices with a given id and the token
vertices = sc.parallelize(tokens.first()).distinct().zipWithIndex().collect()

# for every word in tokens, change it to the id
def map_to_id(tokens):
  token_ids = []
  for token in tokens:
    for node in vertices:
      if node[0] == token:
        token_ids.append(node[1])
  return token_ids

# Create edges that use the same id-s as the vertices
edges = sc.parallelize(tokens.map(map_to_id).map(window).map(make_edges).first()).distinct()

# Create a sqlContext and map the vertices and edges to dataframes
sqlContext = SQLContext(sc)
v = sqlContext.createDataFrame(vertices, ["term", "id"])
e = sqlContext.createDataFrame(edges, ["src", "dst"])

# Show the vertices and edges
v.show()
e.show()

# Construct the graph with graphframes
g = GraphFrame(v, e)

# From @56 in piazza it could be good to change out tol=0001 with maxIter=30 to reduce computation time 
pr = g.pageRank(resetProbability=0.15, tol=0.0001)
pr.vertices.select("term", "pagerank").show()

