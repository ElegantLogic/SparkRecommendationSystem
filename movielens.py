import sys
from time import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS
import threading
import io

try:
    from Tkinter import *
    import Tkinter as tk
except ImportError:
    from tkinter import *
    import tkinter as tk
try:
    import ttk
    py3 = False
except ImportError:
    import tkinter.ttk as ttk
    py3 = True
    
class AutocompleteCombobox(ttk.Combobox):

        def set_completion_list(self, completion_list):
                """Use our completion list as our drop down selection menu, arrows move through menu."""
                self._completion_list = sorted(completion_list, key=str.lower) # Work with a sorted list
                self._hits = []
                self._hit_index = 0
                self.position = 0
                self.bind('<KeyRelease>', self.handle_keyrelease)
                self['values'] = self._completion_list  # Setup our popup menu

        def autocomplete(self, delta=0):
                """autocomplete the Combobox, delta may be 0/1/-1 to cycle through possible hits"""
                if delta: # need to delete selection otherwise we would fix the current position
                        self.delete(self.position, tk.END)
                else: # set position to end so selection starts where textentry ended
                        self.position = len(self.get())
                # collect hits
                _hits = []
                for element in self._completion_list:
                        if element.lower().startswith(self.get().lower()): # Match case insensitively
                                _hits.append(element)
                # if we have a new hit list, keep this in mind
                if _hits != self._hits:
                        self._hit_index = 0
                        self._hits=_hits
                # only allow cycling if we are in a known hit list
                if _hits == self._hits and self._hits:
                        self._hit_index = (self._hit_index + delta) % len(self._hits)
                # now finally perform the auto completion
                if self._hits:
                        self.delete(0,tk.END)
                        self.insert(0,self._hits[self._hit_index])
                        self.select_range(self.position,tk.END)

        def handle_keyrelease(self, event):
                """event handler for the keyrelease event on this widget"""
                if event.keysym == "BackSpace":
                        self.delete(self.index(tk.INSERT), tk.END)
                        self.position = self.index(tk.END)
                if event.keysym == "Left":
                        if self.position < self.index(tk.END): # delete the selection
                                self.delete(self.position, tk.END)
                        else:
                                self.position = self.position-1 # delete one character
                                self.delete(self.position, tk.END)
                if event.keysym == "Right":
                        self.position = self.index(tk.END) # go to end (no selection)
                if len(event.keysym) == 1:
                        self.autocomplete()


def startGUI():
    global val, w, root, top, newUserRatings, mDict
    newUserRatings=[]
    m = open('/home/node/datasets/ml-latest/movies.csv').readlines()
    #m = open('/home/node/Desktop/test/electronics.csv').readlines()
    mDict = {name:idx for name,idx in [(i[1],i[0]) for i in [j.split(',') for j in m]]}
    root = Tk()
    top = Spark_Recommendation_System(root)
    root.mainloop()
    
def submitRating():
    try:
        rating = float(top.txtRating.get('1.0','end-1c'))
        if (top.combo.get() != '' and rating > 0 and rating < 6):
            movieId = int(mDict[top.combo.get()])
            newUserRatings.append((0,movieId,rating))
            top.txtRating.delete('1.0','end')
    except:
         pass

def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

def recommend():
    spark = SparkSession.builder.config("spark.app.name","Movies Recommendation System 8m").getOrCreate()
    #    .config("spark.executor.memory","2500m")\
    #    .config("spark.executor.cores","3")\
    #    .config("spark.driver.memory","1024m")\
    #    .config("spark.submit.deployMode","client")\
    #    .config("spark.driver.cores","1")\
    sc = spark.sparkContext

    # Load the complete dataset file

    #complete_ratings_raw_data = sc.textFile("file:///home/node/Desktop/test/elec_ratings.csv")
    #complete_ratings_raw_data = sc.textFile("hdfs:///datasets/elec_ratings.csv")
    complete_ratings_raw_data = sc.textFile("hdfs:///datasets/ratings8.csv")
    #complete_ratings_raw_data = sc.textFile("file:///home/node/datasets/ml-latest-small/ratings.csv")
    #complete_ratings_raw_data = sc.textFile("hdfs:///datasets/ml-latest/ratings.csv")

# Parse
    complete_ratings_data = complete_ratings_raw_data.map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
    
    print "There are %s ratings in the complete dataset" % (complete_ratings_data.count())

    seed = 5L
    iterations = 10
    regularization_parameter = 0.1
    rank = 8 #[4, 8, 12]
    errors = [0, 0, 0]
    err = 0
    tolerance = 0.02

    #complete_movies_raw_data = sc.textFile("file:///home/node/Desktop/test/electronics.csv")
    #complete_movies_raw_data = sc.textFile("hdfs:///datasets/electronics.csv")
    complete_movies_raw_data = sc.textFile("hdfs:///datasets/movies8.csv")
    #complete_movies_raw_data = sc.textFile("file:///home/node/datasets/ml-latest-small/movies.csv")
    #complete_movies_raw_data = sc.textFile("hdfs:///datasets/ml-latest/movies.csv")

# Parse
    complete_movies_data = complete_movies_raw_data.map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1])).cache()#,tokens[2]

    complete_movies_titles = complete_movies_data.map(lambda x: (int(x[0]),x[1]))
    
    print "There are %s electronic devices or movies in the complete dataset" % (complete_movies_titles.count())


    movie_ID_with_ratings_RDD = (complete_ratings_data.map(lambda x: (x[1], x[2])).groupByKey())
    movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
    movie_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

    new_user_ID = 0

    new_user_ratings_RDD = sc.parallelize(newUserRatings)
    print 'New user ratings: %s' % new_user_ratings_RDD.take(10)


    complete_data_with_new_ratings_RDD = complete_ratings_data.union(new_user_ratings_RDD)
  
    t0 = time()
    new_ratings_model = ALS.train(complete_data_with_new_ratings_RDD, rank, seed=seed, 
                              iterations=iterations, lambda_=regularization_parameter)
    tt = time() - t0

    print "New model trained in %s seconds" % round(tt,3)

    new_user_ratings_ids = map(lambda x: x[1], newUserRatings) # get just movie IDs
# keep just those not on the ID list (thanks Lei Li for spotting the error!)
    new_user_unrated_movies_RDD = (complete_movies_data.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (new_user_ID, x[0])))

# Use the input RDD, new_user_unrated_movies_RDD, with new_ratings_model.predictAll() to predict new ratings for the movies
    new_user_recommendations_RDD = new_ratings_model.predictAll(new_user_unrated_movies_RDD)


# Transform new_user_recommendations_RDD into pairs of the form (Movie ID, Predicted Rating)
    new_user_recommendations_rating_RDD = new_user_recommendations_RDD.map(lambda x: (x.product, x.rating))
    new_user_recommendations_rating_title_and_count_RDD =     new_user_recommendations_rating_RDD.join(complete_movies_titles).join(movie_rating_counts_RDD)
    new_user_recommendations_rating_title_and_count_RDD.take(3)


    new_user_recommendations_rating_title_and_count_RDD =     new_user_recommendations_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))



    top_movies = new_user_recommendations_rating_title_and_count_RDD.filter(lambda r: r[2]>=25).takeOrdered(5, key=lambda x: -x[1])
    movieTitles = [i[0] for i in top_movies]

    #print ('TOP recommended movies (with more than 25 reviews):\n%s' %
     #       '\n'.join(map(str, top_movies)))



   # recIds = [i[1] for i in r]
    #invDict = dict(zip(mDict.values(),mDict.keys()))
    #recNames = [invDict[str(i)] for i in recIds]
    
    top.lblRecs['text'] = 'Recommended For You :\n\n* '
    top.lblRecs['text'] = top.lblRecs['text'] + '\n* '.join(map(str, movieTitles))
    
    #DELETE NEWRATINGS

class Spark_Recommendation_System: 
    def __init__(self, top=None):
        _bgcolor = '#d9d9d9'  # X11 color: 'gray85'
        _fgcolor = '#000000'  # X11 color: 'black'
        _compcolor = '#d9d9d9' # X11 color: 'gray85'
        _ana1color = '#d9d9d9' # X11 color: 'gray85' 
        _ana2color = '#d9d9d9' # X11 color: 'gray85' 
        font12 = "-family Times -size 24 -weight normal -slant roman "  \
            "-underline 0 -overstrike 0"
        font9 = "-family Times -size 16 -weight normal -slant"  \
            " roman -underline 0 -overstrike 0"
        font13 = "-family Times -size 20 -weight normal -slant roman "  \
            "-underline 0 -overstrike 0"
        self.style = ttk.Style()
        if sys.platform == "win32":
            self.style.theme_use('winnative')
        self.style.configure('.',background=_bgcolor)
        self.style.configure('.',foreground=_fgcolor)
        self.style.configure('.',font="TkDefaultFont")
        self.style.map('.',background=
            [('selected', _compcolor), ('active',_ana2color)])

        top.geometry("864x663+243+28")
        top.title("Spark Recommendation System")
        top.configure(background="#d9d9d9")
        top.configure(highlightbackground="#d9d9d9")
        top.configure(highlightcolor="black")



        self.combo = AutocompleteCombobox(top)
        self.combo.place(relx=0.12, rely=0.11, relheight=0.06, relwidth=0.86)
        self.combo.configure(font=font9)
        self.combo.configure(takefocus="")

        self.btnSubmit = Button(top)
        self.btnSubmit.place(relx=0.45, rely=0.18, height=41, width=221)
        self.btnSubmit.configure(activebackground="#d9d9d9")
        self.btnSubmit.configure(activeforeground="#000000")
        self.btnSubmit.configure(background="#d9d9d9")
        self.btnSubmit.configure(disabledforeground="#a3a3a3")
        self.btnSubmit.configure(font=font9)
        self.btnSubmit.configure(foreground="#000000")
        self.btnSubmit.configure(highlightbackground="#d9d9d9")
        self.btnSubmit.configure(highlightcolor="black")
        self.btnSubmit.configure(pady="0")
        self.btnSubmit.configure(text='''Submit Rating''')

        self.txtRating = Text(top)
        self.txtRating.place(relx=0.12, rely=0.18, relheight=0.05, relwidth=0.06)

        self.txtRating.configure(background="white")
        self.txtRating.configure(font=font13)
        self.txtRating.configure(foreground="black")
        self.txtRating.configure(highlightbackground="#d9d9d9")
        self.txtRating.configure(highlightcolor="black")
        self.txtRating.configure(insertbackground="black")
        self.txtRating.configure(selectbackground="#c4c4c4")
        self.txtRating.configure(selectforeground="black")
        self.txtRating.configure(width=54)
        self.txtRating.configure(wrap=WORD)

        self.Label1 = Label(top)
        self.Label1.place(relx=0.01, rely=0.11, height=33, width=91)
        self.Label1.configure(activebackground="#f9f9f9")
        self.Label1.configure(activeforeground="black")
        self.Label1.configure(anchor=NW)
        self.Label1.configure(background="#d9d9d9")
        self.Label1.configure(disabledforeground="#a3a3a3")
        self.Label1.configure(font=font9)
        self.Label1.configure(foreground="#000000")
        self.Label1.configure(highlightbackground="#d9d9d9")
        self.Label1.configure(highlightcolor="black")
        self.Label1.configure(text='''Item    :''')

        self.Label1 = Label(top)
        self.Label1.place(relx=0.01, rely=0.18, height=33, width=91)
        self.Label1.configure(activebackground="#f9f9f9")
        self.Label1.configure(activeforeground="black")
        self.Label1.configure(anchor=NW)
        self.Label1.configure(background="#d9d9d9")
        self.Label1.configure(disabledforeground="#a3a3a3")
        self.Label1.configure(font=font9)
        self.Label1.configure(foreground="#000000")
        self.Label1.configure(highlightbackground="#d9d9d9")
        self.Label1.configure(highlightcolor="black")
        self.Label1.configure(text='''Rating :''')

        self.btnRcmnd = Button(top)
        self.btnRcmnd.place(relx=0.72, rely=0.18, height=41, width=221)
        self.btnRcmnd.configure(activebackground="#d9d9d9")
        self.btnRcmnd.configure(activeforeground="#000000")
        self.btnRcmnd.configure(background="#d9d9d9")
        self.btnRcmnd.configure(disabledforeground="#a3a3a3")
        self.btnRcmnd.configure(font=font9)
        self.btnRcmnd.configure(foreground="#000000")
        self.btnRcmnd.configure(highlightbackground="#d9d9d9")
        self.btnRcmnd.configure(highlightcolor="black")
        self.btnRcmnd.configure(pady="0")
        self.btnRcmnd.configure(text='''Get Recommendations''')

        self.Label2 = Label(top)
        self.Label2.place(relx=0.01, rely=0.02, height=49, width=468)
        self.Label2.configure(activebackground="#f9f9f9")
        self.Label2.configure(activeforeground="black")
        self.Label2.configure(anchor=NW)
        self.Label2.configure(background="#d9d9d9")
        self.Label2.configure(disabledforeground="#a3a3a3")
        self.Label2.configure(font=font12)
        self.Label2.configure(foreground="#000000")
        self.Label2.configure(highlightbackground="#d9d9d9")
        self.Label2.configure(highlightcolor="black")
        self.Label2.configure(text='''Spark Recommendation System''')

        self.lblRecs = Label(top)
        self.lblRecs.place(relx=0.01, rely=0.27, height=469, width=831)
        self.lblRecs.configure(activebackground="#f9f9f9")
        self.lblRecs.configure(activeforeground="black")
        self.lblRecs.configure(anchor=NW)
        self.lblRecs.configure(justify='left')
        self.lblRecs.configure(background="#d9d9d9")
        self.lblRecs.configure(disabledforeground="#a3a3a3")
        self.lblRecs.configure(font=font13)
        self.lblRecs.configure(foreground="#000000")
        self.lblRecs.configure(highlightbackground="#d9d9d9")
        self.lblRecs.configure(highlightcolor="black")
        self.lblRecs.configure(width=831)
        self.lblRecs.configure(wraplength=831)
        
        self.btnRcmnd.configure(command=recommend)
        self.btnSubmit.configure(command=submitRating)
        
        f = open('/home/node/datasets/ml-latest/movies.csv')#,encoding='utf-8')
	#f = open('/home/node/Desktop/test/electronics.csv')#,encoding='utf-8')
        lines = f.readlines()
        items = [i[1] for i in [j.split(',') for j in lines]]
        self.combo.set_completion_list(items)

if __name__ == '__main__':
    startGUI()


