import pandas as pd
def read_imdb_datasets():
    """This function imports the following datasets and merge them
    1. imdb.title.basics
    2. imdb.title.principals
    3. imdb.title.crew
    4. imdb.title.ratings
    It also create new features for imdb.title.basics in terms of genre
    """
    #read imdb.title.bascis
    imdb_title = pd.read_csv('data/imdb.title.basics.csv.gz')
    
    #Lets make title unique, later we need it when merging different datasets
    imdb_title['original_title'] = imdb_title['original_title'].str.split(':').str[0].str.replace(r"\(.*\)","")\
    .str.replace('\d+', '').str.rstrip().replace('Part', '').str.rstrip()
    
    imdb_title['primary_title'] = imdb_title['primary_title'].str.split(':').str[0].str.replace(r"\(.*\)","")\
    .str.replace('\d+', '').str.rstrip().replace('Part', '').str.rstrip()
    
    # adding two more columns that add year at the end of title
    imdb_title['original_title_year'] = imdb_title['original_title'] + imdb_title['start_year'].map(lambda x: str(x))
    imdb_title['primary_title_year'] = imdb_title['primary_title'] + imdb_title['start_year'].map(lambda x: str(x))
    
    #Lets drop duplicated datapoints
    imdb_title.drop_duplicates(subset='original_title_year', keep = 'first', inplace = True)
    imdb_title.drop_duplicates(subset='primary_title_year', keep = 'first', inplace = True)
    
    ## Lets add genre of each movie as column
    # Convert to string because includes float in some cases. I do not want to get ride of those na values at this step. 
    # Lets face them later
    imdb_title.genres = imdb_title.genres.map(lambda x: str(x))
    all_geners_type = list(set([item for list_1 in imdb_title.genres.str.split(',') for item in list_1]))
    imdb_title['first_genre'] = imdb_title.genres.map(lambda x: x.split(',')[0])
    
    all_geners_type.remove('nan')
    
    # Add columns showing the genre
    for gener in all_geners_type:
        imdb_title[gener] = imdb_title.genres.map(lambda x: 1 if gener in x else 0)
        
    ###Raed imdb rating file
    imdb_rating = pd.read_csv('data/imdb.title.ratings.csv.gz')

    ###Raed imdb crew file
    imdb_crew = pd.read_csv('data/imdb.title.crew.csv.gz')
    
    ###Raed imdb crew file
    imdb_principals = pd.read_csv('data/imdb.title.principals.csv.gz')
    
    
    #lets initially merge principals_imdb and crew_imdb
    crew_principals_imdb = pd.merge(imdb_principals, imdb_crew, on = 'tconst', how = 'inner')
    
    #merge imdb_title and imdb_rating
    imdb_title_rating = pd.merge(imdb_title, imdb_rating, how = 'inner', on = 'tconst')
    
    #lets multiply each gener by rating. This will be useful for plotting later
    #if each genre column has rating it means that movie is categorized as that genre
    #zero means the movie does not belong to that genre
    for gener in all_geners_type:
        imdb_title_rating[gener] *= imdb_title_rating['averagerating']
        
    #Compute the number of genre in each movie
    imdb_title_rating['no_genres'] = imdb_title_rating['genres'].map(lambda x: len(x.split(',')))
        
    #now we are merging all dataframe
    imdb_df = pd.merge(crew_principals_imdb, imdb_title_rating, on = 'tconst')
    
    return imdb_df, all_geners_type

def make_long_df_imdb(input_df, id_vars, value_vars):
    """
    This function removes duplicated observations in imdb dataframe 
    and makes a long format data frame based on genre.
    """
    df_for_long = input_df.drop_duplicates(subset = 'tconst', keep = 'first')
    
    long_df = pd.melt(df_for_long, id_vars= id_vars, 
                  value_vars = value_vars)
    #Remove zero values. 
    # This is required for long list and does not have any effect on results.
    long_df = long_df.loc[long_df['value'] != 0] 
    
    #lets to remove 'Talk-Show' and 'Short' variables from the data to make the plot better. 
    #There are few data points for these features
    long_df = long_df.loc[(long_df['variable'] != 'Talk-Show') & (long_df['variable'] != 'Short')]
    
    return long_df

def map_nconst_names(input_df):
    """
    This function maps nconst to real name using the following dataframe
    """
    imdb_name_basics = pd.read_csv('data/imdb.name.basics.csv.gz')
    
    #Convert it to a dictionary
    nconst_to_name = dict(zip(imdb_name_basics['nconst'], imdb_name_basics['primary_name']))
    
    #Add a column of called name
    input_df['principal_name'] = input_df['nconst'].map(lambda x: nconst_to_name.get(x))
    
    return input_df

def read_budgets():
    """
    This file reads tn.movie_budgets.csv and bom.movie_gross.csv.
    After, preprocessing them, it merge both of them.
    """
    tn_movie_budgets = pd.read_csv('data/tn.movie_budgets.csv.gz')
    
    #In some cases, we can see that there are four numbers with paranthesis at the end of the title. Lets remove them.
    tn_movie_budgets['movie'] = tn_movie_budgets['movie'].str.split(':').str[0].str.replace(r"\(.*\)","")\
    .str.replace('\d+', '').str.rstrip().replace('Part', '').str.rstrip()
    
    #make the title unique by adding the year at the end of it
    tn_movie_budgets['movie_year'] = tn_movie_budgets['movie'] +\
    tn_movie_budgets['release_date'].map(lambda x: x[-4:])
    
    # Dealing with duplicates or simply remove them. There are  just a few duplicates after the above process
    tn_movie_budgets = tn_movie_budgets[~tn_movie_budgets['movie_year'].duplicated()]
    
    ##################Next dataset
    
    movie_gross = pd.read_csv('data/bom.movie_gross.csv.gz')
    
    #In some cases, we can see that there are four numbers with paranthesis at the end of the title. Lets remove them.
    movie_gross['title'] = movie_gross['title'].str.split(':').str[0].str.replace(r"\(.*\)","")\
    .str.replace('\d+', '').str.rstrip().replace('Part', '').str.rstrip()
    
    #Now lets make a new column including the title and the year to deal with the duplicated titles.
    movie_gross['title_year'] = movie_gross['title'] + movie_gross['year'].map(lambda x: str(x))
    movie_gross['title_year'].head()
    
    #We still might have some duplicated datapoints. But at this step, we remove them.
    movie_gross = movie_gross.loc[~movie_gross['title_year'].duplicated()]
    
    
    #merge datasets
    #merege databases 
    revenue_df = pd.merge(tn_movie_budgets, movie_gross, how = 'outer', 
                      left_on = 'movie_year', right_on = 'title_year')
    
    return revenue_df
