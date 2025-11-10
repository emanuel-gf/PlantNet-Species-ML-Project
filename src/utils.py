import pandas as pd 
import fastparquet 

def if_else_specie(x, specie=2474):
    if int(x)==2474:
        return 1
    else: 
        return 0
    
def merge_metadata_data(path_metadata, path_data, common_column="surveyId"):
    """
    Merge two dataframes combining the given column. It uses the Inner join.
    
    args:
        metadata:
            metadata file
        data:
            Climatic and all other variables
    returns:
        pd.DataFrame
    """
    df_metadata = pd.read_parquet(path_metadata,
                                  engine="fastparquet")
    df_data = pd.read_parquet(path_data,
                              engine="fastparquet")
    
    df = pd.concat([df_metadata.set_index(common_column), df_data.set_index(common_column)],
                   axis=1,
                   join='inner')
    df.reset_index(inplace=True)
    
    df['predict'] = df['speciesId'].apply(lambda x:if_else_specie(x))
    
    return df



def merge_metadata_data_TEST(path_metadata, path_data, common_column="surveyId"):
    """
    Merge the test dataframe. Two dataframes combining the given column. It uses the Inner join.
    
    args:
        metadata:
            metadata file
        data:
            Climatic and all other variables
    returns:
        pd.DataFrame
    """
    df_metadata = pd.read_parquet(path_metadata,
                                  engine="fastparquet")
    df_data = pd.read_parquet(path_data,
                              engine="fastparquet")
    
    df = pd.concat([df_metadata.set_index(common_column), df_data.set_index(common_column)],
                   axis=1,
                   join='inner')
    df.reset_index(inplace=True)
    
    df['species_list'] = df['predictions'].apply(lambda x:[int(i) for i in x.split()])
    
    
    specie = 2474

    def find_specie(x):
        if specie in x:
            return 1
        else:
            return 0
        
    df['predict'] = df['species_list'].apply(lambda x: find_specie(x))

    return df