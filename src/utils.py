import pandas as pd 
import fastparquet 

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
    return df



