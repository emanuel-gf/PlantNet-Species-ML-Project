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



def get_feature_names(preprocessor, numeric_features, categorical_features):
    """Get feature names from ColumnTransformer
    It includes the new generated columns by the OneHotEncoder.
    
    args:
        preprocessor: Pipeline: scikit.pipeline.Pipeline
        numeric_features: List
            List of numeric features
        categorical_features: List
            List of categorical features
    """
    
    # Numeric features keep their original names
    num_features = numeric_features
    
    # Get categorical feature names after one-hot encoding
    cat_encoder = preprocessor.named_transformers_['cat'].named_steps['encoder']
    cat_features = cat_encoder.get_feature_names_out(categorical_features)
    
    # Combine all feature names
    all_features = list(num_features) + list(cat_features)
    
    return all_features