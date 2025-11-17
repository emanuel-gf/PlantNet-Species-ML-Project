import pandas as pd 
import fastparquet 
import os
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



def export_cv_clfs_metrics(dict_metrics:dict, export=True, file_name='file', default_folder=None):
    """
    Create a Multi-Index DataFrame 
    
    Args:
        dict_metrics: dict.
            Where keys are a string names representing the classifiers and values are the pd.df of metrics for the respective classifier. 
        export: bool.
            Export the multi-index as a .csv
        file_name: str.
            Name of the file (with or without .csv extension)
        default_folder: str.
            Folder path to save the file
        
    Returns:
        pd.DataFrame: Combined long-format DataFrame
    """
    if default_folder is None:
        default_folder = "tmp_csv/"
    
    # Create folder if it doesn't exist
    os.makedirs(default_folder, exist_ok=True)
    
    # Check and add .csv extension if not present
    if not file_name.endswith('.csv'):
        file_name = file_name + ".csv"
    
    out_path = os.path.join(default_folder, file_name)
    
    long_data = []
    for model_name, df in dict_metrics.items():
        # Add fold index
        if 'fold' not in df.columns: 
            df['fold'] = range(1, len(df) + 1)
        
        ## reshape confusion matrix
        # Melt to long format
        df_long = df.melt(id_vars=['fold'], var_name='metric', value_name='value')
        # Add model name
        df_long['model'] = model_name
        
        ##reshape confusion matrix
        long_data.append(df_long)

    # Combine all models
    results_long = pd.concat(long_data, ignore_index=True)

    # Reorder columns for clarity
    results_long = results_long[['model', 'metric', 'fold', 'value']]

    
    # Export if requested
    if export:
        # Save to CSV
        results_long.to_csv(out_path, index=False)
        print(f"Metrics exported to: {out_path}")
    
    return results_long


def results_to_dataframe(results_dict):
    """
    Convert nested results dictionary to a pandas DataFrame.
    
    Parameters:
    -----------
    results_dict : dict
        Dictionary with structure: {dataset: {split: {metric: [value]}}}
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: dataset, split, and all metrics
    """
    import pandas as pd
    
    rows = []
    
    for dataset_name, dataset_results in results_dict.items():
        for split_name, metrics in dataset_results.items():
            row = {
                'dataset': dataset_name,
                'split': split_name
            }
            
            # Add each metric (extract single value from list, except confusion_matrix)
            for metric_name, metric_value in metrics.items():
                if metric_name == 'confusion_matrix':
                    # Flatten confusion matrix into separate columns
                    cm = metric_value
                    row['cm_tn'] = cm[0, 0]
                    row['cm_fp'] = cm[0, 1]
                    row['cm_fn'] = cm[1, 0]
                    row['cm_tp'] = cm[1, 1]
                else:
                    # Extract single value from list
                    try:
                        row[metric_name] = metric_value[0] if isinstance(metric_value, list) else metric_value
                    except Exception:
                        row[metric_name] = 0.0 
            rows.append(row)
    
    df = pd.DataFrame(rows)
    
    # Reorder columns for better readability
    col_order = ['dataset', 'split', 'accuracy', 'balanced_accuracy', 'precision', 
                 'recall', 'f1', 'roc_auc', 'cm_tn', 'cm_fp', 'cm_fn', 'cm_tp']
    
    # Only keep columns that exist
    col_order = [col for col in col_order if col in df.columns]
    
    return df[col_order]