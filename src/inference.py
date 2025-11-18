## function to evaluate with GroupKFold
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import GroupKFold
from sklearn.metrics import (accuracy_score, precision_score, recall_score, f1_score,
                             roc_auc_score, confusion_matrix, classification_report, balanced_accuracy_score)
from sklearn.pipeline import Pipeline
import warnings

warnings.filterwarnings("ignore")


def evaluate_with_group_kfold(estimator, X, y, groups, random_state=420, verbose=False, **GK_kwargs):
    """
    It estimate from the estimator object, for each fold belonging to each group, the inference of the ML model. 
    
    args:
        estimator: an sklearn estimator or Pipeline with 'fit' and 'predict' (and ideally predict_proba)
        X:pd.DataFrame.  Raw df, preprocessor should be inside pipeline to avoid leakage
        y: array-like (1D) or DataFrame with single column "predict" column 
        groups: pd.Series. Series extracted from the df which is the aggregating denominator.
        GK_kwargs: sklearn.model_selection.GroupKfold parameters.
    
    Returns:
        scores: dict. 
            Dictionary of metrics. 
    """
    ## (n,1) array shape
    y_arr = np.asarray(y).ravel()
    
    ## GroupKFold 
    kfold = GroupKFold(random_state=random_state,
                       n_splits=4,
                       shuffle=True,
                       **GK_kwargs)
    

    scores = {
        'recall': [], 'precision': [], 'f1': [], 
        'accuracy': [], 'balanced_accuracy': [], 'roc_auc': [],
        'confusion_matrix': []
    }

    for i, (train_idx, test_idx) in enumerate(kfold.split(X, y_arr, groups=groups)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y_arr[train_idx], y_arr[test_idx]

        # Fit
        estimator.fit(X_train, y_train)

        # Predict
        y_pred = estimator.predict(X_test)

        # Probabilities (for ROC AUC)
        y_score = None
        if hasattr(estimator, "predict_proba"):
            try:
                y_score = estimator.predict_proba(X_test)[:, 1]
            except Exception:
                y_score = None
        elif hasattr(estimator, "decision_function"):
            try:
                y_score = estimator.decision_function(X_test)
            except Exception:
                y_score = None

        # Metrics
        scores['recall'].append(recall_score(y_test, y_pred, average='weighted'))
        scores['precision'].append(precision_score(y_test, y_pred, average='weighted'))
        scores['f1'].append(f1_score(y_test, y_pred, average='weighted'))
        scores['accuracy'].append(accuracy_score(y_test, y_pred))
        scores['balanced_accuracy'].append(balanced_accuracy_score(y_test, y_pred))

        
        ## Calc ROC-AUC
        if y_score is not None and len(np.unique(y_test)) > 1:
            try:
                roc = roc_auc_score(y_test, y_score)
                scores['roc_auc'].append(roc)
            except Exception:
                roc = None

        ## CM
        cm = confusion_matrix(y_test, y_pred, labels=[0,1])
        scores['confusion_matrix'].append(cm)

        if verbose:
            print("-"*30)
            print(f"Folder: {i}")
            print(classification_report(y_test, y_pred, zero_division=0))


    return scores

def describe_kfold_results(df, cols=None):
    if cols != None:
        df = df[cols]
        
    print(f"Global Results:")
    for col in df.select_dtypes(include=np.number):
        print(f"{col}: {df[col].mean():.3f} +- {df[col].std():.3f}")
   
        
def train_test_predict(X_train, X_test, y_train, y_test, models, preprocessor):
    """
    Predict the model for the given Xtrain and Xtest and return scores.
    """
    import numpy as np
    import time
    from sklearn.metrics import (
        recall_score, precision_score, f1_score, accuracy_score,
        balanced_accuracy_score, roc_auc_score, confusion_matrix
    )
    from sklearn.pipeline import Pipeline
    
    # Convert to 1D array if needed
    if hasattr(y_train, 'values'): 
        y_train_array = y_train.values.ravel()
        y_test_array = y_test.values.ravel()
    else:
        y_train_array = np.asarray(y_train).ravel()
        y_test_array = np.asarray(y_test).ravel()
    
    print(f"Train class distribution: {np.bincount(y_train_array)}")
    print(f"Test class distribution: {np.bincount(y_test_array)}")
    
    start_time = time.time()

    # Create pipeline
    clf = Pipeline([
        ("preproc", preprocessor),
        (models[0][0], models[0][1])
    ])

    ##fit 
    print(f"Training the Model")
    clf.fit(X_train, y_train)
    print(f"Training time: {np.absolute(time.time()-start_time):.2f}s")
        
    ## predict 
    list_loop = [
        ('train', X_train, y_train_array), # Use the 1D numpy array target
        ('test', X_test, y_test_array)     # Use the 1D numpy array target
    ]
    
    dict_out = {}

    for name, df, y_vec in list_loop:
        print(f"Predicting {name}")

        # Create a fresh scores dict for the current split (Train or Test)
        # This is where the error was corrected—it now correctly captures 
        # the single run's scores into a new dictionary.
        current_scores = {
            'recall': [], 'precision': [], 'f1': [], 
            'accuracy': [], 'balanced_accuracy': [], 'roc_auc': [],
            'confusion_matrix': None
        }

        y_pred = clf.predict(df)

        # Evaluate
        y_score = None
        n_classes = len(np.unique(y_vec))
        
        if hasattr(clf, "predict_proba"):
            try:
                # For binary classification, predict_proba[:, 1] is typically used
                if n_classes == 2:
                    y_score = clf.predict_proba(df)[:, 1]
                # For multiclass, we need the full probability matrix
                else:
                    y_score = clf.predict_proba(df)
            except Exception:
                pass

        # Metrics: Append to the current_scores lists
        current_scores['recall'].append(recall_score(y_vec, y_pred, average='weighted', zero_division=0))
        current_scores['precision'].append(precision_score(y_vec, y_pred, average='weighted', zero_division=0))
        current_scores['f1'].append(f1_score(y_vec, y_pred, average='weighted', zero_division=0))
        current_scores['accuracy'].append(accuracy_score(y_vec, y_pred))
        current_scores['balanced_accuracy'].append(balanced_accuracy_score(y_vec, y_pred))

        if y_score is not None and len(np.unique(y_pred)) > 1:
            try:
                if n_classes == 2:
                    current_scores['roc_auc'].append(roc_auc_score(y_vec, y_score))
                else:
                     current_scores['roc_auc'].append(roc_auc_score(y_vec, y_score, multi_class='ovr'))
            except Exception as e:
                print(f"Warning: Could not calculate ROC AUC for {name}. Error: {e}")
                pass

        # Use the original labels list [0, 1] for binary
        current_scores['confusion_matrix'] = confusion_matrix(y_vec, y_pred, labels=[0, 1])
        
        # Store the current results under its split name
        dict_out[name] = current_scores 
        
    time_taken = time.time() - start_time
    print(f"Total time taken: {time_taken:.2f}s")
    
    return dict_out