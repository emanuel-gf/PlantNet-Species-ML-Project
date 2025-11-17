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
        
        

def train_test_predict(X_train, X_test, y_train, y_test, models,preprocessor):
    """
    Predict the model for the given Xtrain and Xtest.
    """
    import numpy as np
    import time
    ## scores dict
    scores = {
            'recall': [], 'precision': [], 'f1': [], 
            'accuracy': [], 'balanced_accuracy': [], 'roc_auc': [],
            'confusion_matrix': []
        }

    ## time
    start_time = time.time()

    # Create pipeline
    clf = Pipeline([
        ("preproc", preprocessor),
        (models[0][0], models[0][1])
    ])

    ##fit 
    print(f"Training the Model")
    clf.fit(X_train, y_train)
    print(f"Training time: {np.absolute(time.time()-start_time)}")
    
    ## dict to store
    dict_out = dict()
    ## predict 
    list_loop = [
        ('train', X_train),
        ('test', X_test)
        ]
    for name, df in list_loop:
        print(f"Predicting {name}")
    
        y_pred = clf.predict(df)

        # Evaluate
        y_score = None
        if hasattr(clf, "predict_proba"):
            try:
                y_score = clf.predict_proba(X_test)[:, 1]
            except Exception:
                y_score = None
        elif hasattr(clf, "decision_function"):
            try:
                y_score = clf.decision_function(X_test)
            except Exception:
                y_score = None

        # Metrics
        print(f"Calculating metrics...")
        scores['recall'].append(recall_score(y_test, y_pred, average='weighted'))
        scores['precision'].append(precision_score(y_test, y_pred, average='weighted'))
        scores['f1'].append(f1_score(y_test, y_pred, average='weighted'))
        scores['accuracy'].append(accuracy_score(y_test, y_pred))
        scores['balanced_accuracy'].append(balanced_accuracy_score(y_test, y_pred))  

        ## Calc ROC-AUC
        if y_score is not None and len(np.unique(y_pred)) > 1:
            try:
                roc = roc_auc_score(y_train, y_score)
                scores['roc_auc'].append(roc)
            except Exception:
                roc = None

        # ## CM
        cm = confusion_matrix(y_test, y_pred, labels=[0,1])

        scores['confusion_matrix'] = cm
        
        dict_out[name] = scores 
        
    time_taken =  time.time() - start_time
    print(f"Time taken: {time_taken:.2f}")
    
    return dict_out