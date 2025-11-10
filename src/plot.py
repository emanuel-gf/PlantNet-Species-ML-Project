import seaborn as sns
import matplotlib.pyplot as plt 


def plot_cm(cm, figsize=(5,4)):
    """"
    args:
        cm: np.ndarray. 
            The confusion matrix computed from sklearn.
    """
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=figsize)
    sns.heatmap(total_cm, annot=True, fmt='d', cmap='Blues', xticklabels=[0,1], yticklabels=[0,1])
    axes.set_xlabel('Predicted'); plt.ylabel('True'); plt.title('Aggregated Confusion Matrix (all folds)')
    fig.show()