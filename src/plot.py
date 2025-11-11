import seaborn as sns
import matplotlib.pyplot as plt 


def plot_cm(total_cm, figsize=(5,4)):
    """
    args:
        total_cm: np.ndarray. 
            The confusion matrix computed from sklearn.
    """
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=figsize)
    sns.heatmap(total_cm, annot=True, fmt='d', cmap='Blues', 
                xticklabels=[0,1], yticklabels=[0,1], ax=axes)
    axes.set_xlabel('Predicted')
    axes.set_ylabel('True')
    axes.set_title('Aggregated Confusion Matrix (all folds)')
    plt.tight_layout()
    plt.show() 
    plt.close(fig)  ## to free memory