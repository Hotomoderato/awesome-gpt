from sklearn.metrics import precision_recall_curve, roc_curve
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.metrics import accuracy_score, f1_score
from sklearn.metrics import precision_score, recall_score
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from gensim.models import KeyedVectors
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def get_seqlen(texts, pct=None):
    """
    check sequence/texts length
    """
    # convert to idx and padding
    length = [len(l.split('|')) for l in texts]
    for p in [75, 80, 90, 95, 98]:
        l = np.percentile(length, p)
        print(f'length {p}% pct = {l}')
    if pct:
        return int(np.percentile(length, pct))


def seq2idx(texts, max_words, max_length):
    """
    Convert Sequence to Index
    """
    # tokenize sequence
    token = Tokenizer(num_words=max_words, 
                      char_level=False,
                      filters='',
                      split='|')
    token.fit_on_texts(texts)
    print("total number of unique words:", len(token.word_index))

    # sequence to index and padding
    seq_idx = token.texts_to_sequences(texts)
    seq_pad = pad_sequences(seq_idx,
                            maxlen=max_length,
                            padding='post',
                            truncating='pre')
    
    # convert to dataframe
    cols = ['seq_' + str(f) for f in range(max_length)]
    seq_pad_df = pd.DataFrame(seq_pad, columns=cols)
    return seq_pad_df, token


def plot_pr(y_test, y_proba, save_dir=None):
    # plot pr curve
    plt.figure(1)
    precision, recall, thresholds = precision_recall_curve(y_test, y_proba)
    pr_auc = average_precision_score(y_test, y_proba, average="micro")
    plt.plot(recall, precision, label = 'PR_AUC: {:.4f}'.format(pr_auc))
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.ylim([0.0, 1.05])
    plt.xlim([0.0, 1.0])
    plt.title('Precision-Recall Curve')
    plt.legend(loc="upper right")
    if save_dir:
        plt.savefig(save_dir)
    plt.show()
    return


def plot_roc(y_test, y_proba, save_dir=None):
    plt.figure(2)
    plt.plot([0, 1], [0, 1], 'k--')
    ras = roc_auc_score(y_test, y_proba)
    fpr, tpr, _ = roc_curve(y_test, y_proba)
    plt.plot(fpr, tpr, label= 'ROC_AUC: {:.4f}'.format(ras))
    plt.xlabel('False positive rate')
    plt.ylabel('True positive rate')
    plt.ylim([0.0, 1.05])
    plt.xlim([0.0, 1.0])
    plt.title('ROC Curves')
    plt.legend(loc="lower right")
    if save_dir:
        plt.savefig(save_dir)
    plt.show()
    return


def plot_loss(history, save_path=None):
    """
    Model train/val loss history
    """
    plt.figure(3)
    plt.plot(history.history['loss'], linewidth=2, label='Train')
    plt.plot(history.history['val_loss'], linewidth=2, label='Valid')
    plt.legend(loc='upper right')
    plt.title('Model loss')
    plt.ylabel('Loss')
    plt.xlabel('Epoch')
    if save_path:
        plt.savefig(save_path)
    plt.show()


def plot_multi_pr(name_list, Y_proba, Y_test, save_dir=None):
    """
    plot precision-recall curve
    """
    plt.figure(1)
    for i, item in enumerate(name_list):
        y_score = Y_proba[i]
        y_test = Y_test[i]
        precision, recall, thresholds = precision_recall_curve(y_test, y_score)
        pr_auc = average_precision_score(y_test, y_score, average="micro")
        plt.plot(recall, precision, label=item + ' PR_AUC:{:4f}'.format(pr_auc), linewidth=0.5)
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.ylim([0.0, 1.05])
    plt.xlim([0.0, 1.0])
    plt.title('Precision-Recall Curve')
    plt.legend(loc="upper right")
    if save_dir:
        plt.savefig(save_dir)
    plt.show()


def plot_multi_roc(name_list, Y_proba, Y_test, save_dir=None):
    """
    plot ROC curve
    """
    plt.figure(2)
    plt.plot([0, 1], [0, 1], 'k--')  # draw a dot line
    for i, item in enumerate(name_list):
        y_score = Y_proba[i]
        y_test = Y_test[i]
        ras = roc_auc_score(y_test, y_score)
        fpr, tpr, _ = roc_curve(y_test, y_score)
        plt.plot(fpr, tpr, label=item + ' ROC_AUC:{:4f}'.format(ras), linewidth=0.5)
    plt.xlabel('False positive rate')
    plt.ylabel('True positive rate')
    plt.ylim([0.0, 1.05])
    plt.xlim([0.0, 1.0])
    plt.title('ROC Curves' )
    plt.legend(loc="lower right")
    if save_dir:
        plt.savefig(save_dir)
    plt.show()


def precision_at_k(y_test, y_pred_prob, cutoff):
    """evaluate model - precision at k"""
    result = []
    for i in cutoff:
        result.append(evaluate(y_test, y_pred_prob, i))
    result = np.array(result)
    return result


def evaluate(y_true, y_pred_prob, size):  
    """Evaluate testing performance for given targeting size.
    
    Parameters
    ----------
    y_true: array, shape = [n_samples]
      True values of y.
      
    y_pred_prob: array, shape = [n_samples]
      Predicted scores/probability of y.
      
    size: int
      Targeting patients size
      
    Returns
    -------
    Precision and recall scores
    """
    indices = np.argsort(y_pred_prob)[::-1]
    threshold = y_pred_prob[indices][size-1]
    y_pred = np.where(y_pred_prob>=threshold, 1 ,0)
    return [precision_score(y_true, y_pred), recall_score(y_true, y_pred), threshold]


def load_embed(embed_path):
    def get_coefs(word,*arr): return word, np.asarray(arr, dtype='float32')
    embeddings_index = dict(get_coefs(*o.strip().split(" ")) for o in open(embed_path))
    print("Done. {} words loaded!".format(len(embeddings_index)))

    return embeddings_index


def build_embed_matrix(word_index, max_features, embed_path):
    embeddings_index = load_embed(embed_path)
    embedding_matrix = np.zeros((max_features, 300))
    for word, i in word_index.items():
        if i >= max_features: continue
        try:
            embedding_vector = embeddings_index.get(str(word))
        except:
            embedding_vector = embeddings_index["unknown"]
        if embedding_vector is not None:
            embedding_matrix[i] = embedding_vector
            
    return embedding_matrix