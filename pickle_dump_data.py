import pickle


with open('./model/best_model/auc_lst.pkl', 'wb') as file:
    pickle.dump({'total_batch_num_lst': total_batch_num_lst,
                 'auc_score_lst': auc_score_lst}, file)


with open('./model/best_model/auc_lst.pkl', 'rb') as file:
    obb = pickle.load(file)
    print('pickle load')
    print(obb['total_batch_num_lst'])

    