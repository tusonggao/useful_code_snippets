import os


def get_all_files(dir_name):   # 递归得到文件夹下的所有文件
    all_files_lst = []
    def get_all_files_worker(path):
        allfilelist = os.listdir(path)
        for file in allfilelist:
            filepath = os.path.join(path, file)
            #判断是不是文件夹
            if os.path.isdir(filepath):
                get_all_files_worker(filepath)
            else:
                all_files_lst.append(filepath)
    get_all_files_worker(dir_name)
    return all_files_lst

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 得到当前文件的的父目录

dst_file_name = os.path.split(file_name_with_directory)[1]  # 得到当前文件的文件名，例如输入./dogs-vs-cats/test1/test1/10.jpg 得到10.jpg