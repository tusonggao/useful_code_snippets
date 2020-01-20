import os

def get_all_files_dirs(root_dir_name):  # 得到文件夹下所有文件名路径和目录路径（会递归到子目录）
    filenames_lst, dirnames_lst = [], []
    for dirpath, dirnames, filenames in os.walk(root_dir_name):
        for filename in filenames:
            filenames_lst.append(os.path.join(dirpath, filename))
        for dirname in dirnames:
            dirnames_lst.append(os.path.join(dirpath, dirname))
    return filenames_lst, dirnames_lst


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


##############################################################################

import glob
file_name_lst = glob.glob('./text_classification/**/*.py', recursive=False)  # 得到当前文件夹下的text_classification子文件夹中所有python源文件
file_name_lst = glob.glob('./text_classification/*.py', recursive=True)   # 得到当前文件夹下的text_classification子文件夹中所有python源文件，会进一步查找子目录
for file_name in glob.iglob('./text_classification/**/*.py', recursive=True):  # 通过迭代器遍历
    print('file_name is ', file_name)


##############################################################################

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 得到当前文件的的父目录

filenames, _ = get_all_files_dirs('./flask_api/')
print('filenames is ', filenames)

# dst_file_name = os.path.split(file_name_with_directory)[1]  # 得到当前文件的文件名，例如输入./dogs-vs-cats/test1/test1/10.jpg 得到10.jpg