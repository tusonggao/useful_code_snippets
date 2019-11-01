# FTP操作
import ftplib

# https://www.cnblogs.com/gongxr/p/7529949.html

host = '154.92.19.167'
username = 'root'
password = 'Yisu2019'
# file = '1.txt'

f = ftplib.FTP()  # 实例化FTP对象
f.connect(host, 4098)  # 如果不指定端口，则是默认端口
f.login(username, password)  # 登录

# 获取当前路径
pwd_path = f.pwd()
print("FTP当前路径:", pwd_path)

def ftp_download():
    '''以二进制形式下载文件'''
    for i in range(100):
        file_remote = 'train_data_r1_part_{}.txt'.format(i+1)
        file_local = './round1_train/train_data_r1_part_{}.txt'.format(i+1)
        print('transfer file name', file_remote)
        bufsize = 1024  # 设置缓冲器大小
        fp = open(file_local, 'wb')
        f.retrbinary('RETR %s' % file_remote, fp.write, bufsize)
        fp.close()


def ftp_upload():
    '''以二进制形式上传文件'''
    file_remote = 'ftp_upload.txt'
    file_local = './ftp_upload.txt'
    bufsize = 1024  # 设置缓冲器大小
    fp = open(file_local, 'rb')
    f.storbinary('STOR ' + file_remote, fp, bufsize)
    fp.close()

print('starting...')
ftp_download()
# ftp_upload()
f.quit()
print('program ends here!')