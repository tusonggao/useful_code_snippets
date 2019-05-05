import webbrowser
import time, os

import win32api

num = 1

while num <= 5:
    num += 1
    time.sleep(60*10)
    # webbrowser.open("https://colab.research.google.com/drive/1VVhv7byQBH8K7HJW_t2srOKoQOOYctY4#scrollTo=EAn4Hpz2pEM9", 0, False)
    print('open a url, num: ', num)
    # os.system("C:/Users/tusonggao/AppData/Local/Google/Chrome/Application/chrome.exe https://colab.research.google.com/drive/1VVhv7byQBH8K7HJW_t2srOKoQOOYctY4#scrollTo=EAn4Hpz2pEM9")
    win32api.WinExec("C:/Users/tusonggao/AppData/Local/Google/Chrome/Application/chrome.exe https://colab.research.google.com/drive/1VVhv7byQBH8K7HJW_t2srOKoQOOYctY4#scrollTo=EAn4Hpz2pEM9")