import yagmail

# 连接邮箱服务器
yag = yagmail.SMTP(user="360426145@qq.com", password="zzmaqtctbweobigd", host='smtp.qq.com')

# 邮箱正文
contents = ['今天是周末,我要学习, 学习使我快乐;', '<a href="https://www.python.org/">python官网的超链接</a>', './girl.png']

# 发送邮件
yag.send('tusonggao@163.com', '主题:学习使我快乐', contents)