import requests
from lxml import etree


def spider(url):
	headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36'}
	html = requests.get(url, headers=headers)
	#print(html.text)
	select = etree.HTML(html.text)
	outcomes = select.xpath("//div[@class='opr-recommends-merge-content']//div[contains(@class, 'opr-recommends-merge-item')]")
	print('len of outcomes is ', len(outcomes))
	return outcomes


url = 'https://www.baidu.com/s?wd=css%20selector&rsv_spt=1&rsv_iqid=0x81567c01000034be&issp=1&f=3&rsv_bp=0&rsv_idx=2&ie=utf-8&tn=baiduhome_pg&rsv_enter=1&rsv_sug3=6&rsv_sug1=5&rsv_sug7=100&rsv_sug2=0&prefixsug=css%2520se&rsp=0&inputT=3064&rsv_sug4=3064'
print(spider(url))
#spider(url)

