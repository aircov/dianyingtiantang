import requests
from lxml import etree
import json
from queue import Queue
import threading


class DianyingtiantangSpider:
    def __init__(self):
        self.start_url = "https://www.dy2018.com/html/gndy/dyzz/index.html"
        self.url_temp = "https://www.dy2018.com/html/gndy/dyzz/index_{}.html"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                          " Chrome/70.0.3538.102 Safari/537.36",

        }

        self.url_queue = Queue()
        self.html_queue = Queue()
        self.detail_page_url_queue = Queue()
        self.movie_queue = Queue()

    def get_url_list(self):  # 构造url列表
        self.url_queue.put(self.start_url)
        for i in range(2, 100):
            self.url_queue.put(self.url_temp.format(i))

    def parse_url(self):  # 发送请求获取响应
        while True:
            url = self.url_queue.get()
            response = requests.get(url, headers=self.headers)
            # 电影天堂网站编码格式为charset=gb2312，查看网页源代码
            self.html_queue.put(response.content.decode("gbk"))
            self.url_queue.task_done()

    def get_content_list(self):  # 提取电影详情页的url
        while True:
            html_str = self.html_queue.get()
            html = etree.HTML(html_str)
            detail_url_list = html.xpath("//table[@class='tbspan']//a/@href")
            content_list = list()
            for detail_url_temp in detail_url_list:
                detail_url = "https://www.dy2018.com" + detail_url_temp
                content_list.append(detail_url)
            self.detail_page_url_queue.put(content_list)
            self.html_queue.task_done()

    def parse_detali_url(self):  # 请求详情页数据
        while True:
            detail_page_url_list = self.detail_page_url_queue.get()
            movie_list = []
            for detail_page_url in detail_page_url_list:
                print(detail_page_url)
                movie = {}
                response = requests.get(detail_page_url, headers=self.headers)
                try:
                    html_str = response.content.decode("gbk")
                except Exception as e:
                    print("%s:%s" % (e, detail_page_url))
                else:
                    html = etree.HTML(html_str)

                    # 标题
                    movie["title"] = html.xpath("//div[@class='title_all']/h1/text()")
                    movie["title"] = movie["title"][0] if len(movie["title"]) > 0 else None

                    # 封面、影视截图
                    images = html.xpath("//div[@id='Zoom']//img/@src")
                    movie["cover"] = images[0] if len(images) > 0 else None
                    movie["screenshot"] = images[1] if len(images) > 1 else None

                    # 电影详情
                    infos = html.xpath("//div[@id='Zoom']/p/text()")
                    for index, info in enumerate(infos):
                        # print(index)
                        # print(info)
                        # print("*"*30)
                        if info.startswith("◎年　　代"):
                            info = info.replace("◎年　　代", "").strip()
                            movie["year"] = info
                        elif info.startswith("◎产　　地"):
                            info = info.replace("◎产　　地", "").strip()
                            movie["area"] = info
                        elif info.startswith("◎类　　别"):
                            info = info.replace("◎类　　别", "").strip()
                            movie["category"] = info
                        elif info.startswith("◎豆瓣评分"):
                            info = info.replace("◎豆瓣评分", "").strip()
                            movie["douban_rating"] = info
                        elif info.startswith("◎片　　长"):
                            info = info.replace("◎片　　长", "").strip()
                            movie["duration"] = info
                        elif info.startswith("◎导　　演"):
                            info = info.replace("◎导　　演", "").strip()
                            movie["director"] = info
                        elif info.startswith("◎主　　演"):
                            info = info.replace("◎主　　演", "").strip()
                            actors = [info]
                            for x in range(index + 1, len(infos)):
                                actor = infos[x].strip()
                                if actor.startswith('◎简　　介'):
                                    break
                                actors.append(actor)
                            movie["actors"] = actors
                        elif info.startswith("◎简　　介"):
                            introductions = []
                            for x in range(index + 1, len(infos)):
                                introduction = infos[x].strip()
                                if introduction.startswith("导演剪辑版") or introduction.startswith("◎影片截图"):
                                    break
                                introductions.append(introduction)
                            movie["introduction"] = introductions

                    # 下载地址
                    download_url = html.xpath("//td[@bgcolor='#fdfddf']/a/text()")
                    movie["download_url"] = download_url[0] if len(download_url) > 0 else None

                    movie_list.append(movie)
            # print(movie_list)
            self.movie_queue.put(movie_list)
            self.detail_page_url_queue.task_done()

    def save_movies(self):
        while True:
            movies = self.movie_queue.get()
            with open("电影天堂3.txt", "a", encoding="utf-8") as f:
                for movie in movies:
                    f.write(json.dumps(movie, ensure_ascii=False) + "\n")
            self.movie_queue.task_done()

    def run(self):  # 实现主要逻辑
        thread_list = []

        # 1.构造url列表
        t1 = threading.Thread(target=self.get_url_list)
        thread_list.append(t1)

        # 2. 遍历列表，发送请求获取响应
        t2 = threading.Thread(target=self.parse_url)
        thread_list.append(t2)

        # 3.提取数据
        t3 = threading.Thread(target=self.get_content_list)
        thread_list.append(t3)

        # 4.请求所有详情页数据
        for i in range(5):
            t4 = threading.Thread(target=self.parse_detali_url)
            thread_list.append(t4)

        # 5. 保存数据
        for i in range(5):
            t5 = threading.Thread(target=self.save_movies)
            thread_list.append(t5)

        # 把子线程设置为守护线程，主线程结束，子线程结束
        # 开启线程
        for t in thread_list:
            t.setDaemon(True)
            t.start()

        # 让主线程等待阻塞，等待队列的任务完成之后再完成
        for q in [self.url_queue, self.html_queue, self.detail_page_url_queue, self.movie_queue]:
            q.join()

        print("主线程结束")

if __name__ == '__main__':
    dianyingtiantangspider = DianyingtiantangSpider()
    dianyingtiantangspider.run()
