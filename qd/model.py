# coding: utf-8

from bs4 import BeautifulSoup

__author__ = "lixianyang"
__email__ = "705834854@qq.com"
__date__ = "2017-05-27 11:46"


class BaseModel(object):

    def __init__(self):
        self.title = ""
        self.unique_id = ""
        self.publish_site = ""
        self.publish_time = ""
        self.insert_time = ""
        self.author = ""
        self.author_icon = ""
        self.site_icon = ""
        self.online = True
        self.channel_id = 0
        self.second_channel_id = 0

    def to_dict(self):
        return dict(self.__dict__)

    def is_valid(self):
        raise NotImplementedError


class NewsModel(BaseModel):

    def __init__(self):
        super(NewsModel, self).__init__()
        self.publish_url = ""
        self.images = list()
        self.province = ""
        self.city = ""
        self.district = ""
        self.content = list()
        self.image_number = 0
        self.tags = list()
        self.like = 0
        self.read = 0

    @staticmethod
    def is_title_valid(title):
        if isinstance(title, str):
            title = title.decode("utf-8")
        if len(title) <= 7:
            return False
        for w in title:  # 判断 title 是否包含中文
            if u"\u4e00" <= w <= u"\u9fa5":
                return True
        return False

    def is_valid(self):
        if not self.is_title_valid(self.title):
            return False
        content = self.content
        if len(content) >= 3:
            return True
        for p in content:
            for k, v in p.items():
                if k != "txt":
                    return True
                soup = BeautifulSoup(v, "html.parser")
                if len(soup.text) > 30:
                    return True
        return False


class VideoModel(BaseModel):

    def __init__(self):
        super(VideoModel, self).__init__()
        self.publish_url = ""
        self.video_url = ""
        self.video_thumbnail = ""
        self.video_duration = 0
        self.play_times = 0
        self.like = 0
        self.dislike = 0
        self.comment = 0

    def is_valid(self):
        return self.video_url and self.video_thumbnail and self.title


class JokeModel(BaseModel):

    def __init__(self):
        super(JokeModel, self).__init__()
        self.content = list()
        self.like = 0
        self.dislike = 0
        self.comment = 0

    def is_valid(self):
        return self.title and self.content
