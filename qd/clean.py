# coding: utf-8

import re
from urllib import quote

from bs4 import BeautifulSoup

from qd.model import NewsModel, VideoModel, JokeModel


def adapt_tag(title, cid, channel_name):
    """游戏根据title生成需要的标签信息,inline操作,直接修改news["tags"]字段"""
    word = u"专区"
    tags = list()
    if channel_name.endswith(word):
         tags.append(channel_name.replace(word, u""))  # 游戏tags生成
    if cid == "11" or cid == 11:
        matches = re.findall(u"《([^》]*)》", title)
        if matches:
            tags = list(matches)
    return tags


class MgAdapter(object):
    """ 适配 mongo 数据为 pg 格式,"""

    LinkApi = "http://deeporiginalx.com/search.html#sw="
    IgnoreTexts = {u"点击查看视频"}
    RemoveLinkTexts = {u"阅读原文", u"阅读全文"}

    @classmethod
    def adapt_link(cls, string):
        soup = BeautifulSoup(string, "html.parser")
        for tag in soup.find_all("a"):
            text = tag.text
            if text in cls.IgnoreTexts:
                continue
            elif cls.should_remove_link(text):
                string = cls.replace(text)
                new_tag = soup.new_tag("span")
                if string:
                    new_tag.string = string
                else:
                    new_tag.string = u"阅读原文"
                tag.replace_with(new_tag)
            else:
                tag["href"] = cls.LinkApi + quote(text.encode("utf-8"))
        return str(soup)

    @classmethod
    def should_remove_link(cls, string):
        for text in cls.RemoveLinkTexts:
            if text in string:
                return True
        return False

    @classmethod
    def replace(cls, string):
        for text in cls.RemoveLinkTexts:
            string = string.replace(text, "")
        return string

    @classmethod
    def adapt_item(cls, item):

        def size_too_small(w, h):
            if w / h < 7 and w > 100 and h > 75:
                return False
            else:
                return True

        ignore_tags = {"object", "param", "audio", "source", "track", "canvas",
                      "map", "area", "svg", "math"}
        video_tags = {"iframe", "embed", "video"}
        tag = item["tag"]
        if tag in ignore_tags:
            return None
        if tag == "img":
            if item.get("qr") or item.get("ad"):
                return None
            if size_too_small(item["width"], item["height"]):
                return None
            if item.get("src"):
                return {"img": item["src"]}
            else:
                return None
        if tag == "p":
            return {"txt": cls.adapt_link(item["text"])}
        if tag in video_tags:
            if "src" in item:
                return {"vid": u"<{0} src='{1}'></{0}>".format("iframe",
                                                               item["src"])}
            elif "text" in item and "<source src" in item["text"]:
                string = item["text"].replace("<source", "<iframe")
                return {"vid": string.replace("source>", "iframe>")}
        if item.get("text"):
            text = cls.adapt_link(item["text"]).decode("utf-8")
            template = u"<{tag}>{text}</{tag}>"
            return {"txt": template.format(tag=tag, text=text)}
        return None

    @classmethod
    def adapt_content(cls, content):
        result = list()
        for item in content:
            p = cls.adapt_item(item)
            if p:
                result.append(p)
        return result

    @staticmethod
    def keep_only_one(string, keep):
        pattern = u"%s{2,}" % keep
        if keep.startswith("\\"):
            keep = keep[1:]
        return re.sub(pattern, keep, string)

    @classmethod
    def adapt_title(cls, title):
        """移除title中连续的多个！,？只保留一个"""
        chars = [u"！", u"？", u"!", u"\?"]
        string = title
        for char in chars:
            string = cls.keep_only_one(string, char)
        return string


def adapt_news(doc):
    news = NewsModel()
    news.title = MgAdapter.adapt_title(doc["title"])
    news.publish_url = doc["publish_ori_url"]
    images = [feed["src"] for feed in doc["ori_feeds"]
              if feed["width"] >= 300 and feed["height"] >= 200]
    if not images:
        images = [feed["src"] for feed in doc["gen_feeds"]]
    news.images = images
    news.content = MgAdapter.adapt_content(doc["content"])
    news.image_number = doc["n_images"]
    news.tags = list()
    news.like = int(doc.get("n_like", 0))
    news.read = int(doc.get("n_read", 0))
    return news


def adapt_video(doc):
    video = VideoModel()
    video.title = doc["title"]
    video.publish_url = doc["publish_ori_url"]
    video.video_url = doc["src"]
    video.video_thumbnail = doc["thumbnail"]
    video.video_duration = doc["duration"]
    video.play_times = doc["n_read"]
    video.like = int(doc.get("n_like", 0))
    video.dislike = int(doc.get("n_dislike", 0))
    video.comment = 0  # int(doc.get("n_comment", 0))
    return video


def adapt_joke(doc):
    joke = JokeModel()
    joke.title = doc["text"]  # 段子的内容作为标题
    joke.content = [{"txt": doc["text"]}]
    joke.like = doc["n_like"]
    joke.dislike = doc["n_dislike"]
    joke.comment = doc["n_comment"]
    return joke