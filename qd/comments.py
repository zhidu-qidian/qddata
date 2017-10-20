# coding: utf-8

from datetime import datetime
import hashlib
import json

import http
from utils import html_un_escape

__author__ = "lixianyang"
__email__ = "705834854@qq.com"
__date__ = "2017-05-24 14:00"


class Comment(object):

    def __init__(self):
        self.foreign = ""  # 外键(逻辑关联)
        self.user_name = u"匿名游客"  # 作者
        self.user_icon = ""  # 作者头像
        self.publish_time = ""
        self.text = ""  # 评论内容
        self.n_like = 0  # 赞
        self.n_dislike = 0  # 踩
        self.unique = ""  # 唯一性约束
        self.time = datetime.utcnow()

    def to_dict(self):
        return dict(self.__dict__)

    def is_valid(self):
        if self.user_icon is None:
            self.user_icon = ""
        return self.user_name and self.text


class CommentTaskInterface(object):
    START = 0

    @classmethod
    def get(cls, _id, news_id, news_url, page=0):
        raise NotImplementedError

    @classmethod
    def pages(cls, _id, news_url):
        return 1

    @classmethod
    def run(cls, comment_id, news_id, refer=None):
        news_url = refer
        pages = cls.pages(comment_id, news_url)
        for page in range(cls.START, pages + cls.START):
            comments = cls.get(comment_id, news_id, news_url, page=page)
            for comment in comments:
                comment.text = html_un_escape(comment.text)
                yield comment


class KuaiBaoTask(CommentTaskInterface):
    TEMPLATE = "http://r.cnews.qq.com/getQQNewsComment/?" \
               "comment_id={_id}&page={page}"
    LIMIT = 20
    START = 1

    @classmethod
    def get_response_json(cls, _id, page, refer=None):
        url = cls.TEMPLATE.format(_id=_id, page=page)
        refer = "http://cnews.qq.com/cnews/android/"
        request = http.Request.from_random_mobile(url, headers={"referer": refer})
        try:
            response = http.download(request)
        except Exception:
            return None
        else:
            return json.loads(response.body)

    @classmethod
    def pages(cls, _id, news_url):
        refer = news_url
        result = cls.get_response_json(_id, page=cls.START, refer=refer)
        if result is None:
            return 0
        count = int(result["comments"].get("count", 0))
        return (count + cls.LIMIT - 1) / cls.LIMIT

    @classmethod
    def get(cls, _id, news_id, news_url, page=1):
        refer = news_url
        result = cls.get_response_json(_id, page, refer)
        if result is None:
            return list()
        _comments = result["comments"].get("new")
        comments = list()
        for _comment in _comments:
            for c in _comment:
                comment = Comment()
                comment.user_name = c["nick"]
                comment.user_icon = c["head_url"]
                dt = datetime.fromtimestamp(float(c["pub_time"]))
                comment.publish_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                comment.n_like= c["agree_count"]
                # document["comment_id"] = c["reply_id"]
                comment.text = c["reply_content"]
                comments.append(comment)
        return comments


class News163Task(CommentTaskInterface):
    TEMPLATE = "http://comment.api.163.com/" \
               "api/v1/products/a2869674571f77b5a0867c3d71db5856/" \
               "threads/{_id}/app/comments/newList?" \
               "offset={offset}&limit={limit}"
    LIMIT = 40

    @classmethod
    def get_response_json(cls, _id, page, refer=None):
        offset = page * cls.LIMIT
        url = cls.TEMPLATE.format(_id=_id, offset=offset, limit=cls.LIMIT)
        request = http.Request.from_random_mobile(url, headers={"referer": refer})
        try:
            response = http.download(request)
        except Exception:
            return None
        else:
            return json.loads(response.body)

    @classmethod
    def get_comment_ids(cls, result):
        _ids = list()
        for _id in result.get("commentIds", []):
            ids = _id.split(",")
            if ids:
                _ids.append(ids[0])
        return _ids

    @classmethod
    def pages(cls, _id, news_url):
        refer = news_url
        result = cls.get_response_json(_id, cls.START, refer)
        if result is None:
            return 0
        count = result["newListSize"]
        return (count + cls.LIMIT - 1) / cls.LIMIT

    @classmethod
    def get(cls, _id, news_id, news_url, page=0):
        refer = news_url
        result = cls.get_response_json(_id, page, refer)
        if result is None:
            return list()
        ids = cls.get_comment_ids(result)
        comments = list()
        _comments = result.get("comments", {})
        for _id in ids:
            doc = Comment()
            comment = _comments.get(_id)
            if not comment:
                continue
            nickname = comment["user"].get("nickname")
            if nickname:
                doc.user_name = nickname
            logo = comment["user"].get("avatar", "")
            if ("netease.com" in logo and "noface" in logo) or "face_big" in logo:
                logo = None
            if logo:
                doc.user_icon = logo
            doc.publish_time = comment["createTime"]
            vote = comment.get("vote")
            doc.n_like = int(vote) if vote else 0
            # doc["comment_id"] = _id
            doc.text = comment["content"]
            comments.append(doc)
        return comments


class WechatTask(CommentTaskInterface):

    @classmethod
    def get_comment_url(cls, news_url):
        news_prefix = "http://mp.weixin.qq.com/s"
        comment_prefix = "http://mp.weixin.qq.com/mp/getcomment"
        if not news_url.startswith(news_prefix):
            return None
        return news_url.replace(news_prefix, comment_prefix)

    @classmethod
    def get(cls, _id, news_id, news_url, page=0):
        comment_url = cls.get_comment_url(news_url)
        request = http.Request.from_default_browser(url=comment_url, headers={"referer": news_url})
        try:
            response = http.download(request)
        except Exception:
            return list()
        result = json.loads(response.body)
        _comments = result.get("comment", [])
        comments = list()
        for _comment in _comments:
            comment = Comment()
            comment.user_name = _comment["nick_name"]
            comment.user_icon = _comment["logo_url"]
            dt = datetime.fromtimestamp(_comment["create_time"])
            comment.publish_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            comment.n_like = _comment["like_num"]
            # comment["comment_id"] = _comment["content_id"]
            comment.text = _comment["content"]
            comments.append(comment)
        return comments


class WeixinTask(CommentTaskInterface):

    @classmethod
    def get(cls, _id, news_id, news_url, page=0):
        from qd import db
        comments = list()
        data = db["wechat_comment"].find_one({"meta.comment_id": _id})
        if not data:
            return comments
        items = data["electedcomment"]
        for item in items:
            comment = Comment()
            comment.user_name = item["nickname"]
            comment.user_icon = item["logourl"]
            comment.publish_time = datetime.fromtimestamp(item["createtime"])\
                .strftime("%Y-%m-%d %H:%M:%S")
            comment.n_like = item["likenumber"]
            comment.text = item["content"]
            comments.append(comment)
        return comments


class WeiboTask_V1(CommentTaskInterface):
    ACCESS_TOKEN = "2.004t5RdC0hJy9lac6c22897742owGD"
    TEMPLATE = "https://api.weibo.com/2/comments/show.json?" \
               "access_token={at}&id={_id}&count={count}&page={page}"
    COUNT = 50
    START = 1

    @classmethod
    def get_response_json(cls, _id, page, refer=None):
        url = cls.TEMPLATE.format(at=cls.ACCESS_TOKEN, _id=_id, count=cls.COUNT, page=page, )
        request = http.Request.from_default_browser(url, headers={"referer": refer})
        try:
            response = http.download(request)
        except Exception:
            return None
        else:
            return json.loads(response.body)

    @classmethod
    def pages(cls, config, news_url):
        return 1

    @classmethod
    def get(cls, _id, news_id, news_url, page=1):

        refer = news_url
        result = cls.get_response_json(_id, page, refer)
        if result is None or result == []:
            return list()
        _comments = result["comments"]
        comments = list()
        for _comment in _comments:
            document = Comment()
            document.text = _comment["text"]
            if not cls.comment_filter(document.text):
                continue
            document.user_name = _comment["user"].get("name")
            document.user_icon = _comment["user"].get("profile_image_url")
            document.publish_time = cls.wrap_time(_comment["created_at"])
            document.n_like = _comment["user"].get("favourites_count", 0)
            # document["comment_id"] = _comment["id"]
            comments.append(document)
        return comments

    @classmethod
    def comment_filter(cls, comment_content):
        filter_item = [
            "Comment with pics",
            "@",
            "<img",
            u"转发微博",
        ]
        for i in filter_item:
            if i in comment_content:
                return False
        return True

    @classmethod
    def wrap_time(cls, time_text):
        M = {
            "Jan": "1",
            "Feb": "2",
            "Mar": "3",
            "Apr": "4",
            "May": "5",
            "June": "6",
            "July": "7",
            "Aug": "8",
            "Sept": "9",
            "Oct": "10",
            "Nov": "11",
            "Dec": "12"
        }
        time_info = time_text.split(" ")
        year = time_info[5]
        mon = M[time_info[1]]
        day = time_info[2]
        hms = time_info[3]
        result = "{}-{}-{} {}".format(year, mon, day, hms)
        return result


def unique(news_id, user_name, content):
    m = hashlib.md5()
    m.update(to_utf8(str(news_id)))
    if user_name:
        m.update(to_utf8(user_name))
    if content:
        m.update(to_utf8(content))
    return m.hexdigest()


def to_utf8(string):
    if isinstance(string, unicode):
        return string.encode("utf-8")
    return string


def run_comment_task(news_id, comment_id, news_url, comment_url):
    prefix = news_url[:40]
    if "kuaibao" in prefix:
        cls = KuaiBaoTask
    elif "163.com" in prefix:
        cls = News163Task
    elif "weixin.qq.com" in prefix:
        cls = WeixinTask
    else:
        return
    comments = cls.run(comment_id=comment_id, news_id=news_id, refer=news_url)
    for comment in comments:
        comment.foreign = news_id
        comment.unique = unique(news_id, comment.user_name, comment.text)
        yield comment
