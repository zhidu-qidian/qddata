# coding: utf-8

from collections import defaultdict
from datetime import datetime
import json
import logging
from unicodedata import normalize
import urllib
import re

from bs4 import BeautifulSoup
from bson import ObjectId
import requests

from qd import db, redis
from qd.utils import format_datetime_string

CHANNELS = "spider_channels"
SITES = "spider_sites"
QIDIAN = "qidian_map"
REGION = "region_map"
RULES = "qidian_filter_rule"


class PrepareUtil(object):
    """ 根据 collection, _id 获取需要的资源 """

    @classmethod
    def prepare(cls, col, _id):
        """  根据 collection name, _id 查找处理资讯需要使用到的数据

        :param col: str, collection name
        :param _id: str, 资讯的主键
        :return: site, channel, doc, mapping, region
        """
        query = {"_id": ObjectId(_id)}
        doc = db[col].find_one(query)
        query["_id"] = ObjectId(doc["channel"])
        channel = db[CHANNELS].find_one(query)
        query["_id"] = ObjectId(channel["site"])
        site = db[SITES].find_one(query)
        mapping = db[QIDIAN].find_one({"channel": str(channel["_id"])})
        if channel.get("region"):
            region = db[REGION].find_one({"_id": channel["region"]})
        else:
            region = None
        return site, channel, doc, mapping, region


class Adapter(object):
    """ 资讯数据清洗, 适配到上传接口需要的形式 """

    _map = {
        "news": "adapt_news",
        "video": "adapt_video",
        "joke": "adapt_joke",
        "atlas": "adapt_news",
    }

    @staticmethod
    def split_tag_words(string):
        """ 以 ';', ',', ':' 分割字符串 string, 返回 list """
        tags = list()
        seps = [";", ",", ":"]
        start = 0
        for i, s in enumerate(string):
            if s in seps:
                tags.append(string[start:i])
                start = i + 1
        tags.append(string[start:])
        return [tag for tag in tags if tag]

    @staticmethod
    def generate_tags_for_game(title, cid, channel_name):
        """ 游戏根据title生成需要的标签信息, 返回 list 包含生成的标签信息 """
        word = u"专区"
        tags = list()
        if channel_name.endswith(word):
            tags.append(channel_name.replace(word, u""))  # 游戏tags生成
        if cid == "11" or cid == 11:
            matches = re.findall(u"《([^》]*)》", title)
            if matches:
                tags = list(matches)
        return tags

    @classmethod
    def adapt(cls, site, channel, doc, mapping, region):
        """ 适配　mongodb 里的数据格式到线上存储接口支持的格式 """
        form = channel["form"]
        caller = cls.__dict__[cls._map[form]].__func__
        data = caller(cls, site, channel, doc, mapping)

        # 外媒(29), 奇闻(31) 频道标题和内容繁体转简体
        if mapping["first_cid"] in ["29", 29, "31", 31]:
            import opencc
            data["title"] = opencc.convert(data["title"])
            for i, item in enumerate(data["content"]):
                if "txt" in item:
                    data["content"][i]["txt"] = opencc.convert(
                        data["content"][i]["txt"]
                    )
        data["title"] = cls.normalize_unicode(data["title"])  # 归一化一些字符
        # 统一一些字段
        data["unique_id"] = "%s_%s" % (form, doc["request"])  # docid
        data["publish_site"] = doc["publish_ori_name"] or site["name"]  # pname
        pt = format_datetime_string(doc["publish_time"], g=True)
        data["publish_time"] = pt[:10] + "T" + pt[11:] + "Z"  # ptime
        data["insert_time"] = datetime.now().isoformat()[:-7] + "Z"
        data["site_icon"] = doc["publish_ori_icon"]
        data["channel_id"] = mapping["first_cid"]
        if mapping["second_cid"]:  # 如果有线上二级频道信息，则上传
            data["second_channel_id"] = mapping["second_cid"]
        if doc.get("tags"):
            data["tags"] = cls.split_tag_words(doc["tags"])
        elif form == "news":  # Fixme: 硬判断类型
            data["tags"] = cls.generate_tags_for_game(
                doc["title"],
                mapping["first_cid"],
                channel["name"]
            )  # Fixme: 游戏频道需要根据 title 生成 tags
        else:
            data["tags"] = list()
        if form != "video":  # 统一计算 image number
            data["image_number"] = sum([1 for item in data["content"] if "img" in item])
        data["online"] = True
        if region:  # 地理位置信息
            if region["province"]:
                data["province"] = region["province"]
            if region["city"]:
                data["city"] = region["city"]
            if region["county"]:
                data["district"] = region["county"]
        # Fixme: 为适配老版本api根据online_source_id拉取信息,新版本不需要该字段
        if mapping.get("online_source_sid"):
            data["source_id"] = mapping["online_source_sid"]

        # Add: 添加新的字段支持线上数据查找抓取源
        data["spider_source_id"] = str(channel["_id"])
        return data

    @classmethod
    def adapt_news(cls, site, channel, doc, mapping):
        """ 注意这里可能修改 channel["form"] 信息 """
        news = dict()
        news["title"] = cls.adapt_title(doc["title"])
        images = [feed["src"] for feed in doc["ori_feeds"]
                  if feed["width"] >= 300 and feed["height"] >= 200]
        if not images:
            images = [feed["src"] for feed in doc["gen_feeds"]]
        news["images"] = images
        cid = mapping["first_cid"]

        if cid == 45:  # 线上的段子频道
            return cls.adapt_news_to_joke(channel, doc, news)

        news["publish_url"] = doc["publish_ori_url"]
        news["content"] = cls.adapt_content(doc["content"])
        news["tags"] = list()
        news["like"] = int(doc.get("n_like", 0))
        news["read"] = int(doc.get("n_read", 0))
        return news

    @classmethod
    def adapt_news_to_joke(cls, channel, doc, news):
        channel["form"] = "joke"  # Fixme: 注意这里更改了资讯的形式
        news["like"] = int(doc.get("n_like", 0))
        news["dislike"] = int(doc.get("n_dislike", 0))
        news["comment"] = int(doc.get("n_comment", 0))
        news["images"] = list()
        content = list()
        gif = False
        too_long = False
        has_image = False
        for item in doc["content"]:
            tag = item["tag"]
            if item.get("text"):
                content.append({"txt": cls.adapt_link(item["text"])})
            elif tag == "img" and not has_image:
                has_image = True
                if item["src"].endswith("gif"):
                    gif = True
                if item["height"] >= 1158:
                    too_long = True
                news["images"].append(item["src"])
                content.append({"img": item["src"]})
        news["content"] = content
        if has_image:
            if gif:
                news["style"] = 23
            elif too_long:
                news["style"] = 22
            else:
                news["style"] = 21
        return news

    @classmethod
    def adapt_joke(cls, site, channel, doc, mapping):
        joke = dict()
        joke["title"] = doc["title"] or doc["text"]
        joke["content"] = [{"txt": doc["text"]}]
        joke["like"] = doc["n_like"]
        joke["dislike"] = doc["n_dislike"]
        joke["comment"] = doc["n_comment"]
        return joke

    @classmethod
    def adapt_video(cls, site, channel, doc, mapping):
        video = dict()
        video["title"] = doc["title"]
        video["publish_url"] = doc["publish_ori_url"]
        video["video_url"] = doc["src"]
        video["video_thumbnail"] = doc["thumbnail"]
        video["video_duration"] = doc["duration"]
        video["play_times"] = doc["n_read"]
        video["like"] = int(doc.get("n_like", 0))
        video["dislike"] = int(doc.get("n_dislike", 0))
        video["comment"] = 0  # 评论数量上报0 int(doc.get("n_comment", 0))
        return video

    @staticmethod
    def keep_only_one(string, keep):
        pattern = u"%s{2,}" % keep
        if keep.startswith("\\"):
            keep = keep[1:]
        return re.sub(pattern, keep, string)

    @classmethod
    def adapt_title(cls, title):
        """ 移除 title 中指定格式的字符 """
        callers = [cls.remove_continuous_punctuations, cls.remove_end_digits]
        string = title
        for caller in callers:
            string = caller(string)
        return string

    @staticmethod
    def normalize_unicode(string):
        if isinstance(string, str):
            string = string.decode("utf-8")
        return normalize("NFKC", string)

    @classmethod
    def remove_continuous_punctuations(cls, string):
        """ 移除字符串中连续的 '!', '?' 只保留一个 """
        chars = [u"！", u"？", u"!", u"\?"]
        s = string
        for char in chars:
            s = cls.keep_only_one(s, char)
        return s

    @classmethod
    def remove_end_digits(cls, string):
        """ 移除字符串末尾 (1/12) 格式的字符 """
        pattern = re.compile(r"\(\d/\d+\)")
        match = pattern.search(string, -10)
        if match:
            return string.replace(match.group(), "").strip()
        else:
            return string

    @classmethod
    def adapt_content(cls, content):
        """ 适配 mongodb content 字段格式到线上 postgres 支持的 content 格式"""
        result = list()
        for item in content:
            p = cls.adapt_item(item)
            if p:
                result.append(p)
        return result

    @classmethod
    def adapt_item(cls, item):
        """ 适配 content 中的每一项 """

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
                tag["href"] = cls.LinkApi + urllib.quote(text.encode("utf-8"))
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


class Filter(object):
    """ 过滤不合法的数据 """

    rules = None  # title 的过滤规则
    pnames = [u"尤果", u"ugirls"]

    @classmethod
    def filter(cls, site, channel, doc, mapping):
        """ 根据规则判断是否需过滤掉该条数据 True(丢弃数据), False(保留数据)"""
        if not mapping or not mapping["online"]:  # 没有上传的映射信息
            return True
        form = channel["form"]
        if form == "news" or form == "atlas":
            return cls.news_filter(doc, mapping)
        elif form == "joke":
            return cls.joke_filter(doc, mapping)
        elif form == "video":
            return cls.video_filter(doc, mapping)
        else:
            logging.error("not support form:%s in _id:%s" % (form, str(doc["_id"])))
            return True

    @classmethod
    def news_filter(cls, doc, mapping):
        uid = doc["unique_id"]
        if cls.news_title_filter(doc["title"], mapping["first_cid"], uid):
            return True
        if cls.news_content_filter(doc["content"], uid):
            return True
        if cls.news_publish_name_filter(doc["publish_site"], uid):
            return True
        return False

    @classmethod
    def news_publish_name_filter(cls, p_site, uid):
        """ 根据 publish site 字段过滤 """
        if isinstance(p_site, str):
            p_site = p_site.decode("utf-8")
        p_site = p_site.lower()
        for word in cls.pnames:
            if word in p_site:
                logging.warn("publish name contains filter word: %s" % uid)
                return True
        return False

    @classmethod
    def news_title_filter(cls, title, cid, uid):
        """  根据线上的频道信息判断该标题是否应该被过滤

        :param title: str or unicode, 标题
        :param cid: int, 线上一级频道 id
        :return: bool, True(需要被过滤) False(不应该被过滤)
        """
        if isinstance(title, str):
            title = title.decode("utf-8")
        if len(title) <= 7:  # 标题长度小于 7 个汉字
            logging.warn("too short title: %s" % uid)
            return True
        for word in title:
            if u"\u4e00" <= word and word <= u"\u9fa5":
                break
        else:  # 标题不包含中文字符
            logging.warn("english title: %s" % uid)
            return True
        title = title.lower()
        rules = cls.get_title_filter_rules()  # 根据关键字过滤
        for word in rules[0]:  # 0 表示所有 title 都需要过滤的关键字
            if word in title:
                logging.warn("title contains filter word: %s" % uid)
                return True
        for word in rules[int(cid)]:
            if word in title:
                logging.warn("title contains filter word: %s" % uid)
                return True
        return False

    @classmethod
    def get_title_filter_rules(cls):
        """ 获取标题过滤的规则

        :return: dict, {int: set()} int(要过滤的频道), set(要过滤的关键字)
        """
        if cls.rules is None:
            rules = defaultdict(set)
            for rule in db[RULES].find():
                rules[int(rule["chid"])].add(rule["word"].lower())
            cls.rules = rules
            return rules
        else:
            return cls.rules

    @classmethod
    def news_content_filter(cls, content, uid):
        """　判断 content 内容长度是否需要过滤掉 """
        if len(content) >= 3:
            return False
        for item in content:
            for k, v in item.items():
                if k != "txt" and k != "vid":
                    return False
                soup = BeautifulSoup(v, "html.parser")
                if len(soup.text) > 30:
                    return False
        logging.warn("content text too short: %s" % uid)
        return True

    @classmethod
    def joke_filter(cls, doc, mapping):
        return not (doc["title"] and doc["content"])

    @classmethod
    def video_filter(cls, doc, mapping):
        return not (doc["video_url"] and doc["video_thumbnail"] and doc["title"])


class Uploader(object):
    """ 上传数据到存储接口，并触发后续处理步骤

    1. 上传到存储数据
    2. 上报特殊属性
    3. 插入 ES 引擎
    4. 触发离线处理任务
    5. 触发评论抓取任务
    """

    baseurl = "http://10.25.60.218:8081/api/store/"
    _map = {
        "news": "news",
        "joke": "joke",
        "video": "video",
        "atlas": "news",
    }
    _map_roles = {"hot": 3, "big": 2}

    @classmethod
    def upload(cls, channel, doc, mapping, olddoc):
        """ 存储并触发后续任务 """
        uid = doc["unique_id"]
        url = cls.baseurl + cls._map[channel["form"]]
        r = requests.post(url, json=doc, timeout=(5, 10))  # 存储
        status = r.status_code
        if 500 == status:
            logging.warn("unique_id:%s duplicate key warn" % uid)
        elif 400 == status:
            logging.error("unique_id:%s bad params for upload" % uid)
        else:
            _id = r.json()["id"]
            cls.report(_id, channel, doc, mapping)  # 上报特殊属性,插入es,触发离线处理
            if olddoc.get("comment"):  # 调用抓取评论的任务
                d = {
                    "news_id": doc["unique_id"],
                    "news_url": doc["publish_url"],
                    "comment_id": olddoc["comment"]["id"],
                    "comment_url": ""
                }
                redis.lpush("v1:qd:comments", json.dumps(d))
            logging.info("unique_id:%s nid:%s upload success" % (uid, _id))

    @classmethod
    def report(cls, _id, channel, doc, mapping):
        """ 上报资讯的特殊属性, 并插入 es, 调用离线处理接口 """
        have_image = doc.get("images")
        for w in mapping.get("type", "").split("#"):
            if w not in cls._map_roles:
                continue
            if w == "big" and not have_image:  # 大图没有图片则不报大图
                continue
            cls.report_special_role(_id, cls._map_roles[w])
        cls.report_special_role(_id, 1)  # 插入 es 引擎
        cls.report_offline_process(_id)  # 调用离线处理服务

    @staticmethod
    def report_special_role(_id, role):
        service = "http://bdp.deeporiginalx.com/v2/ns/cr/add"
        params = {"nid": _id, "datatype": role}
        try:
            requests.get(service, params=params, timeout=5)
        except Exception as e:
            logging.error(e.message, exc_info=True)
        else:
            logging.info("nid:%s type:%s" % (_id, role))

    @staticmethod
    def report_offline_process(_id):
        url = "http://120.55.88.11:9000/news_queue/produce_nid?nid=" + str(_id)
        try:
            requests.get(url, timeout=(5, 10))
        except Exception as e:
            logging.error(e.message, exc_info=True)
        else:
            logging.info("nid:%s offline job" % _id)


def show(data, prefix=""):
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, list):
                if len(v) == 0:
                    print prefix, k, ":", v
                elif isinstance(v[0], dict):
                    print prefix, k, ":"
                    for item in v:
                        show(item, prefix=prefix + "\t")
                else:
                    print prefix, k, ":", v
            else:
                print prefix, k, ":", v


def news_upload_task(col, _id, debug=False):
    site, channel, doc, mapping, region = PrepareUtil.prepare(col, _id)
    if not (site and channel and doc and mapping):
        logging.error("_id:%s required data missing" % _id)
        return
    data = Adapter.adapt(site=site, channel=channel, doc=doc,
                         mapping=mapping, region=region)
    if Filter.filter(site=site, channel=channel, doc=data, mapping=mapping):
        return
    if debug:
        show(data)
    else:
        Uploader.upload(channel=channel, doc=data, mapping=mapping, olddoc=doc)


def main():
    news_upload_task("v1_news", "59845324921e6d3f8975d637", debug=True)
    db.client.close()


if __name__ == "__main__":
    main()
