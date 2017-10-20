# coding: utf-8

""" 公共模块 """

from datetime import datetime
import functools
import hashlib
from HTMLParser import HTMLParser
from urllib import urlencode
from urlparse import urlparse, parse_qs, urlunparse

from bs4 import BeautifulSoup


def to_unicode(string):
    assert isinstance(string, (str, unicode))
    if isinstance(string, str):
        return string.decode("utf-8")


def ToUnicode(func):
    @functools.wraps(func)
    def wrapper(string):
        if isinstance(string, str):
            string = string.decode("utf-8")
        return func(string)

    return wrapper


_html_parser = HTMLParser()


@ToUnicode
def normalize_punctuation(string):
    pairs = [(u'\xa0', u' ')]  # 要转换的标点对儿
    for src, dst in pairs:
        string = string.replace(src, dst)
    return string


@ToUnicode
def html_un_escape(string):
    return _html_parser.unescape(string)


def extract_text_from_html(string):
    soup = BeautifulSoup(string, "html.parser")
    return soup.text


@ToUnicode
def get_string_md5(string):
    string = string.encode("utf-8")
    return hashlib.md5(string).hexdigest()


def rebuild_url(url, params):
    result = urlparse(url)
    query = parse_qs(result.query)
    query.update(params)
    new = list(result)
    new[4] = urlencode(query)
    return urlunparse(tuple(new))


def format_datetime_string(d, g=False):
    """ 归一化时间字符串 "%Y-%m-%d %H:%M:%S" or ""

    :param d: 要计算的原始数据(时间戳或字符串)
    :type d: int, float, str
    :param g: 若d不能生成, 是否要返回当前时间
    :type g: bool
    :return: 返回统一格式的时间数据或空
    :rtype: str
    """
    dt = ""
    f = "%Y-%m-%d %H:%M:%S"
    if not d:
        return datetime.now().strftime(f) if g else ""
    if isinstance(d, (int, float)) or d.isdigit():
        timestamp = int(d)
        if len(str(timestamp)) == 13:
            timestamp /= 1000
        try:
            dt = datetime.fromtimestamp(timestamp).strftime(f)
        except Exception:
            pass
    else:
        try:
            dt = datetime.strptime(d, f).strftime(f)
        except Exception:
            pass
    if not dt and g:
        dt = datetime.now().strftime(f)
    return dt


def utc_datetime_now():
    return datetime.utcnow()
