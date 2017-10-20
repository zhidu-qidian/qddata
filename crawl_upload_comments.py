# coding: utf-8

import json
import logging

import requests

from qd import get_cache_client
from qd.comments import run_comment_task
from qd.utils import format_datetime_string

__author__ = "lixianyang"
__email__ = "705834854@qq.com"
__date__ = "2017-06-06 16:27"


redis = get_cache_client(2)
API_URL_PREFIX = "http://10.25.60.218:8081"
STORE_COMMENT_URL = "%s/api/store/comment" % API_URL_PREFIX
UPDATE_COMMENT_URL = "%s/api/update/comment" % API_URL_PREFIX


def upload(comment):
    t = format_datetime_string(comment.publish_time, g=True)
    data = {
        "content": comment.text,
        "commend": comment.n_like,
        "insert_time": t[:10] + "T" + t[11:] + "Z",
        "user_name": comment.user_name,
        "avatar": comment.user_icon,
        "foreign_id": comment.foreign,
        "unique_id": comment.unique,
    }
    try:
        r = requests.post(STORE_COMMENT_URL, json=data)
    except Exception as e:
        logging.error(e.message, exc_info=True)
    else:
        if r.status_code == 200:
            logging.info(r.content)
            return True
    return False


def update_comment_number(docid, n):
    requests.put(UPDATE_COMMENT_URL, data={"docid": docid, "n": n})


def main():
    while 1:
        key, data = redis.brpop("v1:qd:comments")
        try:
            doc = json.loads(data)
            news_id = doc["news_id"]
            logging.info("Crawl comment for %s" % news_id)
            comment_id = doc.get("comment_id", "")
            news_url = doc.get("news_url", "")
            comment_url = doc.get("comment_url", "")
        except Exception as e:
            logging.error(e.message, exc_info=True)
        else:
            comments = run_comment_task(news_id, comment_id, news_url, comment_url)
            n = 0
            for comment in comments:
                if comment.is_valid():
                    success = upload(comment)
                    if success:
                        n += 1
            update_comment_number(docid=news_id, n=n)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        filename="log-comment.log",
                        filemode="a+")
    main()
