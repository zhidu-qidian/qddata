# coding: utf-8

import json
import logging

from qd import redis
from upload import news_upload_task


class Runner(object):

    def __init__(self, key):
        self.key = key

    def run_forever(self, names, caller):
        while 1:
            try:
                key, data = redis.brpop(self.key)
                doc = json.loads(data)
                args = [doc.get(name) for name in names]
                caller(*args)
            except Exception as e:
                logging.error(e.message, exc_info=True)


def config_logging(suffix=""):
    from logging.handlers import TimedRotatingFileHandler
    filename = "log-" + suffix + ".log"
    fileHandler = TimedRotatingFileHandler(
        filename=filename, when='midnight', backupCount=15)
    baseFormatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                     "%Y-%m-%d %H:%M:%S")
    fileHandler.setFormatter(baseFormatter)
    logging.getLogger().addHandler(fileHandler)
    logging.getLogger().setLevel(level=logging.INFO)


def main():
    import sys
    import crawl_upload_comments
    m = sys.argv[1].lower()
    config_logging(m)
    if m == "upload":  # 上传资讯任务
        runner = Runner(key="v1:store:qdzx")
        runner.run_forever(["col", "_id"], news_upload_task)
    elif m == "comment":  # 评论下载上传任务
        crawl_upload_comments.main()
    else:
        raise ValueError("Only support upload, comment")


if __name__ == "__main__":
    main()
