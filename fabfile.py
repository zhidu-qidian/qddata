# coding: utf-8

from fabric.api import run, env, roles, cd

env.parallel = True
env.use_ssh_config = True

env.roledefs = {
    "qd": ["node2"],
}


COMMANDS = {"start", "stop", "restart", "status"}


@roles("qd")
def pull():
    """ pull qidian upload code """
    with cd("~/github/qddata"):
        run("git pull")


@roles("qd")
def command(cmd):
    """部署 qddata 的机器在 shell 执行命令

    :param cmd: shell 要执行的指令
    :type cmd: str
    """
    run(cmd)


def _supervisor_pipeline(pipeline, command):
    project = pipeline + ":*"
    run("supervisorctl %s %s" % (command, project))


@roles("qd")
def do(cmd, name):
    """通过 supervisorctl 命令管理该任务, cmd:start, stop, restart, status; name:upload, comment, all"""
    assert cmd in COMMANDS
    assert name in ["upload", "comment", "all"]
    names = list()
    if name == "upload":
        names.append("qidian-upload")
    elif name == "comment":
        names.append("comment-upload")
    else:
        names.extend(["qidian-upload", "comment-upload"])
    for name in names:
        _supervisor_pipeline(name, cmd)
