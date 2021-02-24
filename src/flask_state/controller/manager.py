import os
import sched
import threading
import time

from flask import current_app, request

## convert imports from relative to absolute
from ..conf.config import Config
from ..controller.interceptors import json_required
from ..exceptions import ErrorResponse, FlaskStateError
from ..exceptions.error_code import MsgCode
from ..exceptions.log_msg import ErrorMsg, InfoMsg
from ..models import model_init_app
from ..services import redis_conn
from ..services.host_status import (
    query_flask_state_host,
    record_flask_state_host,
    record_flask_state_io_host,
)
from ..utils.auth import auth_method, auth_user
from ..utils.constants import HttpMethod, HTTPStatus
from ..utils.file_lock import Lock
from ..utils.format_conf import format_address
from ..utils.logger import DefaultLogger, logger
from .response_methods import make_response_content


# should this be in a view module instead?
@auth_user
@auth_method
@json_required
def query_flask_state():
    """
    Query the local state and redis status
    :return: flask response
    """
    try:
        b2d = request.json
        time_quantum = b2d.get("timeQuantum")
        return make_response_content(resp=query_flask_state_host(time_quantum))
    except FlaskStateError as e:
        logger.warning(e)
        return make_response_content(e, http_status=e.status_code)
    except Exception as e:
        logger.exception(e)
        error_response = ErrorResponse(MsgCode.UNKNOWN_ERROR)
        http_status = HTTPStatus.INTERNAL_SERVER_ERROR
        return make_response_content(error_response, http_status=http_status)


# consider extracting this logic into its own module and break into functions
def record_timer(app, function, interval, lock_group, lock_key, priority=1)
    app.locks[lock_group][lock_key] = Lock.get_file_lock(lock_key)

    with app.app_context():
        try:
            current_app.locks[lock_group][lock_key].acquire()
            scheduler = sched.scheduler(time.time, time.sleep)

            event = {
                "action": function,
                "priority": priority,
                "kwargs": {
                    "interval": interval,
                    "target_time": round(time.time()) / 60 + 1) * 60)
                }
            }

            time.sleep(10)
            action(interval, target_time)
            while True:
                event["kwargs"]["target_time"] += interval
                event["delay"] = target_time - time.time()
                scheduler.enter(**event)
                scheduler.run()

        except BlockingIOError:
            pass
        except Exception as e:
            current_app.lock_flask_state.release()
            raise e


def init_url_rules(app)
    app.add_url_rule(
        "/v0/state/hoststatus",
        endpoint="state_host_status",
        view_func=query_flask_state,
        methods=[HttpMethod.POST.value],
    )

def init_db(app):
    try:
        sqlalchemy_binds = app.config["SQLALCHEMY_BINDS"]
        sqlite_binds = sqlalchemy_binds[Config.DEFAULT_BIND_SQLITE]
        sqlalchemy_binds[Config.DEFAULT_BIND_SQLITE] = format_address(sqlite_binds)
    except KeyError as e:
        # Raise a custom exception here i.e => raise SqliteBindingMissing from e
        raise e(ErrorMsg.LACK_SQLITE.get_msg())

def init_redis(app):
    state = app.config.get("REDIS_CONF", {})
    if state.get("REDIS_STATUS"):
        keys = ["REDIS_HOST", "REDIS_PORT", "REDIS_PASSWORD"]
        config = {k: v for k, v in state.items() if k in keys}
        redis_conn.set_redis(redis_conf)

def init_recorder_threads(app, interval)
    lock_group = "record_flask_state"
    app.locks = {lock_group: {}}
    target_kwargs = {"app": app, "lock_group": lock_group}

    threads = {}
    threads["host"] = threading.Thread(
        target=record_timer,
        kwargs={
            "function": record_flask_state_host,
            "interval": interval,
            **target_kwargs
        }
    )
    threads["io"] = threading.Thread(
        target=record_timer,
        kwargs={
            "function": record_flask_state_io_host,
            "interval": 10,
            **target_kwargs
        }
    )

    return threads

def init_app(app, interval=60, custom_logger=None):
    logger.set(custom_logger or DefaultLogger())

    if not isinstance(interval, int):
        ## use custom exceptions and inherit from TypeError instead
        error_template = ErrorMsg.DATA_TYPE_ERROR.get_msg
        raise TypeError(error_template.format(int.__name__, type(interval).__name__)

    init_url_rules(app)
    init_db(app)
    init_redis(app)
    model_init_app(app)

    recorder_threads = init_recorder_threads(app)
    for thread in recorder_threads.values():
        thread.setDaemon(True)
        thread.start()