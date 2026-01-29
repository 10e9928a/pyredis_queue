# -*- coding: utf-8 -*-
"""
工具函数模块

提供一些辅助功能。
"""

from __future__ import annotations

import functools
import logging
import signal
import time
from collections.abc import Generator, Iterable
from itertools import islice
from types import FrameType
from typing import Callable, TypeVar

logger = logging.getLogger(__name__)

# 泛型类型变量
T = TypeVar('T')
R = TypeVar('R')


def retry(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,)
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    重试装饰器

    :param max_retries: 最大重试次数
    :param delay: 初始延迟（秒）
    :param backoff: 延迟递增因子
    :param exceptions: 需要重试的异常类型
    :return: 装饰器函数
    """
    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        @functools.wraps(func)
        def wrapper(*args: object, **kwargs: object) -> R:
            current_delay = delay
            last_exception: BaseException | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f'{func.__name__} 执行失败，'
                            + f'{current_delay}秒后进行第{attempt + 1}次重试: {e}'
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff

            if last_exception is not None:
                raise last_exception
            raise RuntimeError('重试失败')

        return wrapper
    return decorator


def timeout(seconds: int) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    超时装饰器（仅在Unix系统有效）

    :param seconds: 超时时间（秒）
    :return: 装饰器函数
    """
    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        def handler(_signum: int, _frame: FrameType | None) -> None:
            raise TimeoutError(f'{func.__name__} 执行超时 ({seconds}秒)')

        @functools.wraps(func)
        def wrapper(*args: object, **kwargs: object) -> R:
            old_handler = signal.signal(signal.SIGALRM, handler)
            _ = signal.alarm(seconds)

            try:
                result = func(*args, **kwargs)
            finally:
                _ = signal.alarm(0)
                _ = signal.signal(signal.SIGALRM, old_handler)

            return result

        return wrapper
    return decorator


def log_execution(func: Callable[..., R]) -> Callable[..., R]:
    """
    日志装饰器

    记录函数的执行时间和结果。
    """
    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> R:
        start_time = time.time()
        func_name = func.__name__

        logger.info(f'开始执行: {func_name}')

        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            logger.info(f'执行完成: {func_name}, 耗时: {elapsed:.3f}秒')
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f'执行失败: {func_name}, 耗时: {elapsed:.3f}秒, 错误: {e}'
            )
            raise

    return wrapper


class Singleton[T]:
    """单例模式装饰器"""

    _instances: dict[type[object], object] = {}

    def __init__(self, cls: type[T]) -> None:
        self._cls: type[T] = cls

    def __call__(self, *args: object, **kwargs: object) -> T:
        if self._cls not in Singleton._instances:
            Singleton._instances[self._cls] = self._cls(*args, **kwargs)
        # 由于_instances存储的是object类型，需要显式转换为T类型
        from typing import cast
        return cast(T, Singleton._instances[self._cls])


def chunked(iterable: Iterable[T], size: int) -> Generator[list[T], None, None]:
    """
    将可迭代对象分块

    :param iterable: 可迭代对象
    :param size: 块大小
    :return: 块生成器
    """
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


def format_duration(seconds: float) -> str:
    """
    格式化时间间隔

    :param seconds: 秒数
    :return: 格式化字符串
    """
    if seconds < 60:
        return f'{seconds:.2f}秒'
    elif seconds < 3600:
        minutes = seconds / 60
        return f'{minutes:.2f}分钟'
    elif seconds < 86400:
        hours = seconds / 3600
        return f'{hours:.2f}小时'
    else:
        days = seconds / 86400
        return f'{days:.2f}天'


def generate_task_id() -> str:
    """生成唯一任务ID"""
    import uuid
    return str(uuid.uuid4())
