# -*- coding: utf-8 -*-
"""
Redis连接管理模块

提供Redis连接的创建和管理功能。
"""

from __future__ import annotations

from redis import Redis
import redis as redis_module
from redis import ConnectionError as RedisConnectionError


class RedisConnection:
    """Redis连接管理类"""

    _instance: RedisConnection | None = None
    _client: Redis | None = None  # type: ignore[type-arg]

    def __new__(
        cls,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        decode_responses: bool = True,
    ) -> RedisConnection:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        decode_responses: bool = True,
    ) -> None:
        """
        初始化Redis连接

        :param host: Redis服务器地址
        :param port: Redis服务器端口
        :param db: 数据库编号
        :param password: 密码
        :param decode_responses: 是否解码响应
        """
        if self._client is None:
            self._client = redis_module.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=decode_responses,
            )

    @property
    def client(self) -> Redis:  # type: ignore[type-arg]
        """获取Redis客户端"""
        if self._client is None:
            raise RuntimeError('Redis连接未初始化')
        return self._client

    @classmethod
    def get_client(cls) -> Redis:  # type: ignore[type-arg]
        """获取Redis客户端（类方法）"""
        if cls._instance is None or cls._instance._client is None:
            raise RuntimeError('Redis连接未初始化，请先创建RedisConnection实例')
        return cls._instance._client

    @classmethod
    def reset(cls) -> None:
        """重置连接（主要用于测试）"""
        if cls._instance is not None and cls._instance._client is not None:
            cls._instance._client.close()
        cls._instance = None
        cls._client = None

    def ping(self) -> bool:
        """测试连接"""
        if self._client is None:
            return False
        try:
            return bool(self._client.ping())
        except RedisConnectionError:
            return False

    def close(self) -> None:
        """关闭连接"""
        if self._client is not None:
            self._client.close()
            RedisConnection._client = None
            RedisConnection._instance = None
