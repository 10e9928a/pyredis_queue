# PyRedis Queue

基于 Redis 的任务分发 Python 库，支持任务队列、优先级、延迟执行、失败重试、定时任务等功能。

## 特性

- 🚀 **高性能**: 基于 Redis 实现，支持高并发场景
- 📊 **优先级队列**: 支持任务优先级排序，高优先级任务优先执行
- ⏰ **延迟任务**: 支持延迟执行，任务可以在指定时间后执行
- 🔄 **失败重试**: 支持自动重试机制，可配置重试次数
- 📅 **定时任务**: 支持周期性任务和 Cron 表达式
- 🎛️ **速率限制**: 内置令牌桶算法实现的速率限制器
- 🔒 **分布式支持**: 支持多 Worker 分布式执行
- 📝 **任务状态追踪**: 完整的任务生命周期管理

## 安装

```bash
pip install pyredis_queue
```

或者从源码安装：

```bash
git clone https://github.com/10e9928a/pyredis_queue.git
cd pyredis_queue
pip install -e .
```

## 快速开始

### 1. 基础使用

```python
from pyredis_queue import (
    RedisConnection,
    Task,
    TaskQueue,
    Worker,
    TaskPriority,
)
from pyredis_queue.task import task_handler

# 初始化 Redis 连接
conn = RedisConnection(host='localhost', port=6379, db=0)

# 创建任务队列
queue = TaskQueue(queue_name='my_queue')

# 注册任务处理器
@task_handler('send_email')
def send_email(to: str, subject: str, body: str):
    print(f'发送邮件到 {to}: {subject}')
    return {'status': 'sent'}

# 创建并提交任务
task = Task(
    name='send_email',
    payload={
        'to': 'user@example.com',
        'subject': '测试邮件',
        'body': '这是一封测试邮件'
    },
    priority=TaskPriority.HIGH
)
queue.enqueue(task)

# 创建 Worker 执行任务
worker = Worker(queue_name='my_queue', concurrency=4)
worker.start()
```

### 2. 延迟任务

```python
# 延迟 60 秒执行
task = Task(name='delayed_task', payload={'message': 'Hello'})
queue.enqueue(task, delay=60)
```

### 3. 优先级队列

```python
from pyredis_queue import TaskPriority

# 创建高优先级任务
high_priority_task = Task(
    name='important_task',
    payload={'data': 'urgent'},
    priority=TaskPriority.CRITICAL  # 优先级：LOW=1, NORMAL=5, HIGH=10, CRITICAL=100
)
queue.enqueue(high_priority_task)
```

### 4. 失败重试

```python
# 创建支持重试的任务
task = Task(
    name='unreliable_task',
    payload={'data': 'test'},
    max_retries=3,  # 最多重试 3 次
    timeout=300     # 超时时间 5 分钟
)
queue.enqueue(task)
```

### 5. 定时任务

```python
from pyredis_queue import TaskScheduler
from pyredis_queue.scheduler import ScheduledJob

# 创建调度器
scheduler = TaskScheduler(queue_name='scheduled')

# 添加周期性任务（每 60 秒执行一次）
job = ScheduledJob(
    name='heartbeat',
    task_name='heartbeat_task',
    interval=60,
    run_immediately=True
)
scheduler.add_job(job)

# 使用 Cron 表达式（每天凌晨 2 点执行）
cron_job = ScheduledJob(
    name='daily_cleanup',
    task_name='cleanup_task',
    cron='0 2 * * *'
)
scheduler.add_job(cron_job)

scheduler.start(daemon=True)
```

### 6. Worker 池

```python
from pyredis_queue.worker import WorkerPool

# 创建 Worker 池，处理多个队列
pool = WorkerPool(
    queue_names=['queue_a', 'queue_b', 'queue_c'],
    workers_per_queue=2,
    concurrency=4
)
pool.start()
```

### 7. 速率限制

```python
from pyredis_queue.scheduler import RateLimiter

# 创建速率限制器：每秒 10 个请求，最多积累 50 个令牌
limiter = RateLimiter(
    name='api_limiter',
    rate=10,
    capacity=50
)

# 获取令牌
if limiter.acquire(tokens=1, block=True):
    # 执行操作
    pass
```

## API 参考

### Task 类

| 属性 | 类型 | 说明 |
|------|------|------|
| `name` | str | 任务名称 |
| `payload` | dict | 任务数据 |
| `task_id` | str | 任务 ID（自动生成） |
| `priority` | int | 优先级 |
| `max_retries` | int | 最大重试次数 |
| `timeout` | int | 超时时间（秒） |
| `status` | str | 任务状态 |

### TaskQueue 类

| 方法 | 说明 |
|------|------|
| `enqueue(task, delay=0)` | 将任务加入队列 |
| `dequeue(timeout=0)` | 从队列取出任务 |
| `complete(task, result)` | 标记任务完成 |
| `fail(task, error)` | 标记任务失败 |
| `cancel(task_id)` | 取消任务 |
| `get_task(task_id)` | 获取任务信息 |
| `get_queue_stats()` | 获取队列统计 |

### Worker 类

| 方法 | 说明 |
|------|------|
| `start(daemon=False)` | 启动 Worker |
| `stop(wait=True)` | 停止 Worker |
| `on_task_start(callback)` | 设置任务开始回调 |
| `on_task_success(callback)` | 设置任务成功回调 |
| `on_task_failure(callback)` | 设置任务失败回调 |

## 配置

### Redis 连接配置

```python
conn = RedisConnection(
    host='localhost',      # Redis 地址
    port=6379,             # Redis 端口
    db=0,                  # 数据库编号
    password='your_pass',  # 密码（可选）
    decode_responses=True  # 解码响应
)
```

### Worker 配置

```python
worker = Worker(
    queue_name='default',   # 队列名称
    concurrency=4,          # 并发数
    poll_interval=1         # 轮询间隔（秒）
)
```

## 最佳实践

1. **合理设置优先级**: 避免所有任务都使用高优先级
2. **设置合理的超时时间**: 防止任务长时间阻塞
3. **使用延迟任务代替轮询**: 减少资源消耗
4. **监控队列状态**: 及时发现积压问题
5. **处理死信队列**: 定期检查并处理失败的任务

## 运行示例

```bash
# 安装依赖
pip install -r requirements.txt

# 运行示例
python examples/basic_usage.py
```

## 开发

```bash
# 安装开发依赖
pip install -e ".[dev]"

# 运行测试
pytest

# 代码格式化
black pyredis_queue
isort pyredis_queue

# 类型检查
mypy pyredis_queue
```

## 许可证

MIT License
