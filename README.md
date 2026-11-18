# RPA流程自动化中心

面向企业数据采集场景，提供RPA流程编排、元素定位、执行调度和结果校验的自动化运营平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流程定义 | `bot_definition` | RPA流程 | 流程名称、目标系统、触发方式 |
| 步骤编排 | `step_editor` | 流程步骤 | 步骤序号、步骤名称、操作类型 |
| 元素定位 | `element_selector` | 定位规则 | 规则名称、选择器类型、定位表达式 |
| 触发配置 | `trigger_config` | 触发器 | 触发器名称、触发类型、调度表达式 |
| 执行记录 | `execution_log` | 执行日志 | 执行编号、所属流程、开始时间 |
| 数据提取 | `data_extract` | 提取规则 | 规则名称、源页面结构、提取字段 |
| 凭据管理 | `credential_store` | 系统凭据 | 凭据名称、目标系统、认证方式 |
| 环境配置 | `env_profile` | 执行环境 | 环境名称、浏览器版本、操作系统 |
| 反封锁 | `anti_block` | 防护策略 | 策略名称、目标站点、请求间隔 |
| 结果校验 | `result_verify` | 校验规则 | 规则名称、校验字段、校验条件 |
| 批次追踪 | `batch_tracker` | 采集批次 | 批次编号、触发流程、采集时间 |
| 页面建模 | `page_model` | 页面模型 | 模型名称、目标站点、页面结构 |
| 数据补全 | `data_enrich` | 补全规则 | 规则名称、目标字段、补全来源 |
| 代理路由 | `proxy_router` | 代理节点 | 节点名称、IP地址、出口地域 |
| 频控策略 | `rate_limiter` | 频控规则 | 规则名称、目标站点、每秒上限 |
| 输出格式 | `output_format` | 格式模板 | 模板名称、输出类型、字段映射 |
| 异常日志 | `anomaly_log` | 异常事件 | 事件编号、关联流程、异常类型 |
| 调度计划 | `scheduler` | 采集计划 | 计划名称、绑定流程、cron表达式 |
| 运行概览 | `report_dashboard` | 运行数据 | 统计日期、总执行数、成功数 |
| 任务队列 | `task_queue` | 排队任务 | 任务编号、流程名称、提交时间 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 分页续做（页面建模 × 数据提取）

页面采集按「站点 + 页面模型」持久化分页游标与已见内容指纹，重开页面不会
从首页重复累计同一行数据。

- 游标与指纹同存：`PageCursorStore`（`app/store.py`）统一保存
  `covered_pages` / `next_page` / `seen` / `page_versions`，可 `save()` / `load()`
  落盘恢复，恢复时只合并向前边界，不回退。
- 源端配置快照：`PageModelService.build_source_snapshot()` 输出带
  `pagination_version`（由站点、结构、分页模式、每页条数、边界策略、修订号
  决定的配置指纹）、剩余页边界和已见内容数量；`publish_source_config()` 带
  版本发布到 `rpa.source_configured`。
- 处理项边界：`DataExtractService.consume_source_and_extract()` 先核对快照
  版本，再只消费未覆盖页；同一 `handle_ref` 重复或并发提交只算一次，
  中断原因写入 `FailureRecovery` 与处理项台账，重试沿既有边界继续。
- 两套边界：`content_first`（同页内容变化按指纹追加版本）与 `page_first`
  （挂起内容变化，等页码推进后整页重抓）；反复冲突落到页面源配置的处理终态
  `结构失效` 并记录原因。
- 工作台：`/page_model` 页内的「分页续做工作台」按站点展示分页状态、已覆盖
  页和已见内容数量，支持「继续未覆盖页」、保存增量字段映射明细与版本核对。

主要接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/page_model/resume` | 按站点查看分页状态、已覆盖页、已见内容数 |
| POST | `/api/page_model/{id}/pagination` | 保存分页配置（不改动续做边界） |
| POST | `/api/page_model/{id}/resume` | 继续未覆盖页（可带 `handle_ref`、`max_pages`） |
| GET/POST | `/api/page_model/mappings` | 读取/保存增量字段映射明细 |
| GET | `/api/data_extract/summary` | 采集与分页统计 |
| GET | `/api/data_extract/duplicate-check` | 核对重开页面是否重复累计同一行 |

领域：数据工程
子方向：数据采集、爬虫与RPA
