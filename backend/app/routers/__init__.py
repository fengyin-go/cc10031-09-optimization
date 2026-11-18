"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

页面建模工作台的分页续做接口先注册，避免被 /api/{module}/{entry_id}
这样的通配路径抢先匹配。
"""
from __future__ import annotations

from app.routers import page_workbench, resource

ROUTERS = [page_workbench, resource]
