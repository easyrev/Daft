# 未修改基线请求计数

14 个场景，每个 3 个全新进程；42 次完整读取均通过。各场景三次的所有计数完全一致。

bytes 为服务端成功写出的响应 body 字节，包含 LIST XML，不含 HTTP headers/TCP/TLS；不等于 Daft 消费字节。

configuration 中 suffixon=默认 IOConfig；suffixoff=显式 disable_suffix_range=True。schema given=现有 infer_schema=False 入口；direct=带全匹配 predicate 的 eager MicroPartition 入口。

construction=DataFrame 构造（发现+schema/路径检查）；collection=惰性文件发现+实际读取；reader=无 glob/ScanTask 的直接 eager reader。

| 输入/backend/configuration | 大小信息 | 阶段 | HEAD | LIST | GET | bytes | 校验 |
|---|---|---|---:|---:|---:|---:|---|
| http-direct-suffixoff-schemainfer | 直接 reader 无已知大小 | reader | 2 | 0 | 4 | 5189 | 完整 96/192 行通过 |
| http-direct-suffixon-schemainfer | 直接 reader 无已知大小 | reader | 1 | 0 | 4 | 5189 | 完整 96/192 行通过 |
| http-largefooter-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 3 | 0 | 5 | 353367 | 完整 96/192 行通过 |
| http-largefooter-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 2 | 0 | 2 | 351462 | 完整 96/192 行通过 |
| http-largefooter-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 2 | 0 | 5 | 484439 | 完整 96/192 行通过 |
| http-largefooter-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 1 | 0 | 2 | 482534 | 完整 96/192 行通过 |
| http-multi-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 6 | 0 | 8 | 10410 | 完整 96/192 行通过 |
| http-multi-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 2 | 0 | 1 | 3284 | 完整 96/192 行通过 |
| http-multi-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 4 | 0 | 8 | 10410 | 完整 96/192 行通过 |
| http-multi-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 1 | 0 | 1 | 3284 | 完整 96/192 行通过 |
| http-single-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 3 | 0 | 4 | 5189 | 完整 96/192 行通过 |
| http-single-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 2 | 0 | 1 | 3284 | 完整 96/192 行通过 |
| http-single-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 2 | 0 | 4 | 5189 | 完整 96/192 行通过 |
| http-single-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 1 | 0 | 1 | 3284 | 完整 96/192 行通过 |
| s3-glob-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 4 | 1 | 8 | 11328 | 完整 96/192 行通过 |
| s3-glob-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 1 | 1 | 1 | 4202 | 完整 96/192 行通过 |
| s3-glob-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 2 | 1 | 8 | 11328 | 完整 96/192 行通过 |
| s3-glob-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 0 | 1 | 1 | 4202 | 完整 96/192 行通过 |
| s3-multi-suffixoff-schemagiven | 发现得到物理大小；reader 入口仍未知 | collection | 6 | 0 | 8 | 10410 | 完整 96/192 行通过 |
| s3-multi-suffixoff-schemagiven | 发现得到物理大小；reader 入口仍未知 | construction | 1 | 0 | 0 | 0 | 完整 96/192 行通过 |
| s3-multi-suffixon-schemagiven | 发现得到物理大小；reader 入口仍未知 | collection | 4 | 0 | 8 | 10410 | 完整 96/192 行通过 |
| s3-multi-suffixon-schemagiven | 发现得到物理大小；reader 入口仍未知 | construction | 1 | 0 | 0 | 0 | 完整 96/192 行通过 |
| s3-single-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 3 | 0 | 4 | 5189 | 完整 96/192 行通过 |
| s3-single-suffixoff-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 2 | 0 | 1 | 3284 | 完整 96/192 行通过 |
| s3-single-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | collection | 2 | 0 | 4 | 5189 | 完整 96/192 行通过 |
| s3-single-suffixon-schemainfer | 发现得到物理大小；reader 入口仍未知 | construction | 1 | 0 | 1 | 3284 | 完整 96/192 行通过 |

# 分阶段归属审计

16 次独立日志插桩运行；28 个 API 边界计数行与对应未修改基线完全一致。IOStats 只辅助归属，HEAD/LIST/GET 总数由实际服务端记录核对。

原始日志中请求归属仍保留 construction/collection 边界。下表根据互相独立的发现/推断/执行 IOStats context 拆分计数；使用前述受限矩阵的实际 Range 请求与 LIST body 归属字节，没有将逻辑 get_size 次数转换成 HEAD。

| 场景 | 发现/ScanSource 物理大小 | reader 入参大小 | 阶段 | HEAD | LIST | GET | bytes | 校验 |
|---|---|---|---|---:|---:|---:|---:|---|
| http-direct-suffixoff-schemainfer | not provided (direct reader) | unknown on entry | reader execution | 2 | 0 | 4 | 5189 | 完整数据通过 |
| http-direct-suffixon-schemainfer | not provided (direct reader) | unknown on entry | reader execution | 1 | 0 | 4 | 5189 | 完整数据通过 |
| http-single-suffixoff-schemainfer | 3284 | unknown on entry | schema inference | 1 | 0 | 1 | 3284 | 完整数据通过 |
| http-single-suffixoff-schemainfer | 3284 | unknown on entry | discovery during construction | 1 | 0 | 0 | 0 | 完整数据通过 |
| http-single-suffixoff-schemainfer | 3284 | unknown on entry | discovery during collection | 1 | 0 | 0 | 0 | 完整数据通过 |
| http-single-suffixoff-schemainfer | 3284 | unknown on entry | reader execution | 2 | 0 | 4 | 5189 | 完整数据通过 |
| http-single-suffixon-schemainfer | 3284 | unknown on entry | schema inference | 0 | 0 | 1 | 3284 | 完整数据通过 |
| http-single-suffixon-schemainfer | 3284 | unknown on entry | discovery during construction | 1 | 0 | 0 | 0 | 完整数据通过 |
| http-single-suffixon-schemainfer | 3284 | unknown on entry | discovery during collection | 1 | 0 | 0 | 0 | 完整数据通过 |
| http-single-suffixon-schemainfer | 3284 | unknown on entry | reader execution | 1 | 0 | 4 | 5189 | 完整数据通过 |
| s3-glob-suffixoff-schemainfer | 3300/3284 | unknown on entry | schema inference | 1 | 0 | 1 | 3284 | 完整数据通过 |
| s3-glob-suffixoff-schemainfer | 3300/3284 | unknown on entry | discovery during construction | 0 | 1 | 0 | 918 | 完整数据通过 |
| s3-glob-suffixoff-schemainfer | 3300/3284 | unknown on entry | discovery during collection | 0 | 1 | 0 | 918 | 完整数据通过 |
| s3-glob-suffixoff-schemainfer | 3300/3284 | unknown on entry | reader execution | 4 | 0 | 8 | 10410 | 完整数据通过 |
| s3-glob-suffixon-schemainfer | 3284/3300 | unknown on entry | schema inference | 0 | 0 | 1 | 3284 | 完整数据通过 |
| s3-glob-suffixon-schemainfer | 3284/3300 | unknown on entry | discovery during construction | 0 | 1 | 0 | 918 | 完整数据通过 |
| s3-glob-suffixon-schemainfer | 3284/3300 | unknown on entry | discovery during collection | 0 | 1 | 0 | 918 | 完整数据通过 |
| s3-glob-suffixon-schemainfer | 3284/3300 | unknown on entry | reader execution | 2 | 0 | 8 | 10410 | 完整数据通过 |
| s3-multi-suffixoff-schemagiven | 3284/3300 | unknown on entry | discovery during construction | 1 | 0 | 0 | 0 | 完整数据通过 |
| s3-multi-suffixoff-schemagiven | 3284/3300 | unknown on entry | discovery during collection | 2 | 0 | 0 | 0 | 完整数据通过 |
| s3-multi-suffixoff-schemagiven | 3284/3300 | unknown on entry | reader execution | 4 | 0 | 8 | 10410 | 完整数据通过 |
| s3-multi-suffixon-schemagiven | 3284/3300 | unknown on entry | discovery during construction | 1 | 0 | 0 | 0 | 完整数据通过 |
| s3-multi-suffixon-schemagiven | 3284/3300 | unknown on entry | discovery during collection | 2 | 0 | 0 | 0 | 完整数据通过 |
| s3-multi-suffixon-schemagiven | 3284/3300 | unknown on entry | reader execution | 2 | 0 | 8 | 10410 | 完整数据通过 |
