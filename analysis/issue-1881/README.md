# Daft Issue 1881 调查交接

调查对象：[Eventual-Inc/Daft #1881](https://github.com/Eventual-Inc/Daft/issues/1881) — Eliminate redundant HEAD requests when reading parquet。

## 先读什么

1. [完整报告](REPORT.md)：结论、实测 SHA、调用链、H1/H2 条件、查重及最小修改范围。
2. [完整计数表](RESULTS.md)：42 次未修改基线与独立插桩的阶段计数。
3. [构建与复现命令](BUILD_AND_RUN.md)：实际源码构建、依赖来源、服务配置和已保留的环境错误。
4. [英文评论草稿](COMMENT_DRAFT.md)：仅草稿，未发送。
5. [ChatGPT 交接提示](HANDOFF.md)：后续范围和持续约束。
6. [文件哈希清单](MANIFEST.json)：本目录材料的路径、字节数、SHA-256；不含清单自身。

## 已确认的核心事实

2026 年 10 月 9 日，在 upstream main `d3cd974150ba93dbbe8efef8370b129538fcee4b` 的源码 dev 构建、native runner 上实测：

- H1：文件发现/ScanSource 已掌握物理对象大小，但 reader 未复用。
- H2：只有 `IOConfig(disable_suffix_range=True)` 时，同一远程 reader 初始化出现两次成功 HEAD。HTTP eager reader 默认 1 HEAD，禁用 suffix 后 2 HEAD；两者均 4 GET、5189 响应 body bytes。
- 双文件 SeaweedFS S3-compatible glob 的 reader 执行阶段默认 2 HEAD、禁用 suffix 后 4 HEAD；两者均 8 GET、10410 body bytes。LIST/schema 阶段另计。
- 未修改基线：14 场景 × 3 全新进程，共 42 次全量读取、471 个请求。所有行列/null 校验通过。
- 独立日志插桩：16 次读取、172 个请求，与对应基线计数一致，IOStats 与服务端对齐。插桩已撤销。

## 原始材料的权威性

- [raw/baseline-final/](raw/baseline-final/) 是权威未修改基线：原始请求 JSONL、计数 CSV、42 份结果及进程日志。
- [raw/instrumented/](raw/instrumented/) 是独立日志插桩：16 份结果和分阶段归属审计，不能冒充未修改基线。
- `raw/baseline/`、`raw/baseline-r1/`、`raw/baseline-v1/` 是脚本开发时失败或不完整的运行，只用于保留过程错误。
- `raw/provenance-*.json`、`raw/build*.log`、`raw/verification.json` 保存来源、构建、结果核对证据。
- `raw/github-evidence.json`、`raw/final-search.json`、`raw/path-history.json` 等保存定向查重证据。宽泛历史查询超过一页，查重未完全穷尽。

脚本和日志共约 2.4 MiB。输入由 [generate_inputs.py](generate_inputs.py) 确定性生成，不提交 Parquet 输入、原生扩展、缓存或 Space 私有文件引用。脚本内固定测试值来自仓库本地模拟服务，不是环境秘密或真实云凭据。

## 当前交付状态和权限

本分支只增加调查材料，没有产品修复，没有修复后的请求数或性能对照，也没有因果补丁。日志插桩仅作为独立 `.patch` 保存，未应用。

REPORT.md 是原调查快照，其中“没有 push”的描述对应调查完成时。此后用户另行授权将材料提交到 `easyrev/Daft` 的新分支，便于 ChatGPT 读取；没有创建 PR、发送 issue 评论或认领。

证据由 Agent 自动核对，用户尚未人工复核。HTTP 与本地 S3-compatible 不代表真实 AWS 性能；请求减少不等于已证明端到端加速。

最值得下一步确认：维护者是否接受仅针对 `disable_suffix_range=True`、保持默认 footer GET/HEAD 并行与未知大小 fallback 的小范围 reader-local 贡献？

