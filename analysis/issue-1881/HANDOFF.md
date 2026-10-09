# 交给 ChatGPT 的提示

请接手本分支的 Daft Issue #1881 调查。先阅读同目录 README.md、REPORT.md、RESULTS.md、BUILD_AND_RUN.md、COMMENT_DRAFT.md；需要核对时查看 raw/baseline-final、raw/instrumented、provenance 和 GitHub 查重材料。

原实测 upstream SHA 是 d3cd974150ba93dbbe8efef8370b129538fcee4b，实测日期是 2026 年 10 月 9 日。使用源码 dev 构建和 native runner，包路径、原生扩展路径与 Cargo 产物哈希均已核对。后续实施先更新 upstream 和相关 PR 的定向查重，不把旧 SHA 的结论直接当作最新 main 结论。

请保持以下区别：H1/H2、未修改基线/日志插桩、发现/schema/执行阶段、内部 get_size 调用/实际网络请求、HTTP/S3-compatible/AWS，以及事实/推断/未覆盖项。宽泛历史查重尚未完全穷尽。

我的目标是在深入 #7458 前完成一到两个适度规模贡献。优先判断只对 disable_suffix_range=True 共享同一次 reader 初始化大小查询的 H2 方案是否值得继续，给出最小范围、错误传播/并发/取消语义、大 footer/小文件回归要求，以及维护者最值得决定的一件事。H1 跨扫描层传递整文件物理大小、schema/footer metadata 复用分别评估；不要扩展成全局 metadata cache、Parquet reader 重构或扫描 redesign。

H1 已确认；H2 仅禁用 suffix 时确认。默认 reader 只有一个 HEAD。ScanSource.size_bytes 在拆分后可能是选中 row groups 的压缩字节和，不能无条件当作整文件物理大小。保留 file_len 契约、未知大小 fallback 和现有 footer 验证。没有修复或修复后性能证据，不外推真实 AWS 延迟或训练吞吐。

已有未修改基线验证；日志插桩单独保存并已撤销。后续只有关键不确定性需要时，才用极小、可撤销、与基线分开的因果对照补丁。不改公共 API 方便测试，不升级依赖/Rust toolchain 掩盖环境问题，不改我已有的工作，不用破坏性 reset/clean。

原调查禁止 GitHub 写入；随后我单独授权把这些材料提交到 easyrev/Daft 的新分支。此授权只用于本次材料交付，不自动授权后续评论/认领/assign/创建 issue/PR/继续 push。英文评论草稿不要发送。所有检查是 Agent 自动完成，我尚未人工复核。

