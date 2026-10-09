# 本轮实际构建与复现命令

以下对应 `/workspace/daft-issue-1881`，upstream SHA `d3cd974150ba93dbbe8efef8370b129538fcee4b`。在其他环境应先按仓库 Development Guide 准备依赖，使用仓库指定 toolchain；不要用 Daft PyPI wheel 替代源码扩展。

## 1. 独立检出与本环境依赖复用

本轮执行了以下步骤。路径已存在时不要覆盖或 clean/reset，使用新的目录。

```bash
git clone --local --no-hardlinks --no-checkout /workspace/Daft /workspace/daft-issue-1881
cd /workspace/daft-issue-1881
git remote set-url origin https://github.com/Eventual-Inc/Daft.git
git fetch origin main
git checkout --detach FETCH_HEAD
git rev-parse HEAD
git status --short
```

读取 `AGENTS.md`、`CONTRIBUTING.md`、`docs/contributing/development.md`、Makefile、rust-toolchain.toml、.cargo/config.toml、Cargo.toml、pyproject.toml 和 `tests/integration/io` 的服务配置后，复用已有 toolchain/依赖，但构建产物与 Python 包保存在新 clone：

```bash
source /workspace/.cloud-setup/activate.sh
uv venv .venv --python /workspace/Daft/.venv/bin/python
.venv/bin/python - <<'PY'
import pathlib, sysconfig
site = pathlib.Path(sysconfig.get_paths()['purelib'])
(site / 'reused_dependencies.pth').write_text('/workspace/Daft/.venv/lib/python3.11/site-packages\n')
PY
cp -a --reflink=auto /workspace/Daft/target ./target
```

`.pth` 仅添加原依赖目录，不复用其 editable package 的 `.pth` 处理；实验设置 PYTHONPATH 指向本 clone，并逐进程核对包和扩展路径。旧 target 被复制，没有硬链接到原目录。Cargo cached dependency/toolchain 可复用，源码 crate 在新路径被重新编译。

本环境 activate.sh 设置 jobs=4、CARGO_INCREMENTAL=0、CARGO_PROFILE_DEV_DEBUG=0、DAFT_RUNNER=native、DAFT_DASHBOARD_SKIP_BUILD=1。继承代理和 CA；没有关闭 TLS 校验或绕过目的地策略。

## 2. 实际成功的源码构建

```bash
cd /workspace/daft-issue-1881
source /workspace/.cloud-setup/activate.sh
export VIRTUAL_ENV=/workspace/daft-issue-1881/.venv
export PYO3_PYTHON="$VIRTUAL_ENV/bin/python"
/workspace/Daft/.venv/bin/maturin develop --skip-install --locked > analysis/issue-1881/raw/build-baseline.log 2>&1
PYTHONPATH="$PWD" .venv/bin/python analysis/issue-1881/capture_provenance.py analysis/issue-1881/raw/provenance-recheck.json
```

使用现有 maturin 1.13.3 executable，不安装新版本。`--skip-install` 原位构建 native extension，不自动解析/升级可选 Python 依赖。新环境本身没有 maturin executable，`python -m maturin` 报 `Unable to find maturin script`，完整错误在 raw/build.log。

之前尝试 `develop --uv --locked` 会自动安装新任务 venv 的依赖/开发组；该流程被停止，任务 venv 中自动安装的包被清除，只保留原依赖路径，随后按上述命令重建。该尝试完整日志在 raw/build-retry.log，最终实际使用 Python 3.11.16、PyArrow 22.0.0 和原 toolchain。没有 Rust build failure 被版本升级掩盖。

## 3. 仓库服务与完整基线矩阵

只启动仓库的 SeaweedFS 服务，使用独立 Compose project。无需整个测试集群。

```bash
env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH \
  docker --host=unix:///var/run/docker.sock compose -p daft-issue1881 \
  -f tests/integration/io/docker-compose/docker-compose.yml up -d seaweedfs
.venv/bin/python analysis/issue-1881/run_matrix.py --label my-recheck --repeats 3
```

本轮权威结果的 label 是 `baseline-final`。新的 label 必须尚不存在，防止覆盖原始数据。matrix 自动生成被 gitignore 的 generated/ 输入，用仓库公开的本地服务测试凭据上传到 `public-bucket-issue1881`。Daft 使用 anonymous S3Config，不需要云账户或真实 credentials。

- SeaweedFS：127.0.0.1:9000，仓库 pinned 4.47 image/digest，见 raw/service-image.txt。
- 记录代理 + HTTP Range server：127.0.0.1:9001，matrix 自动起停。
- HTTP 输入：`http://127.0.0.1:9001/http/part-0.parquet`。
- S3 输入：`s3://public-bucket-issue1881/data/part-*.parquet`，endpoint_url 为 9001。
- baseline 矩阵：HTTP 单 URI/双 URI；S3 单 URI/glob；S3 双 URI + infer_schema=False；HTTP eager MicroPartition；HTTP 大 footer，均跑默认/显式禁用 suffix。

每个 case 起新 Python process。constructor/collection 的服务端阶段由独立 control 请求切换；control 和上传等 setup 不计入数据请求。`run_case.py` 验证 import path/hash，调用实际 native reader，并把完整数据与 PyArrow 独立读回结果核对。所有 `.json` 结果含全数据，不只检查行数。

复现脚本开发时，bucket 创建权限、旧模块 import 路径、IOConfig getter 的错误已修正；原始失败 log 保留在 raw 下，部分 label 不作为权威结果。脚本并未针对产品实现改公共 API。

## 4. 独立日志插桩（可选）

产品源码必须在未修改基线验证后才应用该补丁；它只增加日志，不改变 reader 行为。本轮按以下方式备份/重建/运行，结果保存于 raw/instrumented/。

```bash
cp --reflink=auto daft/daft.abi3.so target/issue1881-baseline.so
git apply --check analysis/issue-1881/instrumentation.patch
git apply analysis/issue-1881/instrumentation.patch
/workspace/Daft/.venv/bin/maturin develop --skip-install --locked > analysis/issue-1881/raw/build-instrumented.log 2>&1
PYTHONPATH="$PWD" .venv/bin/python analysis/issue-1881/capture_provenance.py analysis/issue-1881/raw/provenance-instrumented-recheck.json
.venv/bin/python analysis/issue-1881/run_matrix.py --label my-instrumented-recheck --audit --repeats 2
```

日志补丁使 IOStats drop 时的计数可见，记录 schema discovery 的物理 size 和真正执行的 ScanSource size。IOStats 仅用于定位；网络 HEAD/LIST/GET 仍以服务端实际请求为准。

本轮恢复命令如下，只逆转本任务的插桩，不使用 git reset/clean：

```bash
git apply --check --reverse analysis/issue-1881/instrumentation.patch
git apply --reverse analysis/issue-1881/instrumentation.patch
cp --reflink=auto target/issue1881-baseline.so daft/daft.abi3.so
cp --reflink=auto target/issue1881-baseline.so target/debug/libdaft.so
PYTHONPATH="$PWD" .venv/bin/python analysis/issue-1881/capture_provenance.py analysis/issue-1881/raw/provenance-restored-recheck.json
```

也可恢复源码后按第2节重新构建；不要复制别的工作区的旧扩展当成当前源码产物。本轮原始 baseline build hash 与恢复后的 hash 完全相同。

## 5. 请求/结果复核与停止服务

```bash
.venv/bin/python analysis/issue-1881/analyze_results.py
```

该脚本读取本轮的 baseline-final 与 instrumented 原始日志：断言所有42次基线同场景计数一致，16次插桩与对应基线一致，IOStats分阶段总计与真实网络请求一致，且没有失败状态/response write error。结果写到 RESULTS.md、raw/instrumented/stages.csv、raw/verification.json。复核自己新的 label 时可复制该脚本并修改 label 路径，避免覆盖本轮材料。

运行新实验前应重新确认当前 SHA、未提交产品改动和 `.so` hash。时间记录仅供审计；本轮未将 dev-build 单次耗时作为 benchmark。

本轮最后只停止本任务启动的 Compose project：

```bash
env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH \
  docker --host=unix:///var/run/docker.sock compose -p daft-issue1881 \
  -f tests/integration/io/docker-compose/docker-compose.yml down --volumes
```
