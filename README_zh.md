# go-xxljob

[![CI](https://github.com/ShawineWu/go-xxljob/actions/workflows/ci.yml/badge.svg)](https://github.com/ShawineWu/go-xxljob/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ShawineWu/go-xxljob.svg)](https://pkg.go.dev/github.com/ShawineWu/go-xxljob)
[![Go Report Card](https://goreportcard.com/badge/github.com/ShawineWu/go-xxljob)](https://goreportcard.com/report/github.com/ShawineWu/go-xxljob)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[English](README.md) | 简体中文

健壮、完整的 [XXL-JOB](https://github.com/xuxueli/xxl-job) Go 执行器。

XXL-JOB 是广泛使用的分布式任务调度平台：**调度中心**决定任务什么时候执行，**执行器**负责执行。本库是 Go 服务的执行器。

- **支持调度中心 2.3.0 ~ 3.5.x**，两代协议，改一个配置即可切换。
- **失败就是失败**：任务返回 `error`，调度中心显示失败并触发告警和失败重试；panic 也会被捕获并按失败回报。
- **同一任务永远不会并发执行**。三种阻塞策略（单机串行 / 丢弃后续调度 / 覆盖之前调度）与官方 Java 执行器一致，终止后也成立。
- **功能完整**：终止、超时、调度中心可查看的执行日志、分片广播、结果回调失败重试、优雅退出。
- **默认安全**：每个请求校验 AccessToken，限制请求体大小，所有连接都有超时。
- **易于测试**：`xxljob.Invoke` 不需要调度中心，按真实执行的方式跑一次任务。
- **零依赖**：只依赖 Go 标准库。

## 目录

- [安装](#安装)
- [快速开始](#快速开始)
- [编写任务](#编写任务)
- [调度中心版本](#调度中心版本)
- [配置](#配置)
- [部署](#部署)
- [进阶用法](#进阶用法)
- [设计](#设计)
- [测试](#测试)
- [参与贡献](#参与贡献) · [许可证](#许可证)

## 安装

```bash
go get github.com/ShawineWu/go-xxljob
```

需要 Go 1.24 及以上。

## 快速开始

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ShawineWu/go-xxljob"
)

func main() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AccessToken:    "default_token",
		AppName:        "demo-executor", // 调度中心「执行器管理」里的 AppName
	})
	if err != nil {
		panic(err)
	}

	exec.MustRegister("Hello", xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
		xxljob.Logger(ctx).Info("hello", "params", req.Params) // 调度中心「执行日志」里可见
		return nil
	}))

	if err := exec.Start(); err != nil { // 监听 :9999 并注册到调度中心
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = exec.Shutdown(shutdownCtx) // 等执行中的任务结束，再注销
}
```

然后在调度中心：

1. **执行器管理** → 新增执行器，AppName 填 `demo-executor`，注册方式选自动注册。
2. **任务管理** → 为该执行器新增任务，运行模式 **BEAN**，JobHandler 填 `Hello`。

更完整的程序见 [`examples/`](examples)：一个带多个任务的独立执行器，以及一个挂在已有 HTTP 服务上的执行器。

## 编写任务

### 一个任务一个类型

任务较多的服务，建议每个任务一个类型。`Name` 返回调度中心里配置的 JobHandler，依赖通过构造函数传入：

```go
type SyncItemsTask struct{ db *sql.DB }

func NewSyncItemsTask(db *sql.DB) *SyncItemsTask { return &SyncItemsTask{db: db} }

// Name 即调度中心里配置的 JobHandler。
func (t *SyncItemsTask) Name() string { return "SyncItems" }

func (t *SyncItemsTask) Handle(ctx context.Context, req *xxljob.Request) error {
	p := struct {
		BatchSize int `json:"batchSize"`
	}{BatchSize: 500} // 调度中心没填任务参数时保留默认值
	if err := req.BindJSON(&p); err != nil {
		return err // 返回 error 即本次调度失败
	}

	for _, batch := range batches {
		if err := ctx.Err(); err != nil {
			return err // 超时、被终止、执行器退出时 ctx 会被取消
		}
		xxljob.Logger(ctx).Info("synced batch", "size", len(batch))
	}

	xxljob.SetResult(ctx, "synced 1200 items") // 成功时的执行备注（可选）
	return nil
}
```

一次注册多个任务；只要有一个名称为空或重复，就一个都不注册：

```go
exec.MustRegisterTasks(
	NewSyncItemsTask(db),
	NewDailyReportTask(db, mailer),
)
```

### 函数式任务

简单的任务可以直接注册函数，`HandleJSON` 帮你解析参数：

```go
exec.MustRegister("Cleanup", xxljob.HandleJSON(func(ctx context.Context, req *xxljob.Request, p struct{ Days int }) error {
	...
}))
```

### 任务能拿到什么

| | |
|---|---|
| `req.Params`、`req.BindJSON(&v)` | 调度中心配置的任务参数 |
| `req.ShardIndex`、`req.ShardTotal` | 分片广播：本实例的分片序号（从 0 开始）和分片总数 |
| `req.JobID`、`req.LogID`、`req.Timeout` 等 | 本次调度的元数据 |
| `xxljob.Logger(ctx)` | `*slog.Logger`，同时写入调度中心的执行日志**和**应用日志 |
| `xxljob.SetResult(ctx, msg)` | 成功时回报的执行备注 |
| `ctx` | 超时、终止、覆盖之前调度、执行器退出时被取消，`context.Cause(ctx)` 可以区分原因 |

耗时任务必须检查 `ctx`。任务返回之前，同一任务后续的调度只会排队或被丢弃；不检查 `ctx` 的任务无法被终止。

### 单元测试

`xxljob.Invoke` 不需要调度中心，直接执行一次任务，日志、`SetResult`、超时、panic 的行为和真实执行完全一致：

```go
func TestSyncItemsTask(t *testing.T) {
	res := xxljob.Invoke(context.Background(), NewSyncItemsTask(db), &xxljob.Request{Params: `{"batchSize":10}`})

	require.NoError(t, res.Err)
	assert.Equal(t, "synced 1200 items", res.Msg)
	t.Log(strings.Join(res.Log, "\n")) // 调度中心「执行日志」里会看到的内容
}
```

`Invoke` 也适合用来写「手动执行任务」的后台接口或运维命令。

## 调度中心版本

XXL-JOB 3.5.0 修改了执行器协议，与旧版本不兼容，所以要按调度中心的版本设置 `AdminVersion`：

| 调度中心版本 | `AdminVersion` | `AccessToken` |
|---|---|---|
| 2.3.0 ~ 3.4.x | 不用设置（默认 `"2"`） | 调度中心的全局 token（`xxl.job.accessToken`），可以不配 |
| 3.5.0 及以上 | `"3.5"` | 「执行器管理」里**该执行器**的 token，**必填** |

调度中心升级到 3.5 时只需改这两项，任务代码不用动。另外调度中心从 3.4.1 起去掉了 `/xxl-job-admin` 路径前缀，`AdminAddresses` 也要相应调整。

## 配置

只有 `AdminAddresses` 和 `AppName` 是必填的。

| 字段 | 默认值 | 说明 |
|---|---|---|
| `AdminAddresses` | 必填 | 调度中心地址；配置多个时依次尝试 |
| `AppName` | 必填 | 执行器 AppName |
| `AdminVersion` | `"2"` | `"2"`：调度中心 2.3.0 ~ 3.4.x；`"3.5"`：3.5.0+ |
| `AccessToken` | 空 | 见[调度中心版本](#调度中心版本) |
| `IP` | `$POD_IP`，否则自动探测 | 注册给调度中心的 IP |
| `Port` | `9999` | 执行器端口 |
| `Address` | `http://{IP}:{Port}/` | 注册给调度中心的完整地址（经过 NAT、挂在子路径时设置） |
| `DefaultTimeout` | `0`（不限） | 调度中心没设超时的任务的兜底超时 |
| `MaxConcurrency` | `0`（不限） | 所有任务合计的并发上限，超出的等待空位 |
| `MaxQueueSize` | `1000` | 单个任务最多排队多少次调度（单机串行） |
| `LongRunningThreshold` | `1h` | 单次执行超过这个时长记一条 Warn |
| `RegistryInterval` | `30s` | 注册心跳间隔 |
| `RequestTimeout` | `3s` | 发往调度中心的单个请求的超时 |
| `CallbackRetryInterval` | `10s` | 结果回调失败后的重试间隔 |
| `MaxPendingCallbacks` | `10000` | 待发送结果的上限，超出时丢弃最旧的 |
| `CancelWait` | `3s` | `Shutdown` 取消任务后再等多久 |

传给 `New` 的选项：

| 选项 | 默认值 | |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | 执行器自身日志，以及任务日志的应用日志部分 |
| `WithLogStore(LogStore)` | 内存，16 MB | 执行日志的存储 |
| `WithMiddleware(...)` | 无 | 对所有任务生效的中间件 |
| `WithHTTPClient(*http.Client)` | `&http.Client{}` | 访问调度中心的 HTTP 客户端 |
| `WithoutServer()` | 关 | 不监听端口，自行挂载 `exec.Handler()` |

## 部署

### 优雅退出

`Shutdown(ctx)` 会拒绝新的调度、注销、等执行中的任务结束直到 `ctx` 超时、取消剩下的任务，并发出剩余的结果。要保证运行平台给足时间：

```
Shutdown 的 ctx 超时 + 约 10s  <  进程被强制结束前的时间
```

在 Kubernetes 上就是 `terminationGracePeriodSeconds`（默认 30s）。多出的约 10s 留给注销、`CancelWait` 和最后的回调。

### 兜底超时

调度中心里没设超时的任务，如果卡住又不检查 `ctx`，会一直占着这个任务，后续调度只能排队。按最长的任务设置 `DefaultTimeout` 可以兜底。

### 执行器 IP

没配 `IP` 和 `Address` 时，执行器注册本机第一个已启用、非回环的 IPv4。标准的 Kubernetes Pod 取到的就是 Pod IP。
主机或 Pod 有多张网卡（如 Multus）时，请设置 `IP`，或通过 Downward API 注入 Pod IP（`POD_IP` 优先于自动探测）：

```yaml
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

## 进阶用法

### 中间件

中间件包裹每次执行，适合做链路追踪、指标、日志字段、限流等：

```go
exec.Use(func(next xxljob.Handler) xxljob.Handler {
	return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
		start := time.Now()
		err := next.Handle(ctx, req)
		jobDuration.WithLabelValues(req.Handler).Observe(time.Since(start).Seconds())
		return err
	})
})
```

只对某个任务生效的中间件，放在 `Register(name, handler, middlewares...)` 的参数里。调用链深处的代码可以用 `xxljob.RequestFrom(ctx)` 取到当前调度。

### 指标

`exec.Stats()` 返回执行中、排队中的调度数和待发送的结果数，可以导出为 gauge。`PendingCallbacks` 持续上涨说明调度中心连不上。

### 执行日志存储

| 存储 | |
|---|---|
| `NewMemoryLogStore(maxBytes)`（默认） | 共 16 MB，优先淘汰最早结束的执行，单次执行最多占 1/8；重启后丢失 |
| `NewFileLogStore(dir, retentionDays)` | `{dir}/{yyyy-MM-dd}/{logId}.log`，保留 `retentionDays` 天 |
| 自定义 | 实现 `LogStore` 接口，用 `WithLogStore` 传入 |

单行超过 8 KB 会被截断。调度中心每次最多读取 1 MB（`MaxLogChunkBytes`），大日志会分页读取，不会一次性加载。

### 挂到已有的 HTTP 服务上

服务只能暴露一个端口时：

```go
exec, _ := xxljob.New(xxljob.Config{
	// ...
	Address: "http://10.0.0.8:8080/xxl-job/", // 调度中心访问本执行器的完整地址
}, xxljob.WithoutServer())
mux.Handle("/xxl-job/", http.StripPrefix("/xxl-job", exec.Handler()))
_ = exec.Start() // 只注册、回调，不监听端口
```

## 设计

### 执行模型

每个任务（jobId）有一个**执行线程**（一个 goroutine），按顺序执行这个任务的调度，同一时刻最多一次；空闲时退出，下次调度到来时再创建。

```
调度请求 ─► 校验（token / handler / 重复 logId）─► 阻塞策略 ─► 入队 ─► 执行线程
                                                                      │
                 回调队列 ◄── 结果 ◄── panic 捕获 ◄── 中间件 ◄── 任务 ◄─┘
                    │
                    └─► 批量回报调度中心（失败重试、多地址）
```

每次调度有独立的状态，互不影响。ctx 被取消时，用 `context.Cause(ctx)` 判断原因：

| 原因 | 什么时候 | 回报给调度中心 |
|---|---|---|
| `ErrKilled` | 在调度中心点了「终止」 | 失败（500） |
| `ErrCovered` | 被新的调度覆盖（覆盖之前调度） | 失败（500） |
| `ErrHandlerChanged` | 任务改成了别的 JobHandler | 失败（500） |
| `ErrTimeout` | 超过任务的超时时间 | 超时（502） |
| `ErrShutdown` | 执行器退出且停止等待 | 失败（500） |

### 阻塞策略

同一任务上一次执行还没结束时，新的调度如何处理：

| 策略 | 处理方式 |
|---|---|
| 单机串行（`SERIAL_EXECUTION`） | 排队，上一次结束后执行；超过 `MaxQueueSize` 时拒绝 |
| 丢弃后续调度（`DISCARD_LATER`） | 拒绝新的调度 |
| 覆盖之前调度（`COVER_EARLY`） | 取消正在执行的、丢弃排队的，**等被取消的那次真正返回后**再执行新的 |

**终止**会取消正在执行的调度并丢弃排队的。被取消的那次真正返回之前，任务仍算在执行中，所以终止后马上再触发也不会并发执行。

### 结果回调

执行结果先进入回调队列，由后台 goroutine 批量发给调度中心，依次尝试各个地址。失败的按 `CallbackRetryInterval` 重试，队列有上限。
退出时会发完剩余的结果。重启前没送达的结果会丢失，调度中心大约 10 分钟后会把这些执行标记为「结果丢失」。

### 注册与退出

`Start` 先监听端口再注册：端口被占用时直接返回错误，不会注册一个收不到调度的执行器。之后每 30s 续期一次。

`Shutdown(ctx)` 的顺序：

1. 拒绝新的调度（存活检测也返回失败，故障转移路由会把调度改派给其他实例）；
2. 停止心跳并注销；
3. 排队中的调度回报失败；
4. 等执行中的任务结束直到 `ctx` 超时，然后取消任务，再等 `CancelWait`；
5. 发送剩余的结果；
6. 最后关闭执行器端口，等待期间调度中心仍可终止任务。

### 协议版本

各代调度中心的差异（触发路径、鉴权请求头、字段名、应答格式、回调格式）都封装在内部的 `protocol` 接口里，每个版本一个文件。
执行器的其余部分只处理与版本无关的类型，支持新一代协议只需新增一个实现。

### 安全

- 配置了 `AccessToken` 时，执行器端口的每个请求在执行任何操作前都会校验 token（常量时间比较）；3.5 协议还会校验 `XXL-JOB-APPNAME`。
- 请求体上限 1 MB；HTTP 服务有读写超时；发往调度中心的请求都有超时。
- 只支持 BEAN 模式，调度中心下发的 GLUE 脚本会被拒绝，不会执行。

## 测试

```bash
make test       # 单元测试（-race）
make lint       # golangci-lint
make e2e        # 端到端测试：Docker 起真实的调度中心（默认 2.4.1）
XXL_JOB_VERSION=3.5.0 make e2e
make e2e-all    # 依次测 2.4.1、3.4.2、3.5.0
KEEP=1 make e2e # 测完保留容器，可登录调度中心查看（admin / 123456）
XXLJOB_E2E_VERBOSE=1 make e2e # 输出执行器的全部日志（Warn 和 Error 总会输出）
make fuzz       # 每个模糊测试跑 30s（FUZZTIME=5m 可跑更久）
make bench      # 基准测试
```

- **单元测试**用模拟的调度中心覆盖协议、阻塞策略、终止、超时、回调、退出和并发。
- **压力测试**：`TestMultiJobStress` 对 40 个任务并发执行 800 个随机操作（调度、终止、忙碌检测、读日志），混合三种阻塞策略和成功 / 失败 / panic / 超时 / 只能被终止的任务；
  `TestShutdownUnderLoad` 在调度持续进来时退出。两者都检查不变量：每个被受理的调度恰好回调一次、被拒绝的没有回调、同一任务从不并发、
  全局并发不超过上限、没有泄漏的 goroutine。场景随机生成，失败时可用日志里的种子复现：`XXLJOB_STRESS_SEED=<种子> go test -run TestMultiJobStress .`
- **模糊测试**向两种协议的每个接口发送任意请求，并用任意日志测试分页读取和单行截断，检查不会 panic、所有不变量成立。
- **端到端测试**针对每个支持的版本运行官方 `xuxueli/xxl-job-admin` 镜像，包括调度中心每秒按 Cron 触发、且执行时间与下一次调度重叠的任务。
  需要 Docker（compose v2）。调度中心的建表 SQL 在测试时下载并校验 SHA-256（XXL-JOB 以 GPL 发布，不放进本仓库）。

CI 会运行 lint、Go 1.24 ~ 1.26 的单元测试、简短的模糊测试、`govulncheck`、CodeQL，以及三个调度中心版本的端到端测试。

## 参与贡献

欢迎提 Issue 和 Pull Request，详见 [CONTRIBUTING.md](CONTRIBUTING.md)。安全问题请按 [SECURITY.md](SECURITY.md) 私下报告。

## 许可证

[MIT](LICENSE)。XXL-JOB 是 xuxueli 的独立项目，以 GPL-3.0 发布；本库只实现其执行器协议，不包含其任何代码。
