# go-xxljob

[![CI](https://github.com/ShawineWu/go-xxljob/actions/workflows/ci.yml/badge.svg)](https://github.com/ShawineWu/go-xxljob/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ShawineWu/go-xxljob.svg)](https://pkg.go.dev/github.com/ShawineWu/go-xxljob)
[![Go Report Card](https://goreportcard.com/badge/github.com/ShawineWu/go-xxljob)](https://goreportcard.com/report/github.com/ShawineWu/go-xxljob)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[English](README.md) | 简体中文

健壮、完整的 [XXL-JOB](https://github.com/xuxueli/xxl-job) Go 执行器。

XXL-JOB 是广泛使用的分布式任务调度平台：**调度中心**决定任务什么时候执行，**执行器**负责执行。本库是 Go 服务的执行器。

- **支持调度中心 2.3.0 ~ 3.5.x**，两代协议，改一个配置即可切换。已对调度中心 2.4.1、3.3.1、3.4.2、3.5.0 做端到端测试。
- **结果可信**：任务失败就回报失败，调度中心的告警和失败重试会正常触发。任务被终止或超时时，在发生的那一刻就回报，即使 handler 忽略取消、之后返回 nil 也不会被当成成功。
- **同一任务永远不会并发执行**：三种阻塞策略（单机串行 / 丢弃后续调度 / 覆盖之前调度）与官方执行器一致，并且上一次的 handler 返回后，下一次才会开始。
- **一切有界**：准入、队列、日志、回调都有上限，关停有时间预算，见[资源上限](#资源上限)。
- **功能完整**：终止、超时、调度中心可查看的执行日志、分片广播、结果回调失败重试（可选跨重启保留）、优雅退出、OpenTelemetry（[`otelxxljob`](#链路追踪与指标opentelemetry)）。
- **易于测试**：`xxljob.Invoke` 不需要调度中心，按真实执行的方式跑一次任务。
- **零依赖**：只依赖 Go 标准库（OpenTelemetry 集成是独立的模块）。

## 目录

- [安装](#安装)
- [快速开始](#快速开始)
- [编写任务](#编写任务)
- [调度中心版本](#调度中心版本)
- [配置](#配置)
- [部署](#部署)
- [资源上限](#资源上限)
- [进阶用法](#进阶用法)
- [设计](#设计)
- [测试](#测试)
- [参与贡献](#参与贡献) · [许可证](#许可证)

## 安装

```bash
go get github.com/ShawineWu/go-xxljob
```

需要 Go 1.26 及以上（Go 官方维护的两个版本）。

## 快速开始

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

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
		xxljob.Logger(ctx).InfoContext(ctx, "hello", "params", req.Params) // 调度中心「执行日志」里可见
		return nil
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := exec.Run(ctx); err != nil { // 启动；收到 SIGINT/SIGTERM 后等执行中的任务结束，再注销
		panic(err)
	}
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
		xxljob.Logger(ctx).InfoContext(ctx, "synced batch", "size", len(batch))
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
| `req.JobID`、`req.LogID`、`req.Timeout` 等 | 本次调度的元数据（handler 拿到的是副本） |
| `xxljob.Logger(ctx)` | `*slog.Logger`，同时写入调度中心的执行日志**和**应用日志 |
| `xxljob.SetResult(ctx, msg)` | 成功时回报的执行备注 |
| `ctx` | 超时、终止、覆盖之前调度、更换 JobHandler、执行器退出时被取消，`context.Cause(ctx)` 可以区分原因 |

ctx 被取消时，这次执行会立即回报失败（超时则回报超时），不管 handler 之后返回什么。耗时任务仍然应该检查 `ctx`：handler 返回之前，这个任务一直处于忙碌状态，后续调度会等待（或按阻塞策略被丢弃）。

### 单元测试

`xxljob.Invoke` 不需要调度中心，直接执行一次任务，日志、`SetResult`、超时、panic 的行为和真实执行完全一致：

```go
func TestSyncItemsTask(t *testing.T) {
	res := xxljob.Invoke(t.Context(), NewSyncItemsTask(db), &xxljob.Request{Params: `{"batchSize":10}`})

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

调度中心升级到 3.5 时只需改这两项，任务代码不用动。调度中心从 3.4.1 起去掉了 `/xxl-job-admin` 路径前缀，`AdminAddresses` 也要相应调整。如果调度中心调用了执行器不提供的路径（通常是 `AdminVersion` 为 `"2"` 的执行器收到了 `/trigger`），调度日志里会直接提示去检查 `AdminVersion`。

## 配置

只有 `AdminAddresses` 和 `AppName` 是必填的。零值表示使用默认值，负数会被拒绝。

| 字段 | 默认值 | 说明 |
|---|---|---|
| `AdminAddresses` | 必填 | 调度中心地址；配置多个时优先用最近一次成功的地址，失败再切换到其他地址 |
| `AppName` | 必填 | 执行器 AppName |
| `AdminVersion` | `"2"` | `"2"`：调度中心 2.3.0 ~ 3.4.x；`"3.5"`：3.5.0+ |
| `AccessToken` | 空 | 见[调度中心版本](#调度中心版本)。为空表示执行器端口不鉴权 |
| `IP` | `$POD_IP`，否则自动探测 | 注册给调度中心的 IP |
| `Port` | `9999` | 执行器端口 |
| `Address` | `http://{IP}:{Port}/` | 注册给调度中心的完整地址（经过 NAT、挂在子路径时设置） |
| `DefaultTimeout` | `0`（不限） | 调度中心没设超时的任务的兜底超时 |
| `MaxConcurrency` | `0`（不限） | 所有任务合计的并发执行上限，超出的等待空位 |
| `MaxQueueSize` | `1000` | 单个任务最多排队多少次调度（单机串行） |
| `MaxActiveTriggers` | `2000` | 所有任务合计的在途调度上限（执行中、等待空位、排队中），见[资源上限](#资源上限) |
| `ShutdownTimeout` | `15s` | `Run` 的 ctx 结束后，等执行中的任务多久 |
| `CancelWait` | `3s` | 关停时取消任务后再等多久，也是最后一批回调至少能用的时间 |
| `CallbackDir` | 空 | 发不出去的结果写入这个目录，之后再发送，下一个进程也会接着发 |
| `MaxPendingCallbacks` | `10000` | 待发送结果的上限（同时不超过 64 MiB） |
| `LongRunningThreshold` | `1h` | 单次执行超过这个时长记一条 Warn；负数表示关闭 |
| `RegistryInterval` | `30s` | 注册心跳间隔 |
| `RequestTimeout` | `3s` | 发往调度中心的单个请求的超时 |
| `CallbackRetryInterval` | `10s` | 结果回调失败后的重试间隔 |

传给 `New` 的选项：

| 选项 | 默认值 | |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | 执行器自身日志，以及任务日志的应用日志部分 |
| `WithLogStore(LogStore)` | 内存，16 MiB | 执行日志的存储 |
| `WithMiddleware(...)` | 无 | 对所有任务生效的中间件 |
| `WithHTTPClient(*http.Client)` | `&http.Client{}` | 访问调度中心的 HTTP 客户端（会被复制，且不跟随重定向） |
| `WithoutServer()` | 关 | 不监听端口，在启动前自行挂载 `exec.Handler()` |

## 部署

### 优雅退出

`Run` 的 ctx 结束时（或调用 `Shutdown(ctx)` 时），执行器会拒绝新的调度、注销、最多等执行中的任务 `ShutdownTimeout`（或直到 `ctx` 超时）、取消剩下的任务，并发出剩余的结果。每一步都有上限：只有一个调度中心地址时，关停最多耗时 `ShutdownTimeout + 2×RequestTimeout + 2×CancelWait + 1s`，默认配置下是 28 秒，在 Kubernetes 默认的 30 秒 `terminationGracePeriodSeconds` 之内。如果为了长任务调大了 `ShutdownTimeout`，宽限期也要相应调大。

### 跨重启保留结果

关停时发不出去的结果（调度中心不可用或太慢）默认会丢失，调度中心会把这些执行一直显示为「运行中」。把 `CallbackDir` 设为一个持久目录（能跨 Pod 保留的卷），这些结果就会被保存下来，由下一个进程接着发送。

### 兜底超时

调度中心里没设超时的任务可能一直运行下去。按最长的任务设置 `DefaultTimeout`：到点后这次执行会回报超时，并取消它的 ctx。不检查 ctx 的 handler 会一直占着这个任务直到返回，所以长循环要检查 `ctx`。

### 执行器 IP

没配 `IP` 和 `Address` 时，执行器注册本机第一个已启用、非回环的 IPv4。标准的 Kubernetes Pod 取到的就是 Pod IP。主机或 Pod 有多张网卡（如 Multus）时，请设置 `IP`，或通过 Downward API 注入 Pod IP（`POD_IP` 优先于自动探测）：

```yaml
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

## 资源上限

执行器持有的一切都有上限：

| 资源 | 上限 |
|---|---|
| 任务 goroutine | `MaxActiveTriggers`（每个有执行中或排队调度的任务一个），另加每个正在执行的 handler 一个 |
| 内存中的调度 | 合计 `MaxActiveTriggers`，单个任务 `MaxQueueSize`；每个请求体最多 256 KiB |
| 同时执行的 handler | 设置了 `MaxConcurrency` 时受其限制 |
| 内存中的执行日志 | 16 MiB（`NewMemoryLogStore`），单次执行最多占 1/8，优先淘汰最早结束的执行 |
| 磁盘上的执行日志 | 单次执行 16 MiB（`FileLogStore`），按保留期清理；每个执行中的任务打开一个文件 |
| 待发送的结果 | `MaxPendingCallbacks` 且不超过 64 MiB；超出部分写入 `CallbackDir`，或丢弃并记日志 |
| 关停时间 | 见[优雅退出](#优雅退出) |

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

只对某个任务生效的中间件，放在 `Register(name, handler, middlewares...)` 的参数里。中间件链在调度被受理时组装，所以 `Use` 只对之后受理的调度生效。调用链深处的代码可以用 `xxljob.RequestFrom(ctx)` 取到当前调度。

### 链路追踪与指标（OpenTelemetry）

[`otelxxljob`](otelxxljob) 模块（独立模块，执行器本身仍然零依赖）为每次执行生成链路，并导出执行器的运行状态：

```bash
go get github.com/ShawineWu/go-xxljob/otelxxljob
```

```go
if _, err := otelxxljob.Instrument(exec); err != nil { ... } // 在 Start 之前
```

- **链路**：每次执行一个 span（`xxljob {handler}`，带任务 ID、日志 ID、阻塞策略和分片），记录错误和 panic，并区分失败、超时、被终止、被覆盖等结果（`xxljob.result`）。trace ID 会写进执行日志，从调度中心的日志页面就能找到对应的链路。
- **指标**：按 handler 和结果统计的 `xxljob.job.duration` 直方图；`xxljob.executor.running`、`.queued`、`.pending_callbacks` 几个 UpDownCounter；`.registered` gauge；`xxljob.executor.triggers`（accepted / rejected）和 `xxljob.executor.results`（success / failure / timeout）计数器。全部带 AppName 标签。

默认使用全局的 OpenTelemetry provider，也可以用 `WithTracerProvider` / `WithMeterProvider` 指定。调度中心不会传递 trace 上下文，所以每次执行都是一条新链路，任务内部产生的 span 会挂在它下面。

### 运行状态与告警

不用 OpenTelemetry 时，`exec.Stats()` 是一个开销很小的快照，可以用于指标：

| 字段 | 类型 | |
|---|---|---|
| `Running`、`Queued` | gauge | 有执行中调度的任务数（含等待并发空位的）和排队中的调度数 |
| `PendingCallbacks` | gauge | 还没送达的结果数；持续上涨说明调度中心连不上 |
| `Registered` | 状态 | 最近一次注册心跳是否成功。用它做告警；不要用作就绪探针，否则调度中心一宕机所有实例都会被摘流量 |
| `Accepted`、`Rejected` | 计数 | 受理的调度数，以及被拒绝的（handler 不存在、丢弃后续调度、队列满、执行器繁忙等） |
| `Succeeded`、`Failed`、`TimedOut` | 计数 | 结果；每个受理的调度恰好落在其中一项 |

### 执行日志存储

| 存储 | |
|---|---|
| `NewMemoryLogStore(maxBytes)`（默认） | 共 16 MiB，单次执行最多占 1/8，优先淘汰最早结束的执行；重启后丢失 |
| `NewFileLogStore(dir, retentionDays)` | `{dir}/{yyyy-MM-dd}/{logId}.log`，单次执行最多 16 MiB，保留最近 `retentionDays` 天 |
| 自定义 | 实现 `LogStore` 接口，用 `WithLogStore` 传入 |

单行超过 8 KiB 会被截断。调度中心读取日志时从指定行一直读到末尾，和官方执行器一致。存储出错或 panic 都不会让任务失败，错误只记一次日志。

### 挂到已有的 HTTP 服务上

服务只能暴露一个端口时：

```go
exec, _ := xxljob.New(xxljob.Config{
	// ...
	Address: "http://10.0.0.8:8080/xxl-job/", // 调度中心访问本执行器的完整地址
}, xxljob.WithoutServer())
mux.Handle("/xxl-job/", http.StripPrefix("/xxl-job", exec.Handler())) // 在启动前挂载
go server.ListenAndServe()
err := exec.Run(ctx) // 注册和回调；调度中心的请求由你的 HTTP 服务处理
```

## 设计

[ARCHITECTURE.md](ARCHITECTURE.md) 说明了整体设计：下面每一条不变量都由唯一的一个模块负责。

### 执行模型

每个任务（jobId）有一个**执行线程**（一个 goroutine），按顺序执行这个任务的调度，同一时刻最多一次。每个受理的调度都是一次有显式阶段的执行（排队、就绪、等待空位、执行中、取消中、已结束），恰好回报一次结果。

执行的 ctx 被取消时，可以用 `context.Cause(ctx)` 判断原因，结果会立即回报：

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

**终止**会取消正在执行的调度并丢弃排队的。官方执行器在被取消的 handler 可能还在运行时就开始下一次执行；这里要等被取消的 handler 真正返回才开始下一次，同一任务绝不会重叠执行。

### 结果回调

执行结果先进入队列，批量发给最近一次成功的调度中心地址（失败再切换到其他地址），失败的按 `CallbackRetryInterval` 重试。正在进行的请求不会被中途打断，因为调度中心可能已经处理了它。队列有上限；配置了 `CallbackDir` 时，放不下的、或者关停前发不出去的结果会被保存下来，之后再发，下一个进程也会接着发。投递语义是「至少一次」：响应丢失后同一批可能会重发，调度中心会忽略重复的回调。

### 注册与退出

`Start` 先监听端口再注册：端口被占用时直接返回错误，不会注册一个收不到调度的执行器。之后每 30s 续期一次。

`Shutdown(ctx)` 按开始时确定的时间计划执行：

1. 拒绝新的调度（存活检测也返回失败，故障转移路由会把调度改派给其他实例）；
2. 停止心跳并注销（最多 `RequestTimeout`）；
3. 丢弃排队中和等待并发空位的调度；等执行中的任务结束直到 `ctx` 超时，然后取消任务，最多再等 `CancelWait`；
4. 发送剩余的结果（至少 `CancelWait`）；剩下的写入 `CallbackDir`，或记为丢失；
5. 最后关闭执行器端口，等待期间调度中心仍可终止任务。

### 协议版本

各代调度中心的差异（触发路径、鉴权请求头和文案、字段名、应答格式、回调格式）都封装在 `internal/wire` 里，每个版本一个实现。和官方执行器一样，执行器端口总是返回 HTTP 200，由 JSON 里的 code 区分成功和失败。

### 安全

- 配置了 `AccessToken` 时，执行器端口的每个请求在执行任何操作前都会校验 token（常量时间比较，且不泄露长度）；3.5 协议还会校验 `XXL-JOB-APPNAME`。**不配置时，任何能访问端口的人都能触发、终止任务并读取日志**，执行器启动时会打一条警告。
- 请求体上限 256 KiB；HTTP 服务有超时；发往调度中心的请求都有超时，并且不跟随重定向（重定向会把 token 带到别的主机）。
- 任务参数不会出现在错误信息里，在执行日志中也会截断。
- 只支持 BEAN 模式，调度中心下发的 GLUE 脚本会被拒绝，不会执行。

报告漏洞请见 [SECURITY.md](SECURITY.md)。

## 测试

```bash
make test       # 单元测试（-race，所有模块）
make lint       # golangci-lint
make e2e        # 端到端测试：Docker 起真实的调度中心（默认 2.4.1）
XXL_JOB_VERSION=3.5.0 make e2e
make e2e-all    # 依次测 2.4.1、3.3.1、3.4.2、3.5.0
KEEP=1 make e2e # 测完保留容器，可登录调度中心查看（admin / 123456）
make fuzz       # 每个模糊测试跑 30s（FUZZTIME=5m 可跑更久）
make stress     # 重复跑并发压力测试
make bench      # 基准测试
```

- **调度层测试**在虚拟时钟（`testing/synctest`）上运行：超时、终止、覆盖、关停都在精确的时刻检查，不依赖 sleep，也不会偶发失败。
- **单元测试**用模拟的调度中心覆盖协议、HTTP 接口、回调（故障转移、重试、上限、落盘）、日志存储和生命周期。
- **压力测试**对许多任务并发执行数百个随机操作，检查不变量：每个被受理的调度恰好回调一次、被拒绝的没有回调、同一任务从不并发、并发上限成立、计数平衡、没有泄漏的 goroutine。失败时可用日志里的种子复现：`XXLJOB_STRESS_SEED=<种子> go test -run TestMultiJobStress .`
- **模糊测试**向两种协议的每个接口发送任意请求，并用任意日志测试日志相关代码。
- **端到端测试**针对 2.4.1、3.3.1（不配 token）、3.4.2、3.5.0 运行官方 `xuxueli/xxl-job-admin` 镜像，包括调度中心每秒按 Cron 触发的任务。需要 Docker（compose v2）。调度中心的建表 SQL 在测试时下载并校验 SHA-256（XXL-JOB 以 GPL 发布，不放进本仓库）。

CI 会运行 lint、Go 官方维护的两个版本的单元测试、模糊测试、`govulncheck`、CodeQL，以及四个调度中心版本的端到端测试。

## 参与贡献

欢迎提 Issue 和 Pull Request，详见 [CONTRIBUTING.md](CONTRIBUTING.md) 和 [ARCHITECTURE.md](ARCHITECTURE.md)。安全问题请按 [SECURITY.md](SECURITY.md) 私下报告。

## 许可证

[MIT](LICENSE)。XXL-JOB 是 xuxueli 的独立项目，以 GPL-3.0 发布；本库只实现其执行器协议，不包含其任何代码。
