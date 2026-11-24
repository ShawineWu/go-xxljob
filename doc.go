// Package xxljob is an XXL-JOB executor for Go.
//
// It implements the executor side of XXL-JOB (https://github.com/xuxueli/xxl-job): it registers with the admin,
// runs triggered jobs, reports their results, serves execution logs and shuts down gracefully. It supports admin
// 2.3.0 – 3.5.x and has no dependencies outside the standard library.
//
//	exec, err := xxljob.New(xxljob.Config{
//		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
//		AccessToken:    "default_token",
//		AppName:        "demo-executor",
//	})
//	if err != nil { ... }
//	exec.MustRegister("Hello", xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
//		xxljob.Logger(ctx).InfoContext(ctx, "hello", "params", req.Params)
//		return nil
//	}))
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	defer stop()
//	if err := exec.Run(ctx); err != nil { ... } // start; on SIGINT/SIGTERM shut down gracefully
//
// See the README for the execution model, block strategies, callbacks and shutdown.
package xxljob
