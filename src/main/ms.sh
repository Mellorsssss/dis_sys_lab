go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
rm mr-*
go run -race mrcoordinator.go pg-*.txt
