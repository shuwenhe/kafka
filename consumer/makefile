# 配置
service_name = iot-data-bridge
repository = registry.cn-hangzhou.aliyuncs.com
docker_file = Dockerfile
config_path = configs/dev.yaml
config_path_test = configs/test.yaml
tag = test

.PHONY:help
help: ##@other Show this help.
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)

.PHONY:run-realtime
run-bridge: ##@run 启动服务.
	@echo "启动服务..."
	go run -race . --config=$(config_path) bridge

.PHONY:migrate
migrate: ##@migrate 启动migrate服务.
	@echo "启动服务..."
	go run . --config=$(config_path) migrate

.PHONY:build-linux
build-linux: ##@build 构建二进制文件.
	@echo "构建二进制文件"
	CGO_ENABLED=0 GOOS=linux go build -mod=vendor -a -installsuffix cgo -o $(service_name) .

# 镜像构建
.PHONY:docker-build
docker-build:build-linux ##@docker 构建镜像.
	docker build -t $(repository):$(tag) -f $(docker_file) .
	@echo "build success"


# 镜像构建
.PHONY:docker-push
docker-push: docker-build##@docker 推送镜像.
	docker push $(repository):$(tag)
	@echo "push success"


HELP_FUN = \
	%help; \
	while(<>) { push @{$$help{$$2 // 'options'}}, [$$1, $$3] if /^([a-zA-Z\-]+)\s*:.*\#\#(?:@([a-zA-Z\-]+))?\s(.*)$$/ }; \
	print "usage: make [target]\n\n"; \
	for (sort keys %help) { \
		print "${WHITE}$$_:${RESET}\n"; \
		for (@{$$help{$$_}}) { \
			$$sep = " " x (32 - length $$_->[0]); \
			print "  ${YELLOW}$$_->[0]${RESET}$$sep${GREEN}$$_->[1]${RESET}\n"; \
	}; \
	print "\n"; }
