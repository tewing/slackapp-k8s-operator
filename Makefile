IMG ?= ghcr.io/tewing/slackapp-k8s-operator:latest

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: build
build: fmt vet
	go build -o bin/manager main.go

.PHONY: test
test:
	go test ./...

# Regenerate DeepCopy methods and the CRD. Requires controller-gen:
#   go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5
.PHONY: manifests
manifests:
	controller-gen object:headerFile="" paths="./api/..."
	controller-gen crd paths="./api/..." output:crd:dir=config/crd

.PHONY: docker-build
docker-build:
	docker build -t $(IMG) .

.PHONY: docker-push
docker-push:
	docker push $(IMG)
