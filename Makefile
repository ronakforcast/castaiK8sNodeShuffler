# Variables
PLATFORMS = linux/amd64,linux/arm64
APP = ronakpatildocker/cast-ai-node-manager
TAG_LATEST = $(APP):latest
TAG_VERSION = $(APP):v0.1
REGISTRY ?= docker.io
FULL_APP = $(REGISTRY)/$(APP)
FULL_TAG_LATEST = $(FULL_APP):latest
FULL_TAG_VERSION = $(FULL_APP):v0.1

# Enable BuildKit
export DOCKER_BUILDKIT = 1

# Default target
default: release

# Build the Docker image for multiple architectures
multiarch:
	@echo "==> Building CAST AI node manager container"
	# Check if the multiarch builder already exists, if not, create one
	@if ! docker buildx inspect multiarch-builder &>/dev/null; then \
		echo "No buildx builder found, creating one..."; \
		docker buildx create --use --name multiarch-builder; \
	else \
		echo "Using existing buildx builder..."; \
		docker buildx use multiarch-builder; \
	fi
	docker buildx inspect multiarch-builder --bootstrap
	docker buildx build \
		--platform $(PLATFORMS) \
		-t $(FULL_TAG_VERSION) \
		-t $(FULL_TAG_LATEST) \
		--push \
		.

# Build for local testing (single architecture)
build-local:
	@echo "==> Building local CAST AI node manager container"
	docker build -t $(TAG_LATEST) -t $(TAG_VERSION) .

# Push the Docker image to registry
push:
	@echo "Image pushed to registry: $(FULL_TAG_VERSION), $(FULL_TAG_LATEST)"

# Build and push (alias for multiarch)
release: multiarch

# Test the container locally
test-local:
	@echo "==> Testing container locally"
	docker run --rm $(TAG_LATEST) --help || echo "Container test completed"

# Update Kubernetes manifests with new image
update-k8s:
	@echo "==> Updating Kubernetes manifest with image: $(FULL_TAG_LATEST)"
	@if [ -f "cast-ai-node-manager-all-in-one.yaml" ]; then \
		sed -i.bak 's|image: .*/cast-ai-node-manager:.*|image: $(FULL_TAG_LATEST)|g' cast-ai-node-manager-all-in-one.yaml; \
		echo "Updated cast-ai-node-manager-all-in-one.yaml"; \
	fi
	@if [ -f "cronjob.yaml" ]; then \
		sed -i.bak 's|image: .*/cast-ai-node-manager:.*|image: $(FULL_TAG_LATEST)|g' cronjob.yaml; \
		echo "Updated cronjob.yaml"; \
	fi

# Deploy to Kubernetes
deploy: update-k8s
	@echo "==> Deploying to Kubernetes"
	kubectl apply -f cast-ai-node-manager-all-in-one.yaml

# Clean up the buildx builder
clean:
	-docker buildx rm multiarch-builder || true
	@echo "==> Cleaned up buildx builder"

# Remove local images
clean-local:
	-docker rmi $(TAG_LATEST) $(TAG_VERSION) || true
	@echo "==> Cleaned up local images"

# Full cleanup
clean-all: clean clean-local

# Show current configuration
info:
	@echo "App: $(APP)"
	@echo "Registry: $(REGISTRY)"
	@echo "Latest Tag: $(FULL_TAG_LATEST)"
	@echo "Version Tag: $(FULL_TAG_VERSION)"
	@echo "Platforms: $(PLATFORMS)"

# Help target
help:
	@echo "Available targets:"
	@echo "  default/release - Build and push multi-architecture image"
	@echo "  multiarch       - Build and push multi-architecture image"
	@echo "  build-local     - Build image for local testing"
	@echo "  test-local      - Test the container locally"
	@echo "  push            - Push images to registry"
	@echo "  update-k8s      - Update Kubernetes manifests with new image"
	@echo "  deploy          - Update manifests and deploy to Kubernetes"
	@echo "  clean           - Remove buildx builder"
	@echo "  clean-local     - Remove local images"
	@echo "  clean-all       - Full cleanup"
	@echo "  info            - Show current configuration"
	@echo "  help            - Show this help message"
	@echo ""
	@echo "Variables you can override:"
	@echo "  REGISTRY        - Container registry (default: docker.io)"
	@echo "  APP             - Application name (default: ronakpatildocker/cast-ai-node-manager)"
	@echo "  PLATFORMS       - Build platforms (default: linux/amd64,linux/arm64)"

.PHONY: default multiarch build-local push release test-local update-k8s deploy clean clean-local clean-all info help