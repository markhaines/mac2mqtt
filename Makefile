# Mac2MQTT Makefile

# Variables
BINARY_NAME=mac2mqtt
VERSION=$(shell git describe --tags --always --dirty)
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.Version=${VERSION} -X main.BuildTime=${BUILD_TIME} -s -w"

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Build flags
CGO_ENABLED=1

# Local code signing (see README "Local build and signing").
# macOS Local Network Privacy keys its approval to code identity. The Go linker
# ad-hoc signs as "a.out", so every rebuild looks like a new app and the approval
# is lost. Signing with a stable self-signed identity keeps it. If the keychain
# or identity is absent (CI, other machines) the build warns and carries on.
# Set CODESIGN_IDENTITY= (empty) to skip signing.
CODESIGN_IDENTITY ?= mac2mqtt Local Dev
CODESIGN_IDENTIFIER ?= com.markhaines.mac2mqtt
CODESIGN_KEYCHAIN ?= $(HOME)/Library/Keychains/mac2mqtt-signing.keychain-db
CODESIGN_KEYCHAIN_SERVICE ?= mac2mqtt-signing-keychain
CODESIGN_KEYCHAIN_ACCOUNT ?= $(USER)

# Sign $(1) with the stable identity, then verify the result rather than
# trusting the flags: fail if Authority or Identifier did not take.
define codesign_local
	@if [ -z "$(CODESIGN_IDENTITY)" ]; then \
		echo "WARNING: CODESIGN_IDENTITY is empty, $(1) left ad-hoc signed"; \
	elif [ ! -f "$(CODESIGN_KEYCHAIN)" ]; then \
		echo "WARNING: signing keychain $(CODESIGN_KEYCHAIN) not found, $(1) left ad-hoc signed (Local Network approval will not survive rebuilds)"; \
	elif ! kcpw=$$(security find-generic-password -s "$(CODESIGN_KEYCHAIN_SERVICE)" -a "$(CODESIGN_KEYCHAIN_ACCOUNT)" -w 2>/dev/null); then \
		echo "WARNING: no login keychain item $(CODESIGN_KEYCHAIN_SERVICE), $(1) left ad-hoc signed"; \
	elif ! security unlock-keychain -p "$$kcpw" "$(CODESIGN_KEYCHAIN)"; then \
		echo "ERROR: could not unlock $(CODESIGN_KEYCHAIN)"; exit 1; \
	elif ! security find-identity -p codesigning "$(CODESIGN_KEYCHAIN)" | grep -qF '"$(CODESIGN_IDENTITY)"'; then \
		echo "WARNING: identity '$(CODESIGN_IDENTITY)' not in $(CODESIGN_KEYCHAIN), $(1) left ad-hoc signed"; \
	else \
		echo "Signing $(1) as '$(CODESIGN_IDENTITY)' ($(CODESIGN_IDENTIFIER))..."; \
		codesign --force --keychain "$(CODESIGN_KEYCHAIN)" -s "$(CODESIGN_IDENTITY)" --identifier "$(CODESIGN_IDENTIFIER)" "$(1)" || exit 1; \
		info=$$(codesign -dvvv "$(1)" 2>&1); \
		if ! printf '%s\n' "$$info" | grep -qxF "Authority=$(CODESIGN_IDENTITY)" || \
		   ! printf '%s\n' "$$info" | grep -qxF "Identifier=$(CODESIGN_IDENTIFIER)"; then \
			echo "ERROR: $(1) signature did not take. codesign -dvvv reports:"; \
			printf '%s\n' "$$info"; exit 1; \
		fi; \
		printf '%s\n' "$$info" | grep -E '^(Identifier|Authority|TeamIdentifier)='; \
	fi
endef

.PHONY: all build clean test deps help build-all build-amd64 build-arm64 install uninstall status

all: clean deps test build

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build for current architecture
	@echo "Building $(BINARY_NAME) for current architecture..."
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) mac2mqtt.go
	$(call codesign_local,$(BINARY_NAME))
	@echo "Build complete: $(BINARY_NAME)"

build-all: build-amd64 build-arm64 ## Build for both Intel and ARM architectures

build-amd64: ## Build for Intel Mac (amd64)
	@echo "Building $(BINARY_NAME) for Intel Mac (amd64)..."
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-darwin-amd64 mac2mqtt.go
	chmod +x $(BINARY_NAME)-darwin-amd64
	$(call codesign_local,$(BINARY_NAME)-darwin-amd64)
	@echo "Build complete: $(BINARY_NAME)-darwin-amd64"

build-arm64: ## Build for Apple Silicon Mac (arm64)
	@echo "Building $(BINARY_NAME) for Apple Silicon Mac (arm64)..."
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-darwin-arm64 mac2mqtt.go
	chmod +x $(BINARY_NAME)-darwin-arm64
	$(call codesign_local,$(BINARY_NAME)-darwin-arm64)
	@echo "Build complete: $(BINARY_NAME)-darwin-arm64"

clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME) $(BINARY_NAME)-darwin-*
	@echo "Clean complete"

test: ## Run tests
	@echo "Running tests..."
	$(GOTEST) -v ./...
	@echo "Tests complete"

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	$(GOMOD) download
	@echo "Dependencies downloaded"

install: ## Install Mac2MQTT
	@echo "Installing Mac2MQTT..."
	./install.sh

uninstall: ## Uninstall Mac2MQTT
	@echo "Uninstalling Mac2MQTT..."
	./uninstall.sh

status: ## Check Mac2MQTT status
	@echo "Checking Mac2MQTT status..."
	./status.sh

debug: ## Run debug script
	@echo "Running debug script..."
	./debug.sh

run: build ## Build and run locally
	@echo "Running Mac2MQTT locally..."
	./$(BINARY_NAME)

release: build-all ## Build release packages
	@echo "Creating release packages..."
	@for arch in amd64 arm64; do \
		target="darwin-$$arch"; \
		echo "Creating package for $$target..."; \
		tar -czf $(BINARY_NAME)-$$target.tar.gz \
			$(BINARY_NAME)-$$target \
			mac2mqtt.yaml \
			com.hagak.mac2mqtt.plist \
			install.sh \
			status.sh \
			debug.sh \
			README.md \
			INSTALL.md; \
	done
	@echo "Release packages created:"
	@ls -la $(BINARY_NAME)-darwin-*.tar.gz

format: ## Format Go code
	@echo "Formatting Go code..."
	$(GOCMD) fmt ./...
	@echo "Formatting complete"

vet: ## Run go vet
	@echo "Running go vet..."
	$(GOCMD) vet ./...
	@echo "Vet complete"

lint: format vet ## Run linting tools

# Development helpers
dev-setup: ## Set up development environment
	@echo "Setting up development environment..."
	$(GOMOD) download
	$(GOMOD) tidy
	@echo "Development setup complete"

dev-test: ## Run tests with coverage
	@echo "Running tests with coverage..."
	$(GOTEST) -v -cover ./...
	@echo "Test coverage complete"

# GitHub Actions helpers
gh-build: ## Build for GitHub Actions
	@echo "Building for GitHub Actions..."
	$(GOBUILD) -ldflags="-s -w" -o $(BINARY_NAME) mac2mqtt.go
	chmod +x $(BINARY_NAME)
	@echo "GitHub Actions build complete"

gh-build-matrix: ## Build for GitHub Actions matrix
	@echo "Building for architecture: $(GOARCH)"
	$(GOBUILD) -ldflags="-s -w" -o $(BINARY_NAME)-darwin-$(GOARCH) mac2mqtt.go
	chmod +x $(BINARY_NAME)-darwin-$(GOARCH)
	@echo "Matrix build complete: $(BINARY_NAME)-darwin-$(GOARCH)" 