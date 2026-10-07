# AGENTS.md — AI Agent Instructions for openshift/velero

## Project Overview
This is the OpenShift fork of [Velero](https://velero.io), an open-source tool for backing up, restoring, and migrating Kubernetes cluster resources and persistent volumes. This fork is maintained on the `oadp-dev` branch with OpenShift-specific patches and UBI-based container images for the OADP (OpenShift API for Data Protection) operator.

- **Primary Language**: Go
- **Module**: `github.com/vmware-tanzu/velero`
- **Default Branch**: `oadp-dev`

## Build Instructions
```bash
# Build velero binary locally
make local

# Build all CLI platforms
make all-build

# Build for current OS/arch
make build

# Build container images
make container

# Build UBI-based container
# See Dockerfile.ubi and Dockerfile-velero-restore-helper.ubi
```

## Test Instructions
```bash
# Run unit tests
make test

# Run unit tests locally (without container)
make test-local

# Run specific tests
go test ./pkg/backup/... -run TestName

# Vet code
go vet ./...
```

## Linting
```bash
# Run linter in container
make lint

# Run linter locally
make local-lint
```

Configuration: `.golangci.yaml`

## Verification
```bash
# Run all verification scripts
make verify

# Run all update scripts (codegen, CRDs, etc.)
make update

# Update generated CRD code
make update-crd
```

## Code Conventions
- API types in `api/` with versioning (v1, v2alpha1)
- Controllers and business logic in `pkg/`
- Internal packages in `internal/`
- CLI commands in `cmd/`
- Design proposals in `design/`
- Test helpers and e2e tests in `test/`
- Use `errors.WithStack()` for error wrapping
- Follow controller-runtime patterns for reconcilers
- Changelog entries required in `changelogs/`

## Project Structure
```
api/           - Velero API types (CRDs: Backup, Restore, Schedule, etc.)
cmd/           - CLI binaries (velero, velero-restore-helper)
config/        - Kubernetes manifests and CRD definitions
design/        - Design proposal documents
hack/          - Build and CI scripts
internal/      - Private packages (credentials, hook, volume info, etc.)
pkg/           - Core packages
  backup/      - Backup controller and logic
  restore/     - Restore controller and logic
  controller/  - All Kubernetes controllers
  plugin/      - Plugin framework
  repository/  - Backup repository management
  cmd/         - CLI command implementations
restic/        - Restic integration (legacy, transitioning to Kopia)
test/          - E2E tests, test utilities, and mock implementations
third_party/   - Vendored third-party code
site/          - Documentation website
```

## CI/CD
- GitHub Actions workflows in `.github/workflows/`:
  - `push.yml` — Main CI pipeline
  - `pr-ci-check.yml` — PR validation
  - `pr-linter-check.yml` — Linting on PRs
  - `pr-containers.yml` — Container builds on PRs
  - `pr-codespell.yml` — Spell checking
  - `pr-changelog-check.yml` — Changelog entry validation
  - `publish.yml` — Release publishing
  - `nightly-trivy-scan.yml` — Security scanning
- Prow CI for OpenShift integration testing
- Reproduce CI locally:
  ```bash
  make verify
  make local-lint
  make test-local
  ```

## Common Tasks

### Adding a new Velero controller
1. Define API types in `api/`
2. Create controller in `pkg/controller/`
3. Register in `cmd/velero/`
4. Generate CRDs: `make update-crd`
5. Add tests in the controller package

### Adding a new Velero plugin type
1. Define the plugin interface in `pkg/plugin/`
2. Implement the plugin server/client
3. Add proto definitions if needed
4. Document in `design/`

### Updating OADP-specific patches
- OADP patches live on the `oadp-dev` branch
- Rebase against upstream `vmware-tanzu/velero`
- OpenShift-specific: UBI Dockerfiles, Prow CI config
