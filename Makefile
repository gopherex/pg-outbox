SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

ROOT_MODULE := github.com/gopherex/pg-outbox
# Max major allowed. v2+ needs semantic import versioning (/vN in module paths),
# which we don't support yet — keep releases on v0/v1.
MAX_MAJOR := 1

.PHONY: help test test-integration tidy lint release

help:
	@echo "make test             - gofmt/vet/build/test the library module (no Docker)"
	@echo "make test-integration - run the nested integration module (testcontainers; Docker required)"
	@echo "make lint             - gofmt check + go vet across every module"
	@echo "make tidy             - go mod tidy in every module"
	@echo "make release          - interactive root-module release (tag vX.Y.Z + push)"

# Every go.mod in the repo. '.' is the published library; test/integration is a
# Docker-only test module deliberately kept out of the published dependency graph.
MODDIRS = $(shell find . -name go.mod -not -path './.git/*' -printf '%h\n' | sed 's#^\./##' | sort)

# Fast, hermetic checks for the published library — no Docker, like CI.
test:
	@out=$$(gofmt -l .); [ -z "$$out" ] || { echo "✗ gofmt needed:"; echo "$$out"; exit 1; }
	GOWORK=off go build ./...
	GOWORK=off go vet ./...
	GOWORK=off go test -race ./...

# Black-box integration suite in the nested module (spins up Postgres via
# testcontainers). Requires a running Docker daemon.
test-integration:
	@cd test/integration
	go test ./...

lint:
	@for d in $(MODDIRS); do
	  echo "== $$d =="
	  out=$$(cd "$$d" && gofmt -l .); [ -z "$$out" ] || { echo "✗ gofmt needed:"; echo "$$out"; exit 1; }
	  ( cd "$$d" && GOWORK=off go vet ./... )
	done

tidy:
	@for d in $(MODDIRS); do ( cd "$$d" && go mod tidy ); done

# Single published module: tag the root with vX.Y.Z and push. The
# test/integration module is internal (replace directive) and is never tagged.
release:
	@set -euo pipefail
	cd "$$(git rev-parse --show-toplevel)"

	if [ -n "$$(git status --porcelain)" ]; then
	  echo "✗ Working tree is not clean — commit or stash first:"
	  git status --short
	  exit 1
	fi

	cur="$$(git tag -l 'v[0-9]*.[0-9]*.[0-9]*' | sed 's/^v//' | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)"
	cur="$${cur:-0.0.0}"
	head="$$(git rev-parse --short HEAD)"
	echo "Latest release: v$$cur    HEAD: $$head"
	echo
	echo "  1) recreate last tag (v$$cur) on HEAD   [force]"
	echo "  2) bump version"
	echo "  3) cancel"
	read -r -p "> " action

	case "$$action" in
	1)
	  if [ "$$cur" = "0.0.0" ] && ! git tag -l 'v0.0.0' | grep -q .; then
	    echo "✗ No release tags to recreate."; exit 1
	  fi
	  echo
	  echo "Will DELETE and recreate v$$cur on $$head, then force-push."
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }
	  git tag -d "v$$cur" 2>/dev/null || true
	  git push origin ":refs/tags/v$$cur" 2>/dev/null || true
	  git tag -a "v$$cur" -m "v$$cur"
	  git push origin --force "v$$cur"
	  echo "✓ Recreated v$$cur on $$head."
	  ;;
	2)
	  IFS=. read -r MA MI PA <<< "$$cur"
	  echo
	  echo "  1) major  -> v$$((MA+1)).0.0"
	  echo "  2) minor  -> v$$MA.$$((MI+1)).0"
	  echo "  3) patch  -> v$$MA.$$MI.$$((PA+1))"
	  read -r -p "> " comp
	  case "$$comp" in
	    1) MA=$$((MA+1)); MI=0; PA=0 ;;
	    2) MI=$$((MI+1)); PA=0 ;;
	    3) PA=$$((PA+1)) ;;
	    *) echo "Aborted."; exit 0 ;;
	  esac
	  if [ "$$MA" -gt "$(MAX_MAJOR)" ]; then
	    echo "✗ v$$MA requires semantic import versioning (/v$$MA in module paths)."
	    echo "  Not supported yet — stay on v0/v1."
	    exit 1
	  fi
	  new="$$MA.$$MI.$$PA"
	  echo
	  echo "Release v$$new — create tag v$$new and push."
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }
	  git tag -a "v$$new" -m "v$$new"
	  git push origin HEAD
	  git push origin "v$$new"
	  echo "✓ Released v$$new."
	  ;;
	*)
	  echo "Cancelled."
	  ;;
	esac
