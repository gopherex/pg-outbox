SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

ROOT_MODULE := github.com/gopherex/pg-outbox
# Max major allowed. v2+ needs semantic import versioning (/vN in module paths),
# which we don't support yet — keep releases on v0/v1.
MAX_MAJOR := 1

.PHONY: help test test-integration tidy lint release

help:
	@echo "make test             - gofmt/vet/build/test every non-integration module"
	@echo "make test-integration - run the nested integration module (testcontainers; Docker required)"
	@echo "make lint             - gofmt check + go vet across every module"
	@echo "make tidy             - go mod tidy in every module"
	@echo "make release          - interactive multi-module release (tag root + every contrib)"

# Every go.mod in the repo. '.' is the root module; contrib modules are tagged
# with their path prefix; test/integration is internal and never tagged.
MODDIRS = $(shell find . -name go.mod -not -path './.git/*' -printf '%h\n' | sed 's#^\./##' | sort)

# Fast, hermetic checks for every non-integration module — no Docker, like CI.
test:
	@for d in $(MODDIRS); do
	  [ "$$d" = "test/integration" ] && continue
	  echo "== $$d =="
	  out=$$(cd "$$d" && gofmt -l .); [ -z "$$out" ] || { echo "✗ gofmt needed:"; echo "$$out"; exit 1; }
	  ( cd "$$d" && GOWORK=off go build ./... && GOWORK=off go vet ./... && GOWORK=off go test -race ./... )
	done

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

# Multi-module release: tag the root with vX.Y.Z and every contrib module with
# its path prefix (for example contrib/publishers/nats/vX.Y.Z). The
# test/integration module is internal and is never tagged.
release:
	@set -euo pipefail
	cd "$$(git rev-parse --show-toplevel)"

	if [ -n "$$(git status --porcelain)" ]; then
	  echo "✗ Working tree is not clean — commit or stash first:"
	  git status --short
	  exit 1
	fi

	mods="$$(for d in $(MODDIRS); do [ "$$d" = "test/integration" ] && continue; echo "$$d"; done)"
	cur="$$(git tag -l 'v[0-9]*.[0-9]*.[0-9]*' | sed 's/^v//' | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)"
	cur="$${cur:-0.0.0}"
	head="$$(git rev-parse --short HEAD)"
	echo "Latest release: v$$cur    HEAD: $$head"
	echo
	echo "  1) recreate last tag (v$$cur) on HEAD   [force]"
	echo "  2) bump version"
	echo "  3) cancel"
	read -r -p "> " action

	tags_for() { # $1 = version (without v); prints one tag per released module
	  local v="$$1" d
	  for d in $$mods; do
	    if [ "$$d" = "." ]; then echo "v$$v"; else echo "$$d/v$$v"; fi
	  done
	}

	case "$$action" in
	1)
	  if [ "$$cur" = "0.0.0" ] && ! git tag -l 'v0.0.0' | grep -q .; then
	    echo "✗ No release tags to recreate."; exit 1
	  fi
	  mapfile -t TAGS < <(tags_for "$$cur")
	  echo
	  echo "Will DELETE and recreate $${#TAGS[@]} tags of v$$cur on $$head, then force-push."
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }
	  for t in "$${TAGS[@]}"; do
	    git tag -d "$$t" 2>/dev/null || true
	    git push origin ":refs/tags/$$t" 2>/dev/null || true
	  done
	  for t in "$${TAGS[@]}"; do git tag -a "$$t" -m "$$t"; done
	  git push origin --force "$${TAGS[@]}"
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
	  mapfile -t TAGS < <(tags_for "$$new")
	  echo
	  echo "Release v$$new — will:"
	  echo "  - set 'require $(ROOT_MODULE) v$$new' in every released nested go.mod"
	  echo "  - commit 'release v$$new'"
	  echo "  - create $${#TAGS[@]} tags and push"
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }
	  for d in $$mods; do
	    [ "$$d" = "." ] && continue
	    ( cd "$$d" && go mod edit -require=$(ROOT_MODULE)@v$$new )
	  done
	  git add -A
	  git diff --cached --quiet || git commit -m "release v$$new"
	  for t in "$${TAGS[@]}"; do git tag -a "$$t" -m "$$t"; done
	  git push origin HEAD
	  git push origin "$${TAGS[@]}"
	  echo "✓ Released v$$new ($${#TAGS[@]} modules)."
	  ;;
	*)
	  echo "Cancelled."
	  ;;
	esac
