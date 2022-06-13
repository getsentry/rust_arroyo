format:
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt --all
.PHONY: format

build: setup-git
	cargo +stable build
.PHONY: build

release: setup-git
	@cargo +stable build --release
.PHONY: release

setup-git: .git/hooks/pre-commit
.PHONY: setup-git

.git/hooks/pre-commit:
	@cd .git/hooks && ln -sf ../../scripts/git-precommit-hook pre-commit