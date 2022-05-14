format:
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt --all
.PHONY: format


setup-git: .git/hooks/pre-commit
.PHONY: setup-git

.git/hooks/pre-commit:
	@cd .git/hooks && ln -sf ../../scripts/git-precommit-hook pre-commit