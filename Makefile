
release:
	RUSTC_WRAPPER=`which sccache` cargo build --release

test:
	RUSTC_WRAPPER=`which sccache` cargo test

install-sccache:
	cargo install --force --git https://github.com/mozilla/sccache

format:
	rustfmt --write-mode overwrite **/*.rs

rustfmt-test:
	git cancel
	rustfmt --write-mode overwrite **/*.rs
