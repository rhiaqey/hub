# Hub

## Run in github.dev

### Commands

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup toolchain install nightly
cargo +nightly build --release
cargo +nightly build
```

### Plugins

```
rust-analyzer
better-toml
docker
```
