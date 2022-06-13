from rust:1.61


RUN wget -qO- "https://cmake.org/files/v3.23/cmake-3.23.0-linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local

WORKDIR /usr/src/rust-arroyo

COPY . .

RUN cargo build

# CMD ["cargo run --example base_procesor"]
