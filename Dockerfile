from rust:1.61


RUN wget -qO- "https://cmake.org/files/v3.23/cmake-3.23.0-linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local

WORKDIR /usr/src/rust-arroyo

COPY . .

RUN rustup toolchain install stable
RUN make release

CMD ["./target/release/errors-consumer"]
