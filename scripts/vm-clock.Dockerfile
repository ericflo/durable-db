FROM alpine:3.23@sha256:469c9ea410b9215eaee7976ba6c0639039b444be8e5d638f7abbada189971eb2 AS stock
RUN apk add --no-cache qemu-system-x86_64 qemu-img linux-virt cpio

# A disposable qualification kernel, never installed on the development host.
# Building is cached independently of the Rust binary and test scripts.
FROM stock AS kernel
RUN apk add --no-cache build-base flex bison perl openssl-dev elfutils-dev bc linux-headers xz bash
WORKDIR /build
RUN wget -q https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.60.tar.xz && \
    echo '3c2f44ef6acc021a7ad444d9a2e6ec1ef488ff09368793d0004a6080f69d621e  linux-6.18.60.tar.xz' | sha256sum -c - && \
    tar -xf linux-6.18.60.tar.xz && rm linux-6.18.60.tar.xz
WORKDIR /build/linux-6.18.60
RUN make x86_64_defconfig && \
    scripts/config --disable SUSPEND --disable HIBERNATION --disable MODULES \
      --enable PM --enable PTP_1588_CLOCK --enable PTP_1588_CLOCK_VMCLOCK \
      --enable E1000 --enable DEVTMPFS --enable DEVTMPFS_MOUNT \
      --set-str LOCALVERSION '-durable-db-nosleep' && \
    make olddefconfig && \
    ! grep -Eq '^CONFIG_(SUSPEND|HIBERNATION|PM_SLEEP)=' .config && \
    grep -q '^CONFIG_PTP_1588_CLOCK_VMCLOCK=y$' .config && \
    make -j8 bzImage

FROM stock AS nosleep
COPY --from=kernel /build/linux-6.18.60/arch/x86/boot/bzImage /boot/vmlinuz-virt
COPY --from=kernel /build/linux-6.18.60/.config /boot/durable-db-kernel.config
# Required drivers are built into this kernel; stock modules cannot be loaded.
RUN rm -rf /lib/modules
