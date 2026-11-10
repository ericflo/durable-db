/* Test-only Linux guest init, not the database's clock adapter.
 * The hypervisor supplies the restore marker outside restored application state.
 * SPDX-License-Identifier: Apache-2.0 */
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>

static int guard;

static uint64_t ns(clockid_t id) {
    struct timespec t;
    if (clock_gettime(id, &t))
        _exit(2);
    return (uint64_t)t.tv_sec * 1000000000ULL + t.tv_nsec;
}

static uint64_t marker(void) {
    uint64_t header[4];
    if (pread(guard, header, sizeof header, 0) != sizeof header) {
        perror("vmclock read");
        _exit(3);
    }
    return header[2]; /* v1 disruption_marker at byte 16, little-endian x86 */
}

int main(void) {
    mkdir("/dev", 0755);
    mount("devtmpfs", "/dev", "devtmpfs", 0, NULL);
    int module = open("/vmclock.ko", O_RDONLY);
    if (syscall(SYS_finit_module, module, "", 0)) {
        perror("vmclock module");
        _exit(3);
    }
    close(module);
    guard = open("/dev/vmclock0", O_RDONLY);
    if (guard < 0) {
        perror("vmclock device");
        _exit(3);
    }
    mkdir("/sys", 0755);
    mount("sysfs", "/sys", "sysfs", 0, NULL);
    struct utsname u;
    uname(&u);
    char source[80] = "unknown", line[80];
    FILE *f = fopen("/sys/devices/system/clocksource/clocksource0/current_clocksource", "r");
    if (f) {
        (void)fscanf(f, "%79s", source);
        fclose(f);
    }
    setvbuf(stdout, NULL, _IONBF, 0);
    uint64_t initial = marker();
    printf("READY %s %s\n", u.release, source);
    while (fgets(line, sizeof line, stdin)) {
        uint64_t before = marker();
        uint64_t boot = ns(CLOCK_BOOTTIME);
        uint64_t mono = ns(CLOCK_MONOTONIC);
        uint64_t real = ns(CLOCK_REALTIME);
        uint64_t after = marker();
        printf("SAMPLE %llu %llu %llu %llu %d\n", (unsigned long long)boot,
               (unsigned long long)mono, (unsigned long long)real,
               (unsigned long long)after, before == initial && after == initial);
    }
    for (;;)
        pause();
}
