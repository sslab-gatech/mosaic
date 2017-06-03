#include <assert.h>
#include <cstdio>
#include <cstdlib>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

int main(int argc, char** argv) {
  long sz = sysconf(_SC_PAGESIZE);
  size_t size = 2 * sz;
  int fd = shm_open("test", O_RDWR | O_CREAT, 0777);
  if (ftruncate(fd, size * 2))
    ;

  if (fd == -1) {
    printf("Fail open\n");
  }

  char* p =
      (char*)mmap(NULL, size * 2, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  char* q = (char*)mmap(p + size, size, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_FIXED, fd, 0);
  close(fd);

  if (p == MAP_FAILED) {
    int err = errno;
    printf("Fail mmap p: %d\n", err);
    exit(1);
  }

  if (q == MAP_FAILED) {
    int err = errno;
    printf("Fail mmap q: %d\n", err);
    exit(1);
  }

  for (int i = size; i < size; i++) {
    p[i] = (char)i;
  }

  for (int i = 0; i < size; ++i) {
    assert(p[i] == p[i + size]);
    assert(p[i] == q[i]);
  }

  printf("Success!\n");
  munmap(p, size * 2);
  munmap(q, size);
  shm_unlink("test");
  return 0;
}
