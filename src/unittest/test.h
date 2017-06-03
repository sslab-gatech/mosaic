#define sg_test(__cond, __msg)                                                 \
  do {                                                                         \
    fprintf(stderr, "%s[SG-TEST:%s:%d] [%s] %s%s\n",                           \
            (__cond) ? "\033[92m" : "\033[91m", __func__, __LINE__,            \
            (__cond) ? "PASS" : "FAIL", __msg, "\033[0m");                     \
  } while (0)
