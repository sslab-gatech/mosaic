# find . -type d -execdir realpath "{}" ';'
CLANG_FORMAT=~/tools/clang+llvm-3.7.1-x86_64-linux-gnu-ubuntu-14.04/bin/clang-format
find . \( -name '*.h' -or -name '*.cc' \) -print0 | xargs -0 "$CLANG_FORMAT" -i

