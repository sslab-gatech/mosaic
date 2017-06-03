# architecture-dependent configuration
target_arch=$(shell $(CC) -dumpmachine | sed 's:-.*::')
ifeq ($(target_arch),x86_64)
	CFLAFS += -DHOST -DTARGET_ARCH_X86_64
else ifeq ($(target_arch),k1om)
	CFLAFS += -DTARGET_ARCH_K1OM -mmic
	ifneq (,$(findstring icc,$(CC)))
		CFLAFS += -mmic
	endif
else
        $(error Architecture '$(target_arch)' is not supported)
endif

# common configuration
SRC_ROOT := ..
BLD_ROOT := $(target_arch)

CFLAGS  += -W -Wall
CFLAGS  += -O3 -march=native -mtune=native # max. optimization level
CFLAGS  += -g # include debug symbols
CFLAGS  += -I$(SRC_ROOT)/include
CFLAGS  += -I$(SRC_ROOT)/lib # expose internal headers for unit testcases
LDFLAGS += -lpthread -lrt -lscif
DEPSDIR := .deps
DEPCFLAGS = # -MD -MF $(DEPSDIR)/$*.d -MP

# application-dependent configuration
CFLAGS  += -DRING_BUFFER_CONF_NO_DOUBLE_MMAP # for compatability with kernel
CFLAGS  += -DRING_BUFFER_ASSERT              # for easier debugging

# define objs and bins
pci_ring_buffer_objs := $(addprefix $(BLD_ROOT)/lib/, \
					ring_buffer_scif.o \
				      	ring_buffer_porting.o)
pci_ring_buffer_test_objs := $(patsubst $(SRC_ROOT)/test_user/%.c,\
					$(BLD_ROOT)/test_user/%.o,\
					$(wildcard $(SRC_ROOT)/test_user/*.c))
pci_ring_buffer_test_bins := $(patsubst $(SRC_ROOT)/test_user/%.c,\
					$(BLD_ROOT)/%,\
					$(wildcard $(SRC_ROOT)/test_user/*.c))

# build rules
all: $(BLD_ROOT)/pci_ring_buffer.a \
     $(pci_ring_buffer_test_bins) \
     $(pci_ring_buffer_test_bins)

$(BLD_ROOT)/lib/%.o: $(SRC_ROOT)/lib/%.c
	@mkdir -p $(BLD_ROOT)/lib/
	$(CC) $(CFLAGS) $(DEPCFLAGS) -c -o $@ $<

$(BLD_ROOT)/test_user/%.o: $(SRC_ROOT)/test_user/%.c
	@mkdir -p $(BLD_ROOT)/test_user/
	$(CC) $(CFLAGS) $(DEPCFLAGS) -c -o $@ $<

$(BLD_ROOT)/pci_ring_buffer.a: $(pci_ring_buffer_objs)
	@/bin/rm -f $@
	@mkdir -p $(BLD_ROOT)/
	$(AR) cru $@ $^

$(BLD_ROOT)/rb-ut: $(BLD_ROOT)/test_user/rb-ut.o $(BLD_ROOT)/pci_ring_buffer.a
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

$(BLD_ROOT)/rb-pair: $(BLD_ROOT)/test_user/rb-pair.o $(BLD_ROOT)/pci_ring_buffer.a
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

$(BLD_ROOT)/rbs-ut: $(BLD_ROOT)/test_user/rbs-ut.o $(BLD_ROOT)/pci_ring_buffer.a
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

$(BLD_ROOT)/rbs-flow: $(BLD_ROOT)/test_user/rbs-flow.o $(BLD_ROOT)/pci_ring_buffer.a
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

$(BLD_ROOT)/rbs-blocking-ut: $(BLD_ROOT)/test_user/rbs-blocking-ut.o $(BLD_ROOT)/pci_ring_buffer.a
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f $(pci_ring_buffer_objs) \
	      $(pci_ring_buffer_test_objs) \
	      $(pci_ring_buffer_test_bins) \
	      $(BLD_ROOT)/pci_ring_buffer.a
